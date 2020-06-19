// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Configuration information used here to decide how to wrap Reader and Writer.
// For now, our goal is make all tests pass with (disallow: true).
//
// TODO(sumeer): make this externally configurable.
//
// State transitions are
// (disallow: true) => (disallow: false, enabled: false) <=> (disallow: false, enabled: true)
// The transition to (disallow: false, enabled: false) happens after the cluster has transitioned to
// Pebble permanently. The transition to enabled: true can be rolled back.
//
// Eventually the cluster will be finalized in state (disallow: false, enabled: true), at
// which point there is no rollback. Additionally, we will need to force all remaining
// interleaved intents to be rewritten (these may potentially be of committed transactions
// whose intent resolution did not happen yet).
const disallowSeparatedIntents = true
const enabledSeparatedIntents = false

// A wrapped iterator interface. The wrapping is not directly done by the
// caller, but is a side-effect of using a wrapper Reader. All functions that
// expect to see intents must type-assert that their Iterators are
// MetaIterators, to prevent bugs due to calling paths that did not wrap.
type MetaIterator interface {
	Iterator
	isCurMetaSeparated() bool
}

// A wrapped writer interface. All functions that expect to write intents or
// inline-meta must type-assert that their Writers are MetaWriters, to
// prevent bugs due to calling paths that did not wrap.
type MetaWriter interface {
	Writer

	// REQUIRES: state is ExistingIntentInterleaved or ExistingIntentSeparated
	ClearMVCCMeta(key roachpb.Key, state PrecedingIntentState, possiblyUpdated bool) error

	PutMVCCMeta(
		key roachpb.Key, value []byte, state PrecedingIntentState, precedingPossiblyUpdated bool) error

	ClearInlineMeta(key roachpb.Key) error

	PutInlineMeta(key roachpb.Key, value []byte) error
}

// Our code paths don't need a MetaWriter that can write both intents and
// inline-meta, which allows us to do additional error checking.
type metaKind int

const (
	inlineMetaOnly metaKind = iota
	mvccMetaOnly
)

// A wrapped reader interface. All functions that expect to read intents or
// inline-meta must type-assert that their Readers are MetaReaders, to
// prevent bugs due to calling paths that did not wrap.
type MetaReader interface {
	Reader
	// Marker method.
	metaReaderMarker()
}

// Used for read/write work.
type metaAwareReadWriter struct {
	MetaReader
	MetaWriter
}

// Wraps a ReadWriter so that it is aware of meta, but needs to only handle
// one-of inline or MVCC meta, indicated by the inlineMeta parameter.
func NewMetaAwareReadWriter(
	rw ReadWriter, inlineMeta bool, txn *roachpb.Transaction,
) *metaAwareReadWriter {
	var txnUUID *uuid.UUID
	if txn != nil {
		txnUUID = &txn.ID
	}
	return NewMetaAwareReaderWriterTxnUUID(rw, inlineMeta, txnUUID)
}

func NewMetaAwareReaderWriterTxnUUID(
	rw ReadWriter, inlineMeta bool, txnUUID *uuid.UUID,
) *metaAwareReadWriter {
	var interleaveSeparatedForRead bool
	if !disallowSeparatedIntents && !inlineMeta {
		interleaveSeparatedForRead = true
	}
	marw := &metaAwareReadWriter{}
	if interleaveSeparatedForRead {
		marw.MetaReader = &interleaveSeparatedMetaReader{r: rw}
		marw.MetaWriter = &intentDemuxWriter{
			Writer:           rw,
			separatedIntents: enabledSeparatedIntents,
			txnUUID:          txnUUID,
		}
	} else {
		kind := mvccMetaOnly
		if inlineMeta {
			kind = inlineMetaOnly
		}
		marw.MetaReader = trivialMetaReader{Reader: rw}
		marw.MetaWriter = &interleavedMetaWriter{metaKind: kind}
	}
	return marw
}

// Creates a metaAwareReadWriter that can write to inline meta (not MVCC
// meta), but can read both inline and MVCC meta. This is used by GC since it
// has slightly peculiar needs -- it never updates MVCC meta.
// TODO: once we wrap in the Pebble implementation this can just be a normal
// "observe both"
func newInlineMetaWriterAndBothReader(rw ReadWriter) *metaAwareReadWriter {
	var r MetaReader
	if disallowSeparatedIntents {
		r = trivialMetaReader{Reader: rw}
	} else {
		r = &interleaveSeparatedMetaReader{r: rw}
	}
	return &metaAwareReadWriter{
		MetaReader: r,
		MetaWriter: &interleavedMetaWriter{metaKind: inlineMetaOnly},
	}
}

// Used in blind puts.
func newMetaAwareWriter(w Writer, inlineMeta bool, txn *roachpb.Transaction) MetaWriter {
	if disallowSeparatedIntents || inlineMeta {
		kind := mvccMetaOnly
		if inlineMeta {
			kind = inlineMetaOnly
		}
		return &interleavedMetaWriter{Writer: w, metaKind: kind}
	} else {
		var txnUUID *uuid.UUID
		if txn != nil {
			txnUUID = &txn.ID
		}
		return &intentDemuxWriter{
			Writer:           w,
			separatedIntents: enabledSeparatedIntents,
			txnUUID:          txnUUID,
		}
	}
}

// Will return a MetaReader.
// We don't care about the RocksDB optimization that does iter.(MVCCIterator)
// since RocksDB is being removed.
func newMetaAwareReader(r Reader, onlyInlineMeta bool) MetaReader {
	if onlyInlineMeta || disallowSeparatedIntents {
		return trivialMetaReader{Reader: r}
	}
	return &interleaveSeparatedMetaReader{r: r}
}

// MetaWriter implementations.
//
// disallow: true:  use interleavedMetaWriter with appropriate metaKind
// disallow: false:
// - use interleavedMetaWriter for inline.
// - Use intentDemuxWriter for MVCC.
//
// For now, the only separated locks are exclusive locks that are also intents,
// whose putting and clearing is accomplished using PutMVCCMeta, ClearMVCCMeta,
// so we disallow use of the *Storage() functions below. This will be relaxed
// when there are other kinds of replicated locks.

type wrappableIntentWriter interface {
	Writer
	clearStorageKey(key StorageKey) error
	singleClearStorageKey(key StorageKey) error
}

type wrappedIntentWriter interface {
	ClearMVCCMeta(
		key roachpb.Key, state PrecedingIntentState, possiblyUpdated bool, txnUUID uuid.UUID) error
	PutMVCCMeta(
		key roachpb.Key, value []byte, state PrecedingIntentState, possiblyUpdated bool,
		txnUUID uuid.UUID) error
	ClearMVCCRangeAndIntents(start, end roachpb.Key) error
}

func possiblyMakeWrappedIntentWriter(w wrappableIntentWriter) wrappedIntentWriter {
	if disallowSeparatedIntents {
		return nil
	}
	return intentDemuxWriter{Writer: w}
}

// TODO: remove metaKind
type interleavedMetaWriter struct {
	Writer
	metaKind metaKind
}

var _ MetaWriter = &interleavedMetaWriter{}

func (imw *interleavedMetaWriter) Clear(key MVCCKey) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("bug: caller should call Clear{MVCC,Inline}Meta")
	}
	return imw.Writer.Clear(key)
}

func (imw *interleavedMetaWriter) ClearStorageKey(key StorageKey) error {
	panic("bug: caller should call Clear{MVCC,Inline}Meta")
}

func (imw *interleavedMetaWriter) SingleClear(key MVCCKey) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("bug: caller should call Clear{MVCC,Inline}Meta")
	}
	return imw.Writer.SingleClear(key)
}

func (imw *interleavedMetaWriter) SingleClearStorage(key StorageKey) error {
	panic("bug: caller should call Clear{MVCC,Inline}Meta")
}

func (imw *interleavedMetaWriter) Put(key MVCCKey, value []byte) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("bug: caller should call Put{MVCC,Inline}Meta")
	}
	return imw.Writer.Put(key, value)
}

func (imw *interleavedMetaWriter) PutStorage(key StorageKey, value []byte) error {
	panic("bug: caller should call Put{MVCC,Inline}Meta")
}

func (imw *interleavedMetaWriter) ClearMVCCMeta(
	key roachpb.Key, _ PrecedingIntentState, _ bool,
) error {
	if imw.metaKind == inlineMetaOnly {
		panic("bug: caller should not call ClearMVCCMeta")
	}
	return imw.Writer.Clear(MVCCKey{Key: key})
}

func (imw *interleavedMetaWriter) PutMVCCMeta(
	key roachpb.Key, value []byte, _ PrecedingIntentState, _ bool,
) error {
	if imw.metaKind == inlineMetaOnly {
		panic("bug: caller should not call PutMVCCMeta")
	}
	return imw.Writer.Put(MVCCKey{Key: key}, value)
}

func (imw *interleavedMetaWriter) ClearInlineMeta(key roachpb.Key) error {
	if imw.metaKind == mvccMetaOnly {
		panic("bug: caller should not call ClearInlineMeta")
	}
	return imw.Writer.Clear(MVCCKey{Key: key})
}

func (imw *interleavedMetaWriter) PutInlineMeta(key roachpb.Key, value []byte) error {
	if imw.metaKind == mvccMetaOnly {
		panic("bug: caller should not call PutInlineMeta")
	}
	return imw.Writer.Clear(MVCCKey{Key: key})
}

type intentDemuxWriter struct {
	Writer
	separatedIntents bool
	// txnUUID can be nil. In such a case the caller should not be
	// calling the *MVCCMeta methods.
	txnUUID *uuid.UUID
}

var _ MetaWriter = &intentDemuxWriter{}

func (idw *intentDemuxWriter) Clear(key MVCCKey) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("bug: caller should call ClearMVCCMeta")
	}
	return idw.Writer.Clear(key)
}

func (idw *intentDemuxWriter) ClearStorageKey(key StorageKey) error {
	panic("bug: caller should not call Clear")
}

func (idw *intentDemuxWriter) SingleClear(key MVCCKey) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("bug: caller should call ClearMVCCMeta")
	}
	return idw.Writer.SingleClear(key)
}

func (idw *intentDemuxWriter) SingleClearStorage(key StorageKey) error {
	panic("bug: caller should not call SingleClear")
}

func (idw *intentDemuxWriter) Put(key MVCCKey, value []byte) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("bug: caller should call PutMVCCMeta")
	}
	return idw.Writer.Put(key, value)
}

func (idw *intentDemuxWriter) PutStorage(key StorageKey, value []byte) error {
	panic("bug: caller should not call Put")
}

func (idw *intentDemuxWriter) ClearMVCCMeta(
	key roachpb.Key, state PrecedingIntentState, possiblyUpdated bool,
) error {
	if idw.txnUUID == nil {
		panic("bug: caller has not configured a txn for this meta")
	}
	switch state {
	case ExistingIntentInterleaved:
		return idw.Writer.Clear(MVCCKey{Key: key})
	case ExistingIntentSeparated:
		ltKey := StorageKey{
			key:    keys.LockTableKeyExclusive(key),
			suffix: (*idw.txnUUID)[:],
		}
		if possiblyUpdated {
			return idw.Writer.ClearStorageKey(ltKey)
		}
		return idw.Writer.SingleClearStorage(ltKey)
	default:
		panic("bug: caller used invalid state")
	}
}

func (idw *intentDemuxWriter) PutMVCCMeta(
	key roachpb.Key, value []byte, state PrecedingIntentState, precedingPossiblyUpdated bool,
) error {
	if idw.txnUUID == nil {
		panic("bug: caller has not configured a txn for this meta")
	}
	var ltKey StorageKey
	if state == ExistingIntentSeparated || idw.separatedIntents {
		ltKey = StorageKey{
			key:    keys.LockTableKeyExclusive(key),
			suffix: (*idw.txnUUID)[:],
		}
	}
	if state == ExistingIntentSeparated && !idw.separatedIntents {
		// Switching this intent to be interleaved.
		if precedingPossiblyUpdated {
			if err := idw.Writer.ClearStorageKey(ltKey); err != nil {
				return err
			}
		} else {
			if err := idw.Writer.SingleClearStorage(ltKey); err != nil {
				return err
			}
		}
	} else if state == ExistingIntentInterleaved && idw.separatedIntents {
		// Switching this intent to be separate.
		if err := idw.Writer.Clear(MVCCKey{Key: key}); err != nil {
			return err
		}
	}
	// Write intent
	if idw.separatedIntents {
		return idw.Writer.PutStorage(ltKey, value)
	}
	return idw.Writer.Put(MVCCKey{Key: key}, value)
}

func (idw *intentDemuxWriter) ClearInlineMeta(key roachpb.Key) error {
	panic("bug: caller should not call ClearInlineMeta")
}

func (idw *intentDemuxWriter) PutInlineMeta(key roachpb.Key, value []byte) error {
	panic("bug: caller should not call PutInlineMeta")
}

func (imw *interleavedMetaWriter) ClearRange(start, end MVCCKey) error {
	if start.Timestamp != (hlc.Timestamp{}) || end.Timestamp != (hlc.Timestamp{}) {
		panic("")
	}
	err := imw.Writer.ClearRange(start, end)
	if err != nil {
		return err
	}
	err = imw.Writer.ClearRangeStorage(StorageKey{key: keys.MakeLockTableKeyPrefix(start.Key)},
		StorageKey{key: keys.MakeLockTableKeyPrefix(end.Key)})
	return err
}

func (imw *interleavedMetaWriter) ClearRangeStorage(start, end StorageKey) error {
	if len(start.suffix) != 0 || len(end.suffix) != 0 {
		panic("")
	}
	return imw.ClearRange(MVCCKey{Key: start.key}, MVCCKey{Key: end.key})
}

// TODO: ClearIterMVCCRangeAndIntents should really be considered a higher-level method that we should
// remove from the Writer interface, or panic if called on the lower-level implementations,
// so that there is no change of accidental calling without a wrapped writer and iterator,
// in the other implementations.
func (imw *interleavedMetaWriter) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	// The iterator itself is a MetaIterator that will see intents and MVCC values, if
	// desired.
	if _, ok := iter.(MetaIterator); !ok {
		panic("")
	}
	// The deletion here is only correct if used on a pebbleBatch, since
	pb, ok := imw.Writer.(*pebbleBatch)
	if !ok {
		panic("")
	}
	if pb.distinctOpen {
		panic("distinct batch open")
	}

	// Note that this method has the side effect of modifying iter's bounds.
	// Since all calls to `ClearIterMVCCRangeAndIntents` are on new throwaway iterators with no
	// lower bounds, calling SetUpperBound should be sufficient and safe.
	// Furthermore, the start and end keys are always metadata keys (i.e.
	// have zero timestamps), so we can ignore the bounds' MVCC timestamps.
	iter.SetUpperBound(end)
	iter.SeekGE(MakeMVCCMetadataKey(start))

	for ; ; iter.Next() {
		valid, err := iter.Valid()
		if err != nil {
			return err
		} else if !valid {
			break
		}

		err = pb.batch.Delete(iter.UnsafeRawKeyDangerous(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// wrappableReader is used to implement a wrapped Reader. A wrapped Reader
// should be used and immediately discarded. It maintains no state of its own
// between calls.
//
// TODO: for allocation optimization we may need to provide expose a scratch
// space struct that the caller keeps on behalf of the wrapped reader. But
// can only do such an optimization when know that the wrappableReader will
// be used with external synchronization that prevents preallocated buffers
// from being modified concurrently. pebbleBatc.{Get,GetProto} have MVCCKey
// serialization allocation optimizations which we can't do below. But those
// are probably not performance sensitive.
//
// Why do we not keep the wrapped reader as a member in the caller? Because
// different methods on Reader can need different wrappings depending on what
// they want to observe.
type wrappableReader interface {
	rawGet(key []byte) (value []byte, err error)
	// Only MVCCKeyIterKind and StorageKeyIterKind are permitted.
	// This iterator will not make separated locks appear as interleaved.
	newIterator(opts IterOptions, iterKind IterKind)
}

func possiblyWrapReader(r Reader, iterKind IterKind) (Reader, bool) {
	if disallowSeparatedIntents || iterKind == MVCCKeyIterKind || iterKind == StorageKeyIterKind {
		return r
	}
	if iterKind != StorageKeyIterKind {
		panic("unknown iterKind")
	}
	wr := r.(wrappableReader)
	return interleaveSeparatedMetaReader{r: wr}
}

type interleaveSeparatedMetaReader struct {
	// Reader is not an anonymous field, even though it forces us to explicitly
	// wrap Close and Closed below since we don't want to risk a new function
	// being added to the interface that we forget to wrap.
	r wrappableReader
}

var _ Reader = interleaveSeparatedMetaReader{}

func (imr interleaveSeparatedMetaReader) Close() {
	panic("")
}

func (imr interleaveSeparatedMetaReader) Closed() bool {
	panic("")
}

// ExportToSst is part of the Reader interface.
func (imr interleaveSeparatedMetaReader) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize uint64,
	maxSize uint64,
	io IterOptions,
) (sst []byte, _ roachpb.BulkOpSummary, resumeKey roachpb.Key, _ error) {
	return pebbleExportToSst(
		imr, startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
}

// Get implements the Reader interface.
func (imr interleaveSeparatedMetaReader) Get(key MVCCKey) ([]byte, error) {
	if key.Timestamp != (hlc.Timestamp{}) {
		return imr.r.Get(key)
	}
	// The meta may be interleaved, so try that.
	value, err := imr.r.Get(key)
	if value != nil || err != nil {
		return value, err
	}
	// The meta could be in the lock table. Constructing an Iterator for each
	// Get is not efficient, but this function is deprecated and only used for
	// tests, so we don't care.
	iter := imr.r.NewIterator(IterOptions{Prefix: true, LowerBound: key.Key}, MVCCKeyAndIntentsIterKind)
	iter.SeekStorageGE(StorageKey{key: key.Key})
	valid, err := iter.Valid()
	if !valid || err != nil {
		return nil, err
	}
	value = iter.Value()
	iter.Close()
	return value, nil
}

func (imr *interleaveSeparatedMetaReader) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	val, err := imr.Get(key)
	if err != nil || val == nil {
		return false, 0, 0, err
	}
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	err = protoutil.Unmarshal(val, msg)
	return true, keyBytes, valBytes, err
}

func (imr *interleaveSeparatedMetaReader) Iterate(
	start, end roachpb.Key, seeIntents bool, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(imr, start, end, f)
}

// The Iterator returned also implements MetaIterator.
func (imr *interleaveSeparatedMetaReader) NewIterator(
	opts IterOptions, iterKind IterKind,
) Iterator {
	if !opts.MinTimestampHint.IsEmpty() || !opts.MaxTimestampHint.IsEmpty() {
		// Creating a time bound iterator, so don't need to see intents.
		return trivialMetaIter{Iterator: imr.r.NewIterator(opts, MVCCKeyAndIntentsIterKind)}
	}
	return newIntentInterleavingIterator(imr.r, opts)
}

func (imr *interleaveSeparatedMetaReader) metaReaderMarker() {}

// A trivial implementation that passes through to Reader and implements the
// marker function.
type trivialMetaReader struct {
	Reader
}

var _ MetaReader = trivialMetaReader{}

// The Iterator returned also implements MetaIterator.
func (tmr trivialMetaReader) NewIterator(opts IterOptions, iterKind IterKind) Iterator {
	return trivialMetaIter{Iterator: tmr.Reader.NewIterator(opts, MVCCKeyAndIntentsIterKind)}
}

func (tmr trivialMetaReader) metaReaderMarker() {}
