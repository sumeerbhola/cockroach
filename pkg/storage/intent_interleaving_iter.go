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
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Used for inline etc.
type trivialMetaIter struct {
	Iterator
}

func (tmi trivialMetaIter) isCurMetaSeparated() bool {
	return false
}

func (tmi trivialMetaIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsGo(tmi.Iterator, start, end, nowNanos)
}

type SimplePlusIterator interface {
	SimpleIterator
	Key() MVCCKey
	Value() []byte
}

// Can handle inline too.
type intentInterleavingIter struct {
	prefix     bool
	iter       Iterator
	intentIter Iterator
	// The decoded key from the lock table.
	intentKey roachpb.Key
	// - cmp output of (intentKey, current iter key) when both are non-nil.
	// - intentKey==nil, current iter key!=nil, cmp=dir
	// - intentKey!=nil, current iter key==nil, cmp=-dir
	// If both are nil? valid=false
	intentCmp int

	// The current direction. +1 for forward, -1 for reverse.
	dir   int
	valid bool
	err   error
}

func newIntentInterleavingIterator(reader Reader, opts IterOptions) MetaIterator {
	// TODO: is there a case where mvcc iteration is happening without both lower and
	// upper bounds? Perhaps when doing a Get -- but even then we should have an upper
	// bound so that seekPrefixGE doesn't go into some other part of the key space.
	if opts.UpperBound == nil {
		panic("TODO")
	}
	intentOpts := opts
	if opts.LowerBound != nil {
		intentOpts.LowerBound = keys.MakeLockTableKeyPrefix(opts.LowerBound)
	}
	intentOpts.UpperBound = keys.MakeLockTableKeyPrefix(opts.UpperBound)
	intentIter := reader.NewIterator(intentOpts, MVCCKeyAndIntentsIterKind)
	iter := reader.NewIterator(opts, MVCCKeyAndIntentsIterKind)
	return &intentInterleavingIter{
		prefix:     opts.Prefix,
		iter:       iter,
		intentIter: intentIter,
	}
}

// Always has timestamp 0.
func (i *intentInterleavingIter) SeekGE(key MVCCKey) {
	i.dir = +1
	i.valid = true
	i.err = nil

	intentSeekKey := keys.MakeLockTableKeyPrefix(key.Key)
	// TODO: when prefix=true, the underlying intentIter is also a prefix iter
	// and not do the right thing until we fix things to move the txnID into the
	// timestamp part. Use StorageKey
	i.intentIter.SeekGE(MVCCKey{Key: intentSeekKey})
	if err := i.tryDecodeLockedKey(); err != nil {
		return
	}
	i.iter.SeekGE(key)
	i.extract()
}

func (i *intentInterleavingIter) SeekStorageGE(key StorageKey) {
	panic("called should not call SeekGE")
}

func (i *intentInterleavingIter) extract() {
	valid, err := i.iter.Valid()
	if err != nil || (!valid && i.intentKey == nil) {
		i.err = err
		i.valid = false
		return
	}
	// err == nil && (valid || i.intentKey != nil)
	if !valid {
		i.intentCmp = -i.dir
	} else if i.intentKey == nil {
		i.intentCmp = i.dir
	} else {
		// TODO: we can optimize away some comparisons if we use the fact
		// that each intent needs to have a provisional value.
		i.intentCmp = i.intentKey.Compare(i.iter.UnsafeKey().Key)
	}
}

func (i *intentInterleavingIter) tryDecodeLockedKey() error {
	valid, err := i.intentIter.Valid()
	if err != nil {
		i.err = err
		i.valid = false
		return err
	}
	if !valid {
		i.intentKey = nil
		return nil
	}
	i.intentKey, _, _, err = keys.DecodeLockTableKey(i.intentIter.UnsafeStorageKey().key)
	if err != nil {
		i.err = err
		i.valid = false
		return err
	}
	return nil
}

func (i *intentInterleavingIter) Valid() (bool, error) {
	return i.valid, i.err
}

func (i *intentInterleavingIter) Next() {
	if i.err != nil {
		return
	}
	if i.dir < 0 {
		// Switching from reverse to forward iteration.
		isCurAtIntent := i.isCurAtIntentIter()
		i.dir = +1
		if !i.valid {
			// Both iterators are exhausted, so step both forward.
			i.valid = true
			i.intentIter.Next()
			if err := i.tryDecodeLockedKey(); err != nil {
				return
			}
			i.iter.Next()
			i.extract()
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// iter precedes the intent, so must be at the highest version of the preceding
			// key or exhausted. So step it forward. It will now point to a
			// key that is the same as the intent key since an intent always has a
			// corresponding provisional value.
			// TODO: add a debug mode assertion of the above invariant.
			i.iter.Next()
			i.intentCmp = 0
			if _, err := i.iter.Valid(); err != nil {
				i.err = err
				i.valid = false
				return
			}
		} else {
			// The intent precedes the iter. It could be for the same key, iff this key has an intent,
			// or an earlier key. Either way, stepping forward will take it to an intent for a later
			// key.
			// TODO: add a debug mode assertion of the above invariant.
			i.intentIter.Next()
			i.intentCmp = +1
			if err := i.tryDecodeLockedKey(); err != nil {
				return
			}
		}
	}
	if !i.valid {
		return
	}
	if i.intentCmp <= 0 {
		// Currently, there is at most 1 intent for a key, so doing Next() is correct.
		i.intentIter.Next()
		if err := i.tryDecodeLockedKey(); err != nil {
			return
		}
		i.extract()
	} else {
		i.iter.Next()
		i.extract()
	}
}

func (i *intentInterleavingIter) NextKey() {
	// NextKey is not called to switch directions, i.e., we must already
	// be in the forward direction.
	if i.dir < 0 {
		panic("")
	}
	if !i.valid {
		return
	}
	if i.intentCmp <= 0 {
		// Currently, there is at most 1 intent for a key, so doing Next() is correct.
		i.intentIter.Next()
		if err := i.tryDecodeLockedKey(); err != nil {
			return
		}
		// We can do this because i.intentCmp == 0. TODO: enforce invariant.
		i.iter.NextKey()
		i.extract()
	} else {
		i.iter.NextKey()
		i.extract()
	}
}

func (i *intentInterleavingIter) isCurAtIntentIter() bool {
	return (i.dir > 0 && i.intentCmp <= 0) || (i.dir < 0 && i.intentCmp > 0)
}

func (i *intentInterleavingIter) UnsafeKey() MVCCKey {
	// Note that if there is a separated intent there cannot also be an interleaved
	// intent for the same key.
	if i.isCurAtIntentIter() {
		return MVCCKey{Key: i.intentKey}
	} else {
		return i.iter.UnsafeKey()
	}
}

func (i *intentInterleavingIter) UnsafeStorageKey() StorageKey {
	panic("called should not call UnsafeStorageKey")
}

func (i *intentInterleavingIter) UnsafeValue() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.UnsafeValue()
	} else {
		return i.iter.UnsafeValue()
	}
}

func (i *intentInterleavingIter) Key() MVCCKey {
	unsafeKey := i.UnsafeKey()
	return MVCCKey{Key: append(roachpb.Key(nil), unsafeKey.Key...), Timestamp: unsafeKey.Timestamp}
}

func (i *intentInterleavingIter) StorageKey() StorageKey {
	panic("called should not call StorageKey")
}

func (i *intentInterleavingIter) Value() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.Value()
	} else {
		return i.iter.Value()
	}
}

func (i *intentInterleavingIter) Close() {
	i.iter.Close()
	i.intentIter.Close()
}

func (i *intentInterleavingIter) SeekLT(key MVCCKey) {
	i.dir = -1
	i.valid = true
	i.err = nil

	intentSeekKey := keys.MakeLockTableKeyPrefix(key.Key)
	// TODO: Use StorageKey.
	i.intentIter.SeekLT(MVCCKey{Key: intentSeekKey})
	if err := i.tryDecodeLockedKey(); err != nil {
		return
	}
	i.iter.SeekLT(key)
	i.extract()
}

func (i *intentInterleavingIter) SeekStorageLT(key StorageKey) {
	panic("called should not call SeekLT")
}

func (i *intentInterleavingIter) Prev() {
	if i.err != nil {
		return
	}
	if i.dir > 0 {
		// Switching from forward to reverse iteration.
		isCurAtIntent := i.isCurAtIntentIter()
		i.dir = -1
		if !i.valid {
			// Both iterators are exhausted, so step both backward.
			i.valid = true
			i.intentIter.Prev()
			if err := i.tryDecodeLockedKey(); err != nil {
				return
			}
			i.iter.Prev()
			i.extract()
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// iter is after the intent, so must be at the provisional value. So step it backward.
			// It will now point to a key that is before the intent key.
			// TODO: add a debug mode assertion of the above invariant.
			i.iter.Prev()
			i.intentCmp = +1
			if _, err := i.iter.Valid(); err != nil {
				i.err = err
				i.valid = false
				return
			}
		} else {
			// The intent is after the iter. We don't know whether the iter key has an intent.
			i.intentIter.Prev()
			if err := i.tryDecodeLockedKey(); err != nil {
				return
			}
			i.extract()
		}
	}
	if !i.valid {
		return
	}
	if i.intentCmp > 0 {
		// The current position is at the intent.
		// Currently, there is at most 1 intent for a key, so doing Prev() is correct.
		i.intentIter.Prev()
		if err := i.tryDecodeLockedKey(); err != nil {
			return
		}
		i.extract()
	} else {
		i.iter.Prev()
		i.extract()
	}
}

// UnsafeRawKeyDangerous returns the current raw key (i.e. the encoded MVCC key),
// from the underlying Pebble iterator. Note, this should be carefully
// used since it does not transform the lock table key.
func (i *intentInterleavingIter) UnsafeRawKeyDangerous() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.Value()
	} else {
		return i.iter.Value()
	}
}

func (i *intentInterleavingIter) ValueProto(msg protoutil.Message) error {
	value := i.UnsafeValue()
	return protoutil.Unmarshal(value, msg)
}

func (i *intentInterleavingIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsGo(i, start, end, nowNanos)
}

func (i *intentInterleavingIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	// TODO: refactor pebbleIterator.FindSplitKey and reuse. Should we bother
	// interleaving the separated intents? Probably safer since don't want to
	// not notice intents in a range that is big primarily due to intents.
	panic("")
}

func (i *intentInterleavingIter) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return checkForKeyCollisionsGo(i, sstData, start, end)
}

func (i *intentInterleavingIter) SetUpperBound(roachpb.Key) {
	// TODO
	panic("")
}

func (i *intentInterleavingIter) Stats() IteratorStats {
	// TODO: see who calls this and whether it should even be in this interface
	// given that only TimeBoundNumSSTs are reported. Only used in tests!
	// Combine the stats of the underlying iters?
	panic("")
}

func (i *intentInterleavingIter) SupportsPrev() bool {
	return true
}

func (i *intentInterleavingIter) isCurMetaSeparated() bool {
	return i.isCurAtIntentIter()
}
