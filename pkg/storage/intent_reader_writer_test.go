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
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func readPrecedingIntentState(t *testing.T, d *datadriven.TestData) PrecedingIntentState {
	var str string
	d.ScanArgs(t, "preceding", &str)
	switch str {
	case "interleaved":
		return ExistingIntentInterleaved
	case "separated":
		return ExistingIntentSeparated
	case "none":
		return NoExistingIntent
	}
	panic("unknown state")
}

func readTxnDidNotUpdateMeta(t *testing.T, d *datadriven.TestData) bool {
	var txnDidNotUpdateMeta bool
	d.ScanArgs(t, "txn-did-not-update-meta", &txnDidNotUpdateMeta)
	return txnDidNotUpdateMeta
}

func printMeta(meta *enginepb.MVCCMetadata) string {
	uuid := meta.Txn.ID.ToUint128()
	var hiStr string
	if uuid.Hi != 0 {
		hiStr = fmt.Sprintf("%d,", uuid.Hi)
	}
	return fmt.Sprintf("meta{ts: %s, txn: %s%d}", meta.Timestamp.String(), hiStr, uuid.Lo)
}

func printLTKey(k LockTableKey) string {
	var id uuid.UUID
	copy(id[:], k.TxnUUID[0:uuid.Size])
	idInt := id.ToUint128()
	var hiStr string
	if idInt.Hi != 0 {
		hiStr = fmt.Sprintf("%d,", idInt.Hi)
	}
	return fmt.Sprintf("LT{k: %s, strength: %s, uuid:%s%d}",
		string(k.Key), k.Strength, hiStr, idInt.Lo)
}

func printEngContents(b *strings.Builder, eng Engine) {
	iter := eng.NewEngineIterator(IterOptions{UpperBound: roachpb.KeyMax})
	defer iter.Close()
	fmt.Fprintf(b, "=== Storage contents ===\n")
	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: []byte("")})
	for valid || err != nil {
		if err != nil {
			fmt.Fprintf(b, "error: %s\n", err.Error())
			break
		}
		var key EngineKey
		if key, err = iter.UnsafeEngineKey(); err != nil {
			fmt.Fprintf(b, "error: %s\n", err.Error())
			break
		}
		var meta enginepb.MVCCMetadata
		if err := protoutil.Unmarshal(iter.UnsafeValue(), &meta); err != nil {
			fmt.Fprintf(b, "error: %s\n", err.Error())
			break
		}
		if key.IsMVCCKey() {
			var k MVCCKey
			if k, err = key.ToMVCCKey(); err != nil {
				fmt.Fprintf(b, "error: %s\n", err.Error())
				break
			}
			fmt.Fprintf(b, "k: %s, v: %s\n", k, printMeta(&meta))
		} else {
			var k LockTableKey
			if k, err = key.ToLockTableKey(); err != nil {
				fmt.Fprintf(b, "error: %s\n", err.Error())
				break
			}
			fmt.Fprintf(b, "k: %s, v: %s\n", printLTKey(k), printMeta(&meta))
		}
		valid, err = iter.NextEngineKey()
	}
}

// printWriter wraps Writer and writes the method call to the strings.Builder.
type printWriter struct {
	Writer
	b strings.Builder
}

var _ Writer = &printWriter{}

func (p *printWriter) reset() {
	p.b.Reset()
	fmt.Fprintf(&p.b, "=== Calls ===\n")
}

func (p *printWriter) ClearUnversioned(key roachpb.Key) error {
	fmt.Fprintf(&p.b, "ClearUnversioned(%s)\n", string(key))
	return p.Writer.ClearUnversioned(key)
}

func (p *printWriter) ClearEngineKey(key EngineKey) error {
	ltKey, err := key.ToLockTableKey()
	var str string
	if err != nil {
		fmt.Fprintf(&p.b, "ClearEngineKey param is not a lock table key: %s\n", key)
		str = fmt.Sprintf("%s", key)
	} else {
		str = printLTKey(ltKey)
	}
	fmt.Fprintf(&p.b, "ClearEngineKey(%s)\n", str)
	return p.Writer.ClearEngineKey(key)
}

func (p *printWriter) ClearRawRange(start, end roachpb.Key) error {
	if bytes.HasPrefix(start, keys.LocalRangeLockTablePrefix) {
		ltStart, err := keys.DecodeLockTableSingleKey(start)
		if err != nil {
			fmt.Fprintf(&p.b, "ClearRawRange start is not a lock table key: %s\n", start)
		}
		ltEnd, err := keys.DecodeLockTableSingleKey(end)
		if err != nil {
			fmt.Fprintf(&p.b, "ClearRawRange end is not a lock table key: %s\n", end)
		}
		fmt.Fprintf(&p.b, "ClearRawRange(LT{%s}, LT{%s})\n", string(ltStart), string(ltEnd))
	} else {
		fmt.Fprintf(&p.b, "ClearRawRange(%s, %s)\n", string(start), string(end))
	}
	return p.Writer.ClearRawRange(start, end)
}

func (p *printWriter) PutUnversioned(key roachpb.Key, value []byte) error {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(value, &meta); err != nil {
		fmt.Fprintf(&p.b, "PutUnversioned value param is not MVCCMetadata: %s\n", err)
	} else {
		fmt.Fprintf(&p.b, "PutUnversioned(%s, %s)\n", string(key), printMeta(&meta))
	}
	return p.Writer.PutUnversioned(key, value)
}

func (p *printWriter) PutEngineKey(key EngineKey, value []byte) error {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(value, &meta); err != nil {
		fmt.Fprintf(&p.b, "PutEngineKey value param is not MVCCMetadata: %s\n", err)
		return p.Writer.PutEngineKey(key, value)
	}
	ltKey, err := key.ToLockTableKey()
	var keyStr string
	if err != nil {
		fmt.Fprintf(&p.b, "ClearEngineKey param is not a lock table key: %s\n", key)
		keyStr = fmt.Sprintf("%s", key)
	} else {
		keyStr = printLTKey(ltKey)
	}
	fmt.Fprintf(&p.b, "PutEngineKey(%s, %s)\n", keyStr, printMeta(&meta))
	return p.Writer.PutEngineKey(key, value)
}

func (p *printWriter) SingleClearEngineKey(key EngineKey) error {
	ltKey, err := key.ToLockTableKey()
	var str string
	if err != nil {
		fmt.Fprintf(&p.b, "SingleClearEngineKey param is not a lock table key: %s\n", key)
		str = fmt.Sprintf("%s", key)
	} else {
		str = printLTKey(ltKey)
	}
	fmt.Fprintf(&p.b, "SingleClearEngineKey(%s)\n", str)
	return p.Writer.SingleClearEngineKey(key)
}

func TestIntentDemuxWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := createTestPebbleEngine()
	defer eng.Close()
	pw := printWriter{Writer: eng}
	var w intentDemuxWriter
	var scratch []byte
	var err error
	datadriven.RunTest(t, "testdata/intent_demux_writer",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-writer":
				var separated bool
				d.ScanArgs(t, "enable-separated", &separated)
				w = intentDemuxWriter{w: &pw, enabledSeparatedIntents: separated}
				return ""
			case "put-intent":
				pw.reset()
				key := scanRoachKey(t, d, "k")
				// We don't bother populating most fields in the proto.
				var meta enginepb.MVCCMetadata
				var tsS string
				d.ScanArgs(t, "ts", &tsS)
				ts, err := hlc.ParseTimestamp(tsS)
				if err != nil {
					t.Fatalf("%v", err)
				}
				meta.Timestamp = ts.ToLegacyTimestamp()
				var txn int
				d.ScanArgs(t, "txn", &txn)
				txnUUID := uuid.FromUint128(uint128.FromInts(0, uint64(txn)))
				meta.Txn = &enginepb.TxnMeta{ID: txnUUID}
				val, err := protoutil.Marshal(&meta)
				if err != nil {
					return err.Error()
				}
				state := readPrecedingIntentState(t, d)
				txnDidNotUpdateMeta := readTxnDidNotUpdateMeta(t, d)
				scratch, err = w.PutIntent(key, val, state, txnDidNotUpdateMeta, txnUUID, scratch)
				if err != nil {
					return err.Error()
				}
				printEngContents(&pw.b, eng)
				return pw.b.String()
			case "clear-intent":
				pw.reset()
				key := scanRoachKey(t, d, "k")
				var txn int
				d.ScanArgs(t, "txn", &txn)
				txnUUID := uuid.FromUint128(uint128.FromInts(0, uint64(txn)))
				state := readPrecedingIntentState(t, d)
				txnDidNotUpdateMeta := readTxnDidNotUpdateMeta(t, d)
				scratch, err = w.ClearIntent(key, state, txnDidNotUpdateMeta, txnUUID, scratch)
				if err != nil {
					return err.Error()
				}
				printEngContents(&pw.b, eng)
				return pw.b.String()
			case "clear-range":
				pw.reset()
				start := scanRoachKey(t, d, "start")
				end := scanRoachKey(t, d, "end")
				if scratch, err = w.ClearMVCCRangeAndIntents(start, end, scratch); err != nil {
					return err.Error()
				}
				printEngContents(&pw.b, eng)
				return pw.b.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestBug(t *testing.T) {
	defer leaktest.AfterTest(t)()

	pebbleOpts := DefaultPebbleOptions()
	pebbleOpts.DisableWAL = true
	pebbleOpts.ReadOnly = true
	cfg := PebbleConfig{
		Opts: pebbleOpts,
	}
	cfg.StorageConfig = base.StorageConfig{
		Dir:       "/Users/sumeer/changefeed/engine1/7815618424818949142",
		MustExist: true,
	}
	ctx := context.Background()
	eng, err := NewPebble(ctx, cfg)
	require.NoError(t, err)
	defer eng.Close()

	reader := eng.NewReadOnly().(*pebbleReadOnly)
	defer reader.Close()

	key := keys.SystemSQLCodec.IndexPrefix(53, 1)
	endKey := keys.SystemSQLCodec.IndexPrefix(53, 2)
	log.Infof(ctx, "[%s, %s), [%x, %x)", key.String(), endKey.String(), key, endKey)
	key0 := roachpb.Key([]byte{0xbd, 0x89, 0x88, 0x88})
	key2 := roachpb.Key([]byte{0xbd, 0x89, 0x8a, 0x88})
	log.Infof(ctx, "key0: %s, key2: %s", key0.String(), key2.String())

	uuid := uuid.FromUint128(uint128.FromInts(1, 1))
	// 1612928388296966000.0000000000
	meta := enginepb.MVCCMetadata{
		Txn: &enginepb.TxnMeta{
			ID:             uuid,
			WriteTimestamp: hlc.Timestamp{WallTime: 1612928388296966000},
		},
		Timestamp:           hlc.LegacyTimestamp{WallTime: 1612928388296966000},
		Deleted:             false,
		KeyBytes:            2,
		ValBytes:            2,
		RawBytes:            nil,
		IntentHistory:       nil,
		MergeTimestamp:      nil,
		TxnDidNotUpdateMeta: nil,
	}
	metaBytes, err := protoutil.Marshal(&meta)
	require.NoError(t, err)
	require.Less(t, 0, len(metaBytes))
	// intentWriter, ok := tryWrapIntentWriter(eng)
	// require.True(t, ok)
	// _, err =
	//	intentWriter.PutIntent(key0, metaBytes, NoExistingIntent, true, uuid, nil)
	engineKey, _ := LockTableKey{
		Key:      key0,
		Strength: lock.Exclusive,
		TxnUUID:  uuid[:],
	}.ToEngineKey(nil)
	err = eng.db.Set(engineKey.Encode(), metaBytes, pebble.NoSync)
	// err = eng.db.Set(EncodeKey(MVCCKey{Key: key0}), metaBytes, pebble.NoSync)
	require.NoError(t, err)

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer pebbleMVCCScannerPool.Put(mvccScanner)

	/*
		I210210 03:39:38.624497 49149 storage/mvcc.go:2963  [n1,s1,r36/1:/Table/5{3-4}] 70142  logLogicalOp 2: key: /Table/53/1/2/0, ts: 1612928378591302000.0000000001 pre: 1, tdnum: false bd898a88
		I210210 03:39:38.651209 37435 storage/mvcc.go:2995  [n1,s1,r36/1:/Table/5{3-4}] 70170  logLogicalOp 4: key: /Table/53/1/2/0, pre: 1, tdnum: false

		I210210 03:39:48.297650 37435 storage/mvcc.go:1747  [n1,s1,r36/1:/Table/5{3-4}] 75618  logLogicalOp 1: key: /Table/53/1/0/0, ts: 1612928388296966000.0000000000, val: 0:1612928388296966000.0000000000 1:x 2:x pre: 2, tdnum: true bd898888
		I210210 03:39:48.348054 51578 storage/mvcc.go:2963  [n1,s1,r36/1:/Table/5{3-4}] 75646  logLogicalOp 3: key: /Table/53/1/0/0, ts: 1612928388296966000.0000000000 pre: 1, tdnum: true bd898888


		I210210 03:39:48.350530 51858 storage/mvcc.go:2558  [n1,s1,r36/1:/Table/5{3-4}] 75658  MVCCScanToBytes [/Table/53/1, /Table/53/2) ts: 1612928347659843999.2147483647 max: 10000,10485760
		I210210 03:39:48.350799 51858 storage/pebble_mvcc_scanner.go:198  [-] 75659  MVCCScanner.scan [/Table/53/1, /Table/53/2), reverse: false, ts: 1612928347659843999.2147483647, fomr: false, cu: false
		I210210 03:39:48.350866 51858 storage/pebble_mvcc_scanner.go:369  [-] 75660  getAndAdvance encountered intent for /Table/53/1/0/0
		I210210 03:39:48.350927 51858 storage/pebble_mvcc_scanner.go:707  [-] 75661  seekVersion encountered k: /Table/53/1/0/0 ts: 1612928388296966000.0000000000
		I210210 03:39:48.350954 51858 storage/pebble_mvcc_scanner.go:707  [-] 75662  seekVersion encountered k: /Table/53/1/0/0 ts: 1612928378464494000.0000000000
		I210210 03:39:48.350975 51858 storage/pebble_mvcc_scanner.go:707  [-] 75663  seekVersion encountered k: /Table/53/1/0/0 ts: 1612928377971235000.0000000000
		I210210 03:39:48.350996 51858 storage/pebble_mvcc_scanner.go:707  [-] 75664  seekVersion encountered k: /Table/53/1/0/0 ts: 1612928376620809000.0000000000
		I210210 03:39:48.351017 51858 storage/pebble_mvcc_scanner.go:707  [-] 75665  seekVersion encountered k: /Table/53/1/0/0 ts: 1612928375144313000.0000000000
		I210210 03:39:48.351056 51858 storage/pebble_mvcc_scanner.go:682  [-] 75666  MVCCScanner:addAndAdvance empty val: k: /Table/53/1/3/0,1612928338963925000.0000000000 v:

		[/Table/53/1, /Table/53/2)
		/Table/53/1/2/0 bd898a88
		/Table/53/1/0/0 bd898888
	*/
	ts := hlc.Timestamp{WallTime: 1612928347659843999, Logical: 2147483647}
	opts := MVCCScanOptions{
		TargetBytes:      10485760,
		MaxKeys:          10000,
		Inconsistent:     false,
		FailOnMoreRecent: false,
		Tombstones:       false,
	}
	_, err = MVCCScanToBytes(ctx, reader, key, endKey, ts, opts)
	require.NoError(t, err)
}
