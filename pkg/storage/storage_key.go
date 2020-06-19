package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

type StorageKey struct {
	key    []byte
	suffix []byte
}

// Format implements the fmt.Formatter interface
func (k StorageKey) Format(f fmt.State, c rune) {
	fmt.Fprintf(f, "%s/%x", roachpb.Key(k.key), k.suffix)
}

const (
	sentinelLen            = 1
	suffixEncodedLengthLen = 1
)

func (k StorageKey) EncodedLen() int {
	n := len(k.key) + suffixEncodedLengthLen
	suffixLen := len(k.suffix)
	if suffixLen > 0 {
		n += sentinelLen + suffixLen
	}
	return n
}

func (k StorageKey) Encode() []byte {
	encodedLen := k.EncodedLen()
	buf := make([]byte, encodedLen)
	k.encodeToSizedBuf(buf)
	return buf
}

func (k StorageKey) EncodeToBuf(buf []byte) []byte {
	encodedLen := k.EncodedLen()
	if cap(buf) < encodedLen {
		buf = make([]byte, encodedLen)
	} else {
		buf = buf[:encodedLen]
	}
	k.encodeToSizedBuf(buf)
	return buf
}

func (k StorageKey) encodeToSizedBuf(buf []byte) {
	copy(buf, k.key)
	pos := len(k.key)
	suffixLen := len(k.suffix)
	if suffixLen > 0 {
		buf[pos] = 0
		pos += sentinelLen
		copy(buf[pos:], k.suffix)
	}
	buf[len(buf)-1] = byte(suffixLen)
}

func DecodeStorageKey(key []byte) (skey StorageKey, ok bool) {
	if len(key) == 0 {
		return StorageKey{}, false
	}
	suffixLen := int(key[len(key)-1])
	keyPartEnd := len(key) - 1 - suffixLen
	if keyPartEnd < 0 {
		return StorageKey{}, false
	}

	skey.key = key[:keyPartEnd]
	if suffixLen > 0 {
		skey.suffix = key[keyPartEnd+1 : len(key)-1]
	}
	return skey, true
}

func IsMVCCKey(skey StorageKey) bool {
	return len(skey.suffix) != 16
}

func StorageKeyToMVCCKey(skey StorageKey) (MVCCKey, error) {
	k := MVCCKey{Key: skey.key}
	switch len(skey.suffix) {
	case 0:
		// No-op.
	case 8:
		k.Timestamp.WallTime = int64(binary.BigEndian.Uint64(skey.suffix[0:8]))
	case 12:
		k.Timestamp.WallTime = int64(binary.BigEndian.Uint64(skey.suffix[0:8]))
		k.Timestamp.Logical = int32(binary.BigEndian.Uint32(skey.suffix[8:12]))
	default:
		return k, errors.Errorf(
			"invalid mvcc key: %x bad timestamp %x", skey.key, skey.suffix)
	}
	return k, nil
}

// storageKeyFormatter is an fmt.Formatter for storage keys.
type storageKeyFormatter struct {
	skey StorageKey
}

var _ fmt.Formatter = storageKeyFormatter{}

// Format implements the fmt.Formatter interface.
func (m storageKeyFormatter) Format(f fmt.State, c rune) {
	m.skey.Format(f, c)
}
