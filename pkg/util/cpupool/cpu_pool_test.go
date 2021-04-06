// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cpupool

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type TestingSlotRequester struct {
	numWaitingWork    int
	grantedSlots      int
	grantedSlotsCount int
	granter           SlotGranter
}

func (r *TestingSlotRequester) NumWaitingWork() int {
	return r.numWaitingWork
}

func (r *TestingSlotRequester) GrantWorkSlot() bool {
	if r.numWaitingWork > 0 {
		r.grantedSlots++
		r.numWaitingWork--
		r.grantedSlotsCount++
		return true
	}
	return false
}

func TestCPUPoolBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	pool := NewCPUPool([3]int{1, 1, 1}, [3]int{3, 3, 3}, 2)
	var requesters [SQLStatementRootStartWork + 1]TestingSlotRequester
	for i := range requesters {
		requesters[i].granter = pool.Register(WorkKind(i), &requesters[i])
	}
	require.True(t, requesters[0].granter.TryGetSlot())
	requesters[0].grantedSlots++
	requesters[0].grantedSlotsCount++
	require.False(t, requesters[1].granter.TryGetSlot())
	require.False(t, requesters[2].granter.TryGetSlot())
	require.True(t, requesters[3].granter.TryGetSlot())
	requesters[3].grantedSlots++
	requesters[3].grantedSlotsCount++
	require.False(t, requesters[3].granter.TryGetSlot())
	require.True(t, requesters[4].granter.TryGetSlot())
	requesters[4].grantedSlots++
	requesters[4].grantedSlotsCount++
	require.False(t, requesters[4].granter.TryGetSlot())

	for i := range requesters {
		requesters[i].numWaitingWork = 1
	}
	requesters[0].grantedSlots--
	requesters[0].granter.ReturnWorkSlot()
	require.Equal(t, 2, requesters[0].grantedSlotsCount)
	require.Equal(t, 0, requesters[1].grantedSlotsCount)
	require.Equal(t, 0, requesters[2].grantedSlotsCount)
	requesters[2].numWaitingWork = 2
	requesters[0].grantedSlots--
	requesters[0].granter.ReturnWorkSlot()
	require.Equal(t, 2, requesters[0].grantedSlotsCount)
	require.Equal(t, 1, requesters[1].grantedSlotsCount)
	require.Equal(t, 1, requesters[2].grantedSlotsCount)
	require.Equal(t, 0, requesters[1].numWaitingWork)
	require.Equal(t, 1, requesters[2].numWaitingWork)
	require.True(t, requesters[1].granter.TryGetSlot())
	require.True(t, requesters[1].granter.TryGetSlot())
	requesters[1].grantedSlots += 2
	requesters[1].grantedSlotsCount += 2
	require.Equal(t, 2, requesters[0].grantedSlotsCount)
	require.Equal(t, 3, requesters[1].grantedSlotsCount)
	require.Equal(t, 1, requesters[2].grantedSlotsCount)
	pool.AdjustSlotsUsingLoad(2)
	require.Equal(t, 2, requesters[0].grantedSlotsCount)
	require.Equal(t, 3, requesters[1].grantedSlotsCount)
	require.Equal(t, 2, requesters[2].grantedSlotsCount)
	require.Equal(t, 1, pool.queueTokens)

	requesters[0].granter.TookSlot()
	requesters[0].grantedSlots++
	requesters[0].grantedSlotsCount++
	requesters[0].granter.TookSlot()
	requesters[0].grantedSlots++
	requesters[0].grantedSlotsCount++
	require.Equal(t, 2, pool.usedSlots[0])

	require.False(t, requesters[2].granter.TryGetSlot())
	requesters[2].numWaitingWork = 1
	requesters[0].granter.ReturnWorkSlot()
	requesters[0].grantedSlots--
	require.Equal(t, 4, requesters[0].grantedSlotsCount)
	require.Equal(t, 3, requesters[1].grantedSlotsCount)
	require.Equal(t, 2, requesters[2].grantedSlotsCount)
	require.Equal(t, 1, pool.usedSlots[0])
	requesters[0].granter.TookSlot()
	requesters[0].grantedSlots++
	requesters[0].grantedSlotsCount++
	requesters[0].numWaitingWork = 1
	require.False(t, requesters[0].granter.TryGetSlot())

	require.Equal(t, -1, pool.maxSlots[0]-pool.usedSlots[0])
	require.Equal(t, 0, pool.maxSlots[1]-pool.usedSlots[1])
	require.Equal(t, 0, pool.maxSlots[2]-pool.usedSlots[2])
	requesters[3].grantedSlots--
	requesters[3].granter.ReturnWorkSlot()
	require.Equal(t, -1, pool.maxSlots[0]-pool.usedSlots[0])
	require.Equal(t, 0, pool.maxSlots[1]-pool.usedSlots[1])
	require.Equal(t, 0, pool.maxSlots[2]-pool.usedSlots[2])
	requesters[3].grantedSlots--
	requesters[3].granter.ReturnWorkSlot()
	require.Equal(t, -1, pool.maxSlots[0]-pool.usedSlots[0])
	require.Equal(t, 1, pool.maxSlots[1]-pool.usedSlots[1])
	require.Equal(t, 0, pool.maxSlots[2]-pool.usedSlots[2])

	pool.AdjustSlotsUsingLoad(UnderloadValue)
	require.Equal(t, 0, pool.maxSlots[0]-pool.usedSlots[0])
	require.Equal(t, 2, pool.maxSlots[0])
	require.Equal(t, 1, pool.maxSlots[1]-pool.usedSlots[1])
	require.Equal(t, 1, pool.maxSlots[1])
	require.Equal(t, 0, pool.maxSlots[2]-pool.usedSlots[2])
	require.Equal(t, 2, pool.maxSlots[2])

	pool.AdjustSlotsUsingLoad(UnderloadValue)
	require.Equal(t, 0, pool.maxSlots[0]-pool.usedSlots[0])
	require.Equal(t, 3, pool.maxSlots[0])
	require.Equal(t, 1, pool.maxSlots[1]-pool.usedSlots[1])
	require.Equal(t, 1, pool.maxSlots[1])
	require.Equal(t, 1, pool.maxSlots[2]-pool.usedSlots[2])
	require.Equal(t, 3, pool.maxSlots[2])

	require.Equal(t, 6, requesters[0].grantedSlotsCount)
	require.Equal(t, 3, requesters[1].grantedSlotsCount)
	require.Equal(t, 2, requesters[2].grantedSlotsCount)

	pool.AdjustSlotsUsingLoad(OverloadValue)
	require.Equal(t, -1, pool.maxSlots[0]-pool.usedSlots[0])
	require.Equal(t, 2, pool.maxSlots[0])
	require.Equal(t, 1, pool.maxSlots[1]-pool.usedSlots[1])
	require.Equal(t, 1, pool.maxSlots[1])
	require.Equal(t, 0, pool.maxSlots[2]-pool.usedSlots[2])
	require.Equal(t, 2, pool.maxSlots[2])

	pool.AdjustSlotsUsingLoad(OverloadValue)
	require.Equal(t, -1, pool.maxSlots[0]-pool.usedSlots[0])
	require.Equal(t, 2, pool.maxSlots[0])
	require.Equal(t, 1, pool.maxSlots[1]-pool.usedSlots[1])
	require.Equal(t, 1, pool.maxSlots[1])
	require.Equal(t, -1, pool.maxSlots[2]-pool.usedSlots[2])
	require.Equal(t, 1, pool.maxSlots[2])

}

// TODO(sumeer): test metrics
