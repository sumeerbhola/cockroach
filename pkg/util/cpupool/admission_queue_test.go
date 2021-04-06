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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func makeAI(
	priority roachpb.RequestAdmissionInfo_Priority, issueTime int64, bypass bool,
) AdmissionInfo {
	return AdmissionInfo{
		Priority:              priority,
		OriginalIssueWallTime: issueTime,
		BypassAdmission:       bypass,
	}
}

func TestAdmissionQueueBasic(t *testing.T) {
	pool := NewCPUPool([3]int{1, 1, 1}, [3]int{2, 2, 2}, 0)
	q := NewAdmissionQueue(AdmissionOptions{Name: "KVReq"}, pool)
	var mu syncutil.Mutex
	cond := sync.NewCond(&mu)
	var admitted []int64
	var errored []int64
	workFunc := func(info AdmissionInfo, timeout time.Duration) {
		ctx := context.Background()
		if timeout != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			defer cancel()
		}
		err := q.Admit(ctx, info)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			errored = append(errored, info.OriginalIssueWallTime)
		} else {
			admitted = append(admitted, info.OriginalIssueWallTime)
		}
		cond.Signal()
	}
	go workFunc(makeAI(roachpb.RequestAdmissionInfo_NORMAL, 1, false), 0)
	contains := func(elem int64, slice []int64) bool {
		for i := range slice {
			if slice[i] == elem {
				return true
			}
		}
		return false
	}
	mu.Lock()
	for !contains(1, admitted) {
		cond.Wait()
	}
	mu.Unlock()
	go workFunc(makeAI(roachpb.RequestAdmissionInfo_NORMAL, 3, false), 0)
	go workFunc(makeAI(roachpb.RequestAdmissionInfo_NORMAL, 2, false), 0)
	go workFunc(makeAI(roachpb.RequestAdmissionInfo_NORMAL, 4, true), 0)
	mu.Lock()
	for !contains(4, admitted) {
		cond.Wait()
	}
	mu.Unlock()
	time.Sleep(time.Millisecond * 5)
	mu.Lock()
	require.False(t, contains(2, admitted))
	require.False(t, contains(3, admitted))
	mu.Unlock()
	q.AdmittedWorkDone()
	time.Sleep(time.Millisecond * 5)
	mu.Lock()
	require.False(t, contains(2, admitted))
	require.False(t, contains(3, admitted))
	mu.Unlock()
	q.AdmittedWorkDone()
	time.Sleep(time.Millisecond * 5)
	mu.Lock()
	for !contains(2, admitted) {
		cond.Wait()
	}
	require.False(t, contains(3, admitted))
	mu.Unlock()
	go workFunc(makeAI(roachpb.RequestAdmissionInfo_HIGH, 5, false), 0)

	time.Sleep(time.Millisecond * 5)
	mu.Lock()
	require.False(t, contains(3, admitted))
	require.False(t, contains(5, admitted))
	mu.Unlock()
	q.AdmittedWorkDone()
	mu.Lock()
	for !contains(5, admitted) {
		cond.Wait()
	}
	require.False(t, contains(3, admitted))
	mu.Unlock()
	pool.AdjustSlotsUsingLoad(UnderloadValue)
	mu.Lock()
	for !contains(3, admitted) {
		cond.Wait()
	}
	mu.Unlock()
	go workFunc(makeAI(roachpb.RequestAdmissionInfo_NORMAL, 6, false), time.Millisecond*2)
	time.Sleep(time.Millisecond * 4)
	mu.Lock()
	for !contains(6, errored) {
		cond.Wait()
	}
	mu.Unlock()
	go workFunc(makeAI(roachpb.RequestAdmissionInfo_NORMAL, 7, false), time.Millisecond*4)
	time.Sleep(time.Millisecond * 2)
	q.AdmittedWorkDone()
	mu.Lock()
	for !contains(7, admitted) {
		cond.Wait()
	}
	mu.Unlock()

}
