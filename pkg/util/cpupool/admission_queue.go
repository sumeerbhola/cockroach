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
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// AdmissionQueue is used for
// - KVWork: The AdmissionInfo is derived from the RequestAdmissionInfo proto. This
//   is the only case that may set AdmissionInfo.BypassAdmission to true, for high
//   priority intra-KV work. AdmissionOptions.TiedToRange will be true.
// - SQLKVResponseWork, SQLSQLResponseWork:
// - SQLStatementLeafStartWork.
//
// We also care about replica placement for distsql leaf processing. But
// AFAIK, distsql cannot alter its placement after planning, so cannot really
// do anything useful by setting TiedToRange to true.
type AdmissionQueue struct {
	opts         AdmissionOptions
	rangeInfoMap map[int64]*rangeInfo
	slotGranter  SlotGranter
	queue        []*admissionItem
	// Protects data-structures.
	mu syncutil.Mutex
	// Prevents more than one caller to be in Admit, adding to queue, so can
	// release mu before TryGetSlot and be assured not competing with another
	// Admit.
	admitMu syncutil.Mutex
	Metrics AdmissionQueueMetrics
}

var _ SlotRequester = &AdmissionQueue{}

type AdmissionOptions struct {
	Name        string
	TiedToRange bool
	WorkKind    WorkKind
}

func NewAdmissionQueue(options AdmissionOptions, pool *CPUPool) *AdmissionQueue {
	q := &AdmissionQueue{
		opts:    options,
		Metrics: MakeAdmissionQueueMetrics(options.Name),
	}
	q.slotGranter = pool.Register(options.WorkKind, q)
	if options.TiedToRange {
		// Uncomment once we hook this up with state of ranges.
		// q.rangeInfoMap = make(map[int64]*rangeInfo)
	}
	return q
}

type AdmissionInfo struct {
	TenantID              roachpb.TenantID
	Priority              roachpb.RequestAdmissionInfo_Priority
	OriginalIssueWallTime int64
	RequiresLeaseholder   bool
	RangeID               int64
	// Should never be true for work that is not using real slots.
	// If we were actually tracking tokens, though it is tricky
	// to figure out an appropriate load-based dynamic fill rate,
	// we may be able to handle BypassAdmission=true for even the
	// work that uses tokens masquerading as slots.
	BypassAdmission bool
}

var neverClosedCh = make(chan struct{})

func (q *AdmissionQueue) FakedAdmissionInfo() {
	// TODO: adjust metric
}

func (q *AdmissionQueue) Admit(ctx context.Context, info AdmissionInfo) error {
	q.Metrics.Requested.Inc(1)
	if info.BypassAdmission {
		q.slotGranter.TookSlot()
		q.Metrics.Admitted.Inc(1)
		return nil
	}

	q.admitMu.Lock()
	q.mu.Lock()
	if len(q.queue) == 0 {
		// Fast-path. Try to grab slot.
		q.mu.Unlock()
		if q.slotGranter.TryGetSlot() {
			q.admitMu.Unlock()
			q.Metrics.Admitted.Inc(1)
			return nil
		}
		// Did not get slot.
		q.mu.Lock()
	}
	// Check deadline and start timer.
	startTime := time.Now()
	deadline, ok := ctx.Deadline()
	var doneCh <-chan struct{}
	if ok {
		doneCh = ctx.Done()
		if doneCh == nil {
			panic("have deadline but no done channel")
		}
		select {
		case _, ok := <-doneCh:
			if !ok {
				// Already cancelled. More likely to happen if cpu starvation is
				// causing entering the admission queue to be delayed.
				q.mu.Unlock()
				q.admitMu.Unlock()
				q.Metrics.Errored.Inc(1)
				return errors.Newf("work deadline already expired: deadline: %s, now: %s",
					redact.Safe(deadline), redact.Safe(startTime))
			}
		default:
		}
	}
	// Check range state.
	var rangeInfo *rangeInfo
	if q.rangeInfoMap != nil {
		var ok bool
		rangeInfo, ok = q.rangeInfoMap[info.RangeID]
		if !ok || (info.RequiresLeaseholder && rangeInfo.rangeState != LeaseHolderReplica) {
			q.mu.Unlock()
			q.admitMu.Unlock()
			q.Metrics.Errored.Inc(1)
			return errors.Newf("rangeID %d is not in relevant state", redact.Safe(info.RangeID))
		}
	}
	// Push onto heap.
	ch := make(chan struct{}, 1)
	item := &admissionItem{
		priority:              info.Priority,
		originalIssueWallTime: info.OriginalIssueWallTime,
		ch:                    ch,
	}
	heap.Push(q, item)
	q.Metrics.WaitQueueLength.Inc(1)
	if rangeInfo != nil {
		if info.RequiresLeaseholder {
			rangeInfo.itemsForLeaseholder = append(rangeInfo.itemsForLeaseholder, item)
			item.rangeSliceIndex = len(rangeInfo.itemsForLeaseholder) - 1
		} else {
			rangeInfo.items = append(rangeInfo.items, item)
			item.rangeSliceIndex = len(rangeInfo.items) - 1
		}
	}

	if doneCh == nil {
		doneCh = neverClosedCh
	}
	q.mu.Unlock()
	q.admitMu.Unlock()
	select {
	case <-doneCh:
		q.mu.Lock()
		if item.index == -1 {
			// No longer in heap. Raced with slot grant.
			q.slotGranter.ReturnWorkSlot()
			q.slotGranter.ContinueGrantChain()
		} else {
			heap.Remove(q, item.index)
			if rangeInfo != nil {
				rangeInfo.removeItem(info.RequiresLeaseholder, item)
			}
		}
		q.mu.Unlock()
		q.Metrics.Errored.Inc(1)
		waitDur := time.Now().Sub(startTime)
		q.Metrics.WaitDurationSum.Inc(waitDur.Microseconds())
		q.Metrics.WaitDurations.RecordValue(waitDur.Nanoseconds())
		q.Metrics.WaitQueueLength.Dec(1)
		log.Infof(ctx, "work %s deadline expired after %s",
			redact.Safe(q.opts.Name), redact.Safe(waitDur.String()))
		return errors.Newf("work deadline expired while waiting: deadline: %s, start: %s, dur: %s",
			redact.Safe(deadline.String()), redact.Safe(startTime.String()), redact.Safe(waitDur.String()))
	case _, ok := <-ch:
		waitDur := time.Now().Sub(startTime)
		q.Metrics.WaitDurationSum.Inc(waitDur.Microseconds())
		q.Metrics.WaitDurations.RecordValue(waitDur.Nanoseconds())
		q.Metrics.WaitQueueLength.Dec(1)
		if ok {
			q.Metrics.Admitted.Inc(1)
			if item.index != -1 {
				panic("bug: slot grant should have removed from heap")
			}
			q.slotGranter.ContinueGrantChain()
			if rangeInfo != nil {
				rangeInfo.removeItem(info.RequiresLeaseholder, item)
			}
			log.Infof(ctx, "work %s admitted after %s",
				redact.Safe(q.opts.Name), redact.Safe(waitDur.String()))
			return nil
		} else {
			// Closed because range is no longer in state to satisfy work.
			q.Metrics.Errored.Inc(1)
			q.mu.Lock()
			if item.index == -1 {
				// No longer in heap. Raced with slot grant.
				q.slotGranter.ReturnWorkSlot()
				q.slotGranter.ContinueGrantChain()
			} else {
				heap.Remove(q, item.index)
			}
			q.mu.Unlock()
			log.Infof(ctx, "work %s errored after %s",
				redact.Safe(q.opts.Name), redact.Safe(waitDur.String()))
			return errors.Newf("rangeID %d is no longer in relevant state: start: %s, dur: %s",
				redact.Safe(info.RangeID), redact.Safe(startTime.String()), redact.Safe(waitDur.String()))
		}
	}
}

func (q *AdmissionQueue) AdmittedWorkDone() {
	q.slotGranter.ReturnWorkSlot()
}

type RangeState int8

const (
	NoReplica RangeState = iota
	LeaseHolderReplica
	NonLeaseHolderReplica
)

type rangeInfo struct {
	rangeState          RangeState
	items               []*admissionItem
	itemsForLeaseholder []*admissionItem
}

func (r *rangeInfo) removeItem(requiresLeaseholder bool, item *admissionItem) {
	if item.rangeSliceIndex == -1 {
		return
	}
	items := &r.items
	if requiresLeaseholder {
		items = &r.itemsForLeaseholder
	}
	n := len(*items) - 1
	if n < item.rangeSliceIndex {
		panic("bug: invalid rangeSliceIndex")
	}
	// TODO: add assertions to check that item is actually the same
	(*items)[n], (*items)[item.rangeSliceIndex] = (*items)[item.rangeSliceIndex], (*items)[n]
	(*items)[item.rangeSliceIndex].rangeSliceIndex = item.rangeSliceIndex
	*items = (*items)[:n]
}

func closeWaitersChannel(items []*admissionItem) {
	for i := range items {
		close(items[i].ch)
		items[i].rangeSliceIndex = -1
	}
}

func (q *AdmissionQueue) UpdateRangeInfo(rangeID int64, rangeState RangeState) {
	q.mu.Lock()
	defer q.mu.Unlock()
	rInfo, ok := q.rangeInfoMap[rangeID]
	if !ok && rangeState != NoReplica {
		q.rangeInfoMap[rangeID] = &rangeInfo{rangeState: rangeState}
	} else if ok && rangeState != rInfo.rangeState {
		rInfo.rangeState = rangeState
		if rangeState == NoReplica {
			closeWaitersChannel(rInfo.items)
			closeWaitersChannel(rInfo.itemsForLeaseholder)
			delete(q.rangeInfoMap, rangeID)
		} else if rangeState == NonLeaseHolderReplica {
			closeWaitersChannel(rInfo.itemsForLeaseholder)
			rInfo.itemsForLeaseholder = rInfo.itemsForLeaseholder[:0]
		}
	}
}

func (q *AdmissionQueue) NumWaitingWork() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}

func (q *AdmissionQueue) GrantWorkSlot() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) == 0 {
		return false
	}
	item := heap.Pop(q).(*admissionItem)
	item.ch <- struct{}{}
	return true
}

// TODO: use tenant as part of admission ordering.

type admissionItem struct {
	priority              roachpb.RequestAdmissionInfo_Priority
	originalIssueWallTime int64
	ch                    chan struct{}
	// The index is needed by update and is maintained by the heap.Interface methods.
	index           int // The index of the item in the heap.
	rangeSliceIndex int
}

var _ heap.Interface = &AdmissionQueue{}

func (q *AdmissionQueue) Len() int { return len(q.queue) }

func (q *AdmissionQueue) Less(i, j int) bool {
	if q.queue[i].priority == q.queue[j].priority {
		return q.queue[i].originalIssueWallTime < q.queue[j].originalIssueWallTime
	}
	return q.queue[i].priority > q.queue[j].priority
}

func (q *AdmissionQueue) Swap(i, j int) {
	q.queue[i], q.queue[j] = q.queue[j], q.queue[i]
	q.queue[i].index = i
	q.queue[j].index = j
}

func (q *AdmissionQueue) Push(x interface{}) {
	n := len(q.queue)
	item := x.(*admissionItem)
	item.index = n
	q.queue = append(q.queue, item)
}

func (q *AdmissionQueue) Pop() interface{} {
	old := q.queue
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	q.queue = old[0 : n-1]
	return item
}

var (
	requestedMeta = metric.Metadata{
		Name:        "admission.requested.",
		Help:        "Number of requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	admittedMeta = metric.Metadata{
		Name:        "admission.admitted.",
		Help:        "Number of requests admitted",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	erroredMeta = metric.Metadata{
		Name:        "admission.errored.",
		Help:        "Number of requests not admitted due to error",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	waitDurationSumMeta = metric.Metadata{
		Name:        "admission.wait_sum.",
		Help:        "Total wait time in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	waitDurationsMeta = metric.Metadata{
		Name:        "admission.wait_durations.",
		Help:        "Wait time durations for requests that waited",
		Measurement: "Wait time Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}
	waitQueueLengthMeta = metric.Metadata{
		Name:        "admission.wait_queue_length.",
		Help:        "Length of wait queue",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
)

func addName(name string, meta metric.Metadata) metric.Metadata {
	rv := meta
	rv.Name = rv.Name + name
	return rv
}

// TODO: The different AdmissionQueues should use a label corresponding to
// Name, and share the same metrics.
type AdmissionQueueMetrics struct {
	Requested       *metric.Counter
	Admitted        *metric.Counter
	Errored         *metric.Counter
	WaitDurationSum *metric.Counter
	WaitDurations   *metric.Histogram
	WaitQueueLength *metric.Gauge
}

func (AdmissionQueueMetrics) MetricStruct() {}

func MakeAdmissionQueueMetrics(name string) AdmissionQueueMetrics {
	return AdmissionQueueMetrics{
		Requested:       metric.NewCounter(addName(name, requestedMeta)),
		Admitted:        metric.NewCounter(addName(name, admittedMeta)),
		Errored:         metric.NewCounter(addName(name, erroredMeta)),
		WaitDurationSum: metric.NewCounter(addName(name, waitDurationSumMeta)),
		WaitDurations: metric.NewLatency(
			addName(name, waitDurationsMeta), base.DefaultHistogramWindowInterval()),
		WaitQueueLength: metric.NewGauge(addName(name, waitQueueLengthMeta)),
	}
}
