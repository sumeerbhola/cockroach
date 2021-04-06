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
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

/*

High-level explanation of admission control scheme. Partially reflected in prototype.
For more background see https://docs.google.com/document/d/18S4uE8O1nRxULhSg9Z1Zt4jUPBiLJgMh7X1I6shsbug/edit?usp=sharing,
though that is missing a large number of lower-level practical details specific to CockroachDB.

- Enforcement points:
  - Granularity:
    - Want fine enough granularity. Ideally, also want fixed or bounded size work after enforcement point has admitted, but don't have that without scheduling control.
    - SQL statement considered too coarse.
  - List of primary enforcement points:
    - KV request:
      - Typically "fine enough" granularity in work size
      - Typically high cpu/io time ratio (due to caching)
      - Know when request completes: Explicitly knowing completion helps when requests can be variable in size. My experience is that pure rate control is harder to stabilize.
      - Prototype only enforces at InternalServer.Batch, and ignores other InternalServer functions. Need to include RangeFeed catchup scan.

      Caveats:
      - Can have large scans since limited scans are not universally used (but at least row count is known to be bounded for such unlimited scans -- it is the memory that isn't).
      - Contention can cause low cpu-time/wait-time ratio: So there isn't a fixed function of GOMAXPROCS to compute the desired number of admitted, but not completed, KV requests.
    - KV=>SQL response processing
      - For read statements can be at non-gateway node due to distributed evaluation. Otherwise at gateway node.
      - Don't know when response processing is done: the response potentially reaches deep into downstream sql processing into the Operator graph before it can be considered done in terms of processing (e.g. after all/most rows have been discarded by applying a select, or it has been aggregated into a groupby). And the behavior is data dependent.
      - Prototype limitation: doesn't yet hook up response processing for writes. Need to do it in tablewriter.go
    - SQL=>SQL rpc response processing
      - From upstream to downstream flow, where the downstream flow is at gateway node.
      - Don't know when response processing is done.
    - Prioritization across enforcement points: Prefer lower layer processing over higher layer since
      - higher layer processing generates more lower layer processing, so by prioritizing latter it applies natural backpressure on generation of more lower layer processing.
      - higher layer processing is less bounded in terms of size (perhaps).
      - lowest layer, KV, has non-SQL generated work that we must do (like node liveness).

   - List of secondary enforcement points: Start of SQL statement processing at leaf and root.
     - If node is significantly overloaded should throttle starting these.
     - The work units will be highly variable in size.
     - Know when it completes.
     - Prototype is missing any enforcement here, but the CPUPool has provision for it.

- Queueing at enforcement points and ordering in the queue and deadlocks:
  - Each kind of work (KV Request, KV=>SQL response processing, ...) has its own queue at a node.
  - Work queues until it is admitted.
  - Ordering based on tenant-id (if multi-tenant), priority (within a tenant), "original issue" time.
    - Original issue time is for liveness (avoid starvation): Prototype assigns this is various hacky ways. Ideally, could be based on time when gateway wanted to start processing a sql statement.
    - Prototype doesn't use tenant-id yet. Will try to equalize across tenants.
  - Some work is tied to replica and leaseholder of a replica. Would like to evict from queue and return error if replica or leaseholder moves. Hooks are there in AdmissionQueue but currently not integrated wrt listening to replica changes.
  - KV request processing can generate more KV processing: Subjecting latter to admission control could result in deadlock (since not using tokens -- see below). Latter should bypass.
    - Prototype hacks this with a FromSql field included in the BatchRequest proto and only doing admission control for these.
    - Bypass does decrement slots (see below)

- How these enforcement points will work with
  - single tenant: all enforcement points are used in each node. There is no use of tenant-id.
  - multi-tenant:
    - KV cluster only uses the KV request enforcement point.
    - SQL nodes will use the remaining enforcement points:
      - Prototype ties SQL response processing tokens to KV slot availability. Cannot do this when node not executing KV. In that case tie it to cpu signals.
        - Using the cpu signal directly instead of indirectly means 1ms lag which could mean under-utilization or over-utilization.
        - Could tune towards over-utilization with some burst throttling (see "grant chain" below).

- Enforcement approach:
  - KV request slots:
    - Consumed and returned by KV request processing.
    - Maximum number of slots adjusted based on cpu runnable goroutine signal gathered every 1ms. There is a slot cap too, but experimented with effectively unlimited value (concerned with having a cap if high contention).
      - Overload:
        - Signal: runnable goroutines > GOMAXPROCS. We are undershooting slot count a bit in terms of not reaching maximum cpu utilization when running 3-node kv95. Will try with a history over N ms where N is small (say 5).
        - Reaction: if using some slots and not exceeding max slots, decrease by 1.
      - Underload:
        - Signal: runnable goroutines <= GOMAXPROCS/2.
        - Reaction: if using all the slots increase by 1.
  - KV=>SQL response; SQL=>SQL response:
    - Pretend to be slot, but actually token in that it is never returned.
    - Did not try to predict token rate since load state is very dynamic and want to react very quickly.
      - If nothing in queue: grant token if KV not using all slots. Ignore bursts under assumption that there will be some spacing of response processing arrival.
      - If stuff in queue: add to queue
      - Queue processing: this is where burst can happen. Options
        - Dynamic burst count: Back to predicting what size each request would be.
        - Static burst count: This is what prototype does with refilling of burst tokens every 1ms.
          - For geo queries where sql response processing was significant consumer of cpu, a burst count of 50 was sufficient. Raised it to 200 during kv95 experiments though may not have been necessary (another bug)
          - Limit the burst by chaining the grants to rely on granted work's goroutine to get scheduled and grant to next in chain: In one experiment using grant chain increased p99 sql admission latency to 2.5s versus the direct grant approach of 400ms. The former is better since queueing in the admission queue gives us control compared to queueing in the scheduler.
  - Multi-tenant plan:
    - Slots: divide equally among waiting tenants
    - Tokens: Equalize when granting in a burst.
    - Rate hard to predict when work is
    - Equalize across tenants?

Current results:
- Manage to control cpu usage with hard-wired slot count of 1 despite high GOMAXPROCS of 7.
- Practical runs with no cap on slot count and using GOMAXPROCS=2.
  - Achieve 80+% cpu utilization with runnable goroutines per cpu of ~4. Need more tuning to achieve higher utilization.
  - Most queueing is in AdmissionQueue.
  - Node heartbeat latency is up to 3x lower than with no admission control (300ms vs 900ms).
*/

type WorkKind int8

// KVWork, SQLKVResponseWork, SQLSQLResponseWork are the lower-level work
// units, that are expected to be primarily cpu bound (with some disk I/O),
// and expected to be where most of the CPU consumption happens. These
// are distinguished since we want to prioritize in the order
//   KVWork > SQLKVResponseWork > SQLSQLResponseWork
//
// The high prioritization of KVWork reduces the likelihood that non-SQL KV
// work will be starved. SQLKVResponseWork is prioritized over
// SQLSQLResponseWork since the former includes leaf distsql processing and we
// would like to release memory caught up in RPC responses at lower layers of
// RPC tree. We expect that if SQLSQLResponseWork is delayed, that will end up
// reducing new work being issued which is a desirable form of natural
// backpressure.
//
// SQLStatementLeafStartWork, SQLStatementRootStartWork represent the start of
// root or leaf distsql processing and can be longer lived. Ideally, leaf work
// should be prioritized over root work, so that existing started work is
// finished first, and there is natural backpressure on new leaf work.
const (
	KVWork WorkKind = iota
	// Response processing in SQL for KV response from local or remote node.
	// This can be either leaf or root distsql work.
	SQLKVResponseWork
	// Response processing in SQL for SQL response. This is root work happening
	// in response to leaf SQL work.
	SQLSQLResponseWork
	SQLStatementLeafStartWork
	SQLStatementRootStartWork
)

// CPUPool provides a way to distribute work slots to various types of work
// being scheduled at a server. The slot term is analogous to scheduler slots
// in a cpu scheduler, in that slots that are handed out are eventually
// returned, but there are also significant differences:
// - The work here includes things that will do network or disk I/O without
//   returning a slot.
// - There is no preemption so we need to be careful about not creating
//   deadlock conditions (giving out all the slots to one kind of work that
//   needs slots for another kind of work to be able to relinquish their
//   slots).
// - The number of slots are heuristically initialized (taking into account
//   cpu resources and typical workload characteristics) and adjusted based on
//   load observations.
//
// There is a potential for deadlock if the same slots are shared across all
// these work types. Specifically, if SQLStatementRootStartWork uses up all
// slots, there are no slots available for the work generated by it to be
// done. A similar problem exists with SQLStatementLeafStartWork. So we
// separate slots into 3 categories: low-level work (first 3 kinds above),
// SQLStatementLeafStartWork, and SQLStatementRootStartWork. One deficiency of
// this is that we can't prioritize
//  SQLStatementLeafStartWork > SQLStatementRootStartWork
// since they have different slots. An alternative would be make them share
// slots but cap the number of slots in that shared pool that can be taken by
// SQLStatementRootStartWork.
type CPUPool struct {
	maxSlots           [3]int
	capSlots           [3]int
	requesters         [SQLStatementRootStartWork + 1]SlotRequester
	requesterToSlotMap [SQLStatementRootStartWork + 1]int
	usedSlots          [3]int

	// Currently we hardcode the ones where this is true to refer to index 0 of
	// slots. This is just a temporary hack for experimental validation of the
	// approach.
	requesterSlotIsActuallyToken [SQLStatementRootStartWork + 1]bool
	queueTokenBurst              int
	queueTokens                  int
	// Grant chains are used to throttle grant bursts when granting to queued
	// requests. The chain is continued by the grantee when its goroutine gets
	// scheduled. So if the scheduler is overwhelmed with runnable but not
	// running processes, it will delay the continuation of the chain. Grant
	// chains are not needed for real slots since there the slots are explicitly
	// returned when the work is done.
	grantChainActive bool
	grantChainIndex  int
	grantChainCount  int

	historyMu        syncutil.Mutex
	runnableCount    []int
	procsCount       []int
	nextLoadIndex    int
	oldestLoadIndex  int
	sumRunnableCount int
	sumProcsCount    int
	receivedLoad     bool

	Metrics CPUPoolMetrics
	mu      syncutil.Mutex
}

type SlotRequester interface {
	NumWaitingWork() int
	GrantWorkSlot() bool
}

type SlotGranter interface {
	SlotIsActuallyToken() bool
	TryGetSlot() bool
	ReturnWorkSlot()
	// TookSlot informs the SlotGranter that a slot was taken unilaterally,
	// without permission.
	TookSlot()
	// Should only be called if SlotIsActuallyToken() is true.
	ContinueGrantChain()
}

// NewCPUPool creates a new CPUPool.
func NewCPUPool(initialSlots [3]int, capSlots [3]int, queueTokenBurst int) *CPUPool {
	pool := &CPUPool{
		maxSlots:      initialSlots,
		capSlots:      capSlots,
		runnableCount: make([]int, 1),
		procsCount:    make([]int, 1),
	}
	pool.requesterToSlotMap[SQLStatementLeafStartWork] = 1
	pool.requesterToSlotMap[SQLStatementRootStartWork] = 2
	pool.requesterSlotIsActuallyToken[SQLKVResponseWork] = true
	pool.requesterSlotIsActuallyToken[SQLSQLResponseWork] = true
	pool.queueTokenBurst = queueTokenBurst
	pool.queueTokens = queueTokenBurst
	pool.Metrics = MakeCPUPoolMetrics()
	for i := range initialSlots {
		pool.Metrics.MaxSlots[i].Update(int64(initialSlots[i]))
	}
	return pool
}

type LoadSignal uint32

// TODO(sumeer): these should be more granular than an enum.
const (
	UnderloadValue  LoadSignal = 0
	NormalLoadValue LoadSignal = 1
	OverloadValue   LoadSignal = math.MaxUint32
)

// AdjustSlotsUsingLoad is called by the load monitor. Ideally, the higher
// the frequency, the better we can adjust. This is also the place where
// we refill the queueTokens.
// The current code below is very simple, so if the slot adjustment from the
// previous call has not had enough time to actually effect future admission,
// the slot counts will overshoot in either direction.
func (cp *CPUPool) AdjustSlotsUsingLoad(signal LoadSignal) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	increased := false
	if cp.queueTokens == 0 {
		log.Infof(context.Background(), "tokens not exhausted")
		increased = true
	}
	cp.queueTokens = cp.queueTokenBurst
	for i := range cp.maxSlots {
		if signal == OverloadValue {
			// Any kind of work that has more than 1 maxSlot, is using some slots,
			// and is not using more than its max slots, is attributed blame and
			// decreased. We don't actually know what caused the overload. If it is
			// currently using more than maxSlots it indicates that the previous
			// slot reduction feedback has not been enacted yet, so we hold off on
			// further decreasing.
			//
			// TODO:
			// - lower-bound of slots need not be a constant 1.
			// - multiplicative decrease.
			if cp.usedSlots[i] > 0 && cp.maxSlots[i] > 1 && (cp.usedSlots[i] <= cp.maxSlots[i]) {
				cp.maxSlots[i]--
				cp.Metrics.MaxSlots[i].Dec(1)
			}
		} else if signal == UnderloadValue {
			if cp.usedSlots[i] >= cp.maxSlots[i] && cp.maxSlots[i] != cp.capSlots[i] {
				// Used all its slots and can increase further, so additive increase.
				cp.maxSlots[i]++
				cp.Metrics.MaxSlots[i].Inc(1)
				increased = true
			}
		}
	}
	if increased {
		cp.tryGrant()
	}
}

func (cp *CPUPool) SetHistoryLength(length int) {
	cp.historyMu.Lock()
	defer cp.historyMu.Unlock()
	procsCount := make([]int, length)
	runnableCount := make([]int, length)
	if !cp.receivedLoad {
		cp.procsCount = procsCount
		cp.runnableCount = runnableCount
		return
	}
	i, j := cp.oldestLoadIndex, 0
	oldestLoadIndex := 0
	for {
		procsCount[j] = cp.procsCount[i]
		runnableCount[j] = cp.runnableCount[i]
		i = (i + 1) % len(cp.runnableCount)
		j = (j + 1) % len(runnableCount)
		if i == cp.nextLoadIndex {
			break
		}
		if j == oldestLoadIndex {
			// Wrapped around
			oldestLoadIndex = (oldestLoadIndex + 1) % len(runnableCount)
		}
	}
	cp.procsCount = procsCount
	cp.runnableCount = runnableCount
	cp.nextLoadIndex = j
	cp.oldestLoadIndex = oldestLoadIndex
}

func (cp *CPUPool) AdjustSlotsUsingRunnable(runnable int, procs int) {
	cp.historyMu.Lock()
	sufficientHistory := false
	if cp.nextLoadIndex == cp.oldestLoadIndex && cp.receivedLoad {
		cp.sumRunnableCount -= cp.runnableCount[cp.nextLoadIndex]
		cp.sumProcsCount -= cp.procsCount[cp.nextLoadIndex]
		cp.oldestLoadIndex = (cp.oldestLoadIndex + 1) % len(cp.runnableCount)
		sufficientHistory = true
	}
	if !cp.receivedLoad {
		cp.receivedLoad = true
	}
	cp.runnableCount[cp.nextLoadIndex] = runnable
	cp.procsCount[cp.nextLoadIndex] = procs
	cp.sumRunnableCount += runnable
	cp.sumProcsCount += procs
	cp.nextLoadIndex = (cp.nextLoadIndex + 1) % len(cp.runnableCount)
	if !sufficientHistory {
		cp.historyMu.Unlock()
		return
	}
	load := NormalLoadValue
	if cp.sumRunnableCount >= cp.sumProcsCount {
		load = OverloadValue
	} else if float64(runnable) <= float64(procs)/2.0 {
		load = UnderloadValue
	}
	cp.historyMu.Unlock()
	cp.AdjustSlotsUsingLoad(load)
}

func (cp *CPUPool) ChangeKVCapSlots(slots int) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.capSlots[KVWork] = slots
	if cp.capSlots[KVWork] < cp.maxSlots[KVWork] {
		cp.maxSlots[KVWork] = cp.capSlots[KVWork]
		cp.Metrics.MaxSlots[KVWork].Update(int64(slots))
	}
}

func (cp *CPUPool) adjustUsedSlots(workKind WorkKind, delta int) {
	i := cp.requesterToSlotMap[workKind]
	isToken := cp.requesterSlotIsActuallyToken[workKind]
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if isToken {
		// delta will always be negative.
		cp.queueTokens -= delta
		if cp.queueTokens > cp.queueTokenBurst {
			cp.queueTokens = cp.queueTokenBurst
		}
		cp.tryGrant()
	} else {
		cp.usedSlots[i] += delta
		if delta > 0 {
			cp.Metrics.UsedSlots[i].Inc(int64(delta))
			cp.Metrics.GrantedSlots[i].Inc(int64(delta))
		} else {
			cp.Metrics.UsedSlots[i].Dec(int64(-delta))
			cp.tryGrant()
		}
	}
}

// tryGetSlow is called when there is no work queued for workKind.
func (cp *CPUPool) tryGetSlot(workKind WorkKind) bool {
	i := cp.requesterToSlotMap[workKind]
	isToken := cp.requesterSlotIsActuallyToken[workKind]
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if isToken {
		// Since there was no work queued for workKind, we don't consume a slot
		// from queueTokens.
		if cp.usedSlots[i] < cp.maxSlots[i] {
			cp.Metrics.GrantedTokens.Inc(1)
			return true
		}
	} else {
		if cp.usedSlots[i] < cp.maxSlots[i] {
			cp.usedSlots[i]++
			cp.Metrics.UsedSlots[i].Inc(1)
			cp.Metrics.GrantedSlots[i].Inc(1)
			return true
		}
	}
	return false
}

// tryGrant tries to grant slots.
// REQUIRES: mu is held.
func (cp *CPUPool) tryGrant() {
	for i := range cp.requesters {
		if cp.requesters[i] == nil {
			// For serverless settings, the caller may not register all the kinds of
			// requesters.
			continue
		}
		j := cp.requesterToSlotMap[i]
		isToken := cp.requesterSlotIsActuallyToken[i]
		if isToken {
			if cp.grantChainActive {
				continue
			}
			if cp.requesters[i].NumWaitingWork() > 0 && cp.queueTokens > 0 && cp.maxSlots[j] > cp.usedSlots[j] {
				if cp.requesters[i].GrantWorkSlot() {
					cp.queueTokens--
					if cp.queueTokens <= 0 {
						log.Infof(context.Background(), "tokens exhausted")
					}
					cp.Metrics.GrantedTokens.Inc(1)
					cp.grantChainActive = true
					cp.grantChainIndex = i
					cp.grantChainCount = 1
				}
				// Else, grant was not accepted so will look at next requester.
			}
			/*
				grantCount := 0
				for cp.requesters[i].NumWaitingWork() > 0 && cp.queueTokens > 0 && cp.maxSlots[j] > cp.usedSlots[j] {
					if cp.requesters[i].GrantWorkSlot() {
						grantCount++
						cp.queueTokens--
						if cp.queueTokens <= 0 {
							log.Infof(context.Background(), "tokens exhausted")
						}
						cp.Metrics.GrantedTokens.Inc(1)
					}
				}
				if grantCount > 0 {
					log.Infof(context.Background(), "grant chain length %d", grantCount)
				}
			*/
		} else {
			for cp.requesters[i].NumWaitingWork() > 0 && cp.maxSlots[j] > cp.usedSlots[j] {
				if cp.requesters[i].GrantWorkSlot() {
					cp.usedSlots[j]++
					cp.Metrics.UsedSlots[j].Inc(1)
					cp.Metrics.GrantedSlots[j].Inc(1)
				} else {
					break
				}
			}
		}
	}
}

func (cp *CPUPool) continueGrantChain() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if !cp.grantChainActive {
		panic("grant chain is not active")
	}
	isToken := cp.requesterSlotIsActuallyToken[cp.grantChainIndex]
	if !isToken {
		panic("grant chain running over slots")
	}
	j := cp.requesterToSlotMap[cp.grantChainIndex]
	for i := cp.grantChainIndex; i <= int(SQLSQLResponseWork); i++ {
		cp.grantChainIndex = i
		if cp.requesters[i].NumWaitingWork() > 0 && cp.queueTokens > 0 && cp.maxSlots[j] > cp.usedSlots[j] {
			if cp.requesters[i].GrantWorkSlot() {
				cp.queueTokens--
				cp.grantChainCount++
				if cp.queueTokens <= 0 {
					log.Infof(context.Background(), "tokens exhausted")
				}
				cp.Metrics.GrantedTokens.Inc(1)
				return
			}
		}
	}
	cp.grantChainActive = false
	log.Infof(context.Background(), "grant chain length %d", cp.grantChainCount)
}

func (cp *CPUPool) Register(workKind WorkKind, requester SlotRequester) SlotGranter {
	cp.requesters[workKind] = requester
	return workGranter{
		workKind: workKind,
		pool:     cp,
	}
}

type workGranter struct {
	workKind WorkKind
	pool     *CPUPool
}

func (g workGranter) SlotIsActuallyToken() bool {
	return g.workKind == SQLKVResponseWork || g.workKind == SQLSQLResponseWork
}

// ReturnWorkSlot should only be called for the following cases:
// - For real slots, to return the slot at work completion.
// - For slots or tokens, to return slot or token if work is being abandoned after
//   queueing.
func (g workGranter) ReturnWorkSlot() {
	g.pool.adjustUsedSlots(g.workKind, -1)
}

// TookSlot should only be called for work that is using real slots.
func (g workGranter) TookSlot() {
	g.pool.adjustUsedSlots(g.workKind, +1)
}

func (g workGranter) TryGetSlot() bool {
	return g.pool.tryGetSlot(g.workKind)
}

func (g workGranter) ContinueGrantChain() {
	if g.SlotIsActuallyToken() {
		g.pool.continueGrantChain()
	}
}

var (
	maxSlots0 = metric.Metadata{
		Name:        "cpu_pool.max_slots0",
		Help:        "Max slots for 0th index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	maxSlots1 = metric.Metadata{
		Name:        "cpu_pool.max_slots1",
		Help:        "Max slots for 1st index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	maxSlots2 = metric.Metadata{
		Name:        "cpu_pool.max_slots2",
		Help:        "Max slots for 2nd index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	usedSlots0 = metric.Metadata{
		Name:        "cpu_pool.used_slots0",
		Help:        "Used slots for 0th index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	usedSlots1 = metric.Metadata{
		Name:        "cpu_pool.used_slots1",
		Help:        "Used slots for 1st index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	usedSlots2 = metric.Metadata{
		Name:        "cpu_pool.used_slots2",
		Help:        "Used slots for 2nd index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	grantedSlots0 = metric.Metadata{
		Name:        "cpu_pool.granted_slots0",
		Help:        "Granted slots for 0th index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	grantedSlots1 = metric.Metadata{
		Name:        "cpu_pool.granted_slots1",
		Help:        "Granted slots for 1st index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	grantedSlots2 = metric.Metadata{
		Name:        "cpu_pool.granted_slots2",
		Help:        "Granted slots for 2nd index",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	grantedTokens0 = metric.Metadata{
		Name:        "cpu_pool.granted_tokens0",
		Help:        "Granted tokens for 0th index",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
)

type CPUPoolMetrics struct {
	// TODO: these should use labels rather than be separate metrics.
	MaxSlots      [3]*metric.Gauge
	UsedSlots     [3]*metric.Gauge
	GrantedSlots  [3]*metric.Counter
	GrantedTokens *metric.Counter
}

func (CPUPoolMetrics) MetricStruct() {}

func MakeCPUPoolMetrics() CPUPoolMetrics {
	m := CPUPoolMetrics{}
	m.MaxSlots[0] = metric.NewGauge(maxSlots0)
	m.MaxSlots[1] = metric.NewGauge(maxSlots1)
	m.MaxSlots[2] = metric.NewGauge(maxSlots2)
	m.UsedSlots[0] = metric.NewGauge(usedSlots0)
	m.UsedSlots[1] = metric.NewGauge(usedSlots1)
	m.UsedSlots[2] = metric.NewGauge(usedSlots2)
	m.GrantedSlots[0] = metric.NewCounter(grantedSlots0)
	m.GrantedSlots[1] = metric.NewCounter(grantedSlots1)
	m.GrantedSlots[2] = metric.NewCounter(grantedSlots2)
	m.GrantedTokens = metric.NewCounter(grantedTokens0)
	return m
}
