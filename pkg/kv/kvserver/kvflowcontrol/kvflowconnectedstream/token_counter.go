// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowconnectedstream

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// regularTokensPerStream determines the flow tokens available for regular work
// on a per-stream basis.
//
// TODO: Either unify these settings with kvflowcontroller settings, or rationalize the
// naming.
var regularTokensPerStream = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_controller_v2.regular_tokens_per_stream",
	"flow tokens available for regular work on a per-stream basis",
	16<<20, // 16 MiB
	validateTokenRange,
)

// elasticTokensPerStream determines the flow tokens available for elastic work
// on a per-stream basis.
var elasticTokensPerStream = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_controller_v2.elastic_tokens_per_stream",
	"flow tokens available for elastic work on a per-stream basis",
	8<<20, // 8 MiB
	validateTokenRange,
)

// tokenCounterPerWorkClass is a helper struct for implementing tokenCounter.
// tokens and stats are protected by the mutex in tokenCounter. Operations on
// the signalCh may not be protected by that mutex -- see the comment below.
type tokenCounterPerWorkClass struct {
	wc     admissionpb.WorkClass
	tokens kvflowcontrol.Tokens
	// Waiting requests do so by waiting on signalCh without holding a mutex.
	//
	// Requests first check for available tokens (by acquiring and releasing the
	// mutex), and then wait if tokens for their work class are unavailable. The
	// risk in such waiting after releasing the mutex is the following race:
	// tokens become available after the waiter releases the mutex and before it
	// starts waiting. We handle this race by ensuring that signalCh always has
	// an entry if tokens are available:
	//
	// - Whenever tokens are returned, signalCh is signaled, waking up a single
	//   waiting request. If the request finds no available tokens, it starts
	//   waiting again.
	// - Whenever a request gets admitted, it signals the next waiter if any.
	//
	// So at least one request that observed unavailable tokens will get
	// unblocked, which will in turn unblock others. This turn by turn admission
	// provides some throttling to over-admission since the goroutine scheduler
	// needs to schedule the goroutine that got the entry for it to unblock
	// another. Hopefully, before we over-admit much, some of the scheduled
	// goroutines will complete proposal evaluation and deduct the tokens they
	// need.
	//
	// TODO(irfansharif): Right now we continue forwarding admission grants to
	// request while the available tokens > 0, which can lead to over-admission
	// since deduction is deferred (see testdata/simulation/over_admission).
	//
	// A. One mitigation could be terminating grant forwarding if the
	//    'tentatively deducted tokens' exceeds some amount (say, 16
	//    MiB). When tokens are actually deducted, we'll reduce from
	//    this 'tentatively deducted' count. We can re-signal() on every
	//    actual token deduction where available tokens is still > 0.
	// B. The pathological case with A is when all these tentatively
	//    deducted tokens are due to requests that are waiting on
	//    latches and locks. And the next request that wants to be
	//    admitted is not contending with those latches/locks but gets
	//    stuck behind it anyway. We could instead count the # of
	//    requests that go through this tokenCounter and are also past the
	//    latch/lock acquisition but not yet evaluated, and block if
	//    that count is greater than some small multiple of GOMAXPROCS.

	signalCh chan struct{}
	stats    struct {
		deltaStats
		noTokenStartTime time.Time
	}
}

type deltaStats struct {
	noTokenDuration time.Duration
	tokensDeducted  kvflowcontrol.Tokens
}

func makeTokenCounterPerWorkClass(
	wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens, now time.Time,
) tokenCounterPerWorkClass {
	bwc := tokenCounterPerWorkClass{
		wc:       wc,
		tokens:   tokens,
		signalCh: make(chan struct{}, 1),
	}
	bwc.stats.noTokenStartTime = now
	return bwc
}

func (bwc *tokenCounterPerWorkClass) adjustTokensLocked(
	ctx context.Context,
	delta kvflowcontrol.Tokens,
	limit kvflowcontrol.Tokens,
	admin bool,
	now time.Time,
) (adjustment, unaccounted kvflowcontrol.Tokens) {
	before := bwc.tokens
	bwc.tokens += delta
	if delta > 0 {
		if bwc.tokens > limit {
			unaccounted = bwc.tokens - limit
			bwc.tokens = limit
		}
		if before <= 0 && bwc.tokens > 0 {
			bwc.signal()
			bwc.stats.noTokenDuration += now.Sub(bwc.stats.noTokenStartTime)
		}
	} else {
		bwc.stats.deltaStats.tokensDeducted -= delta
		if before > 0 && bwc.tokens <= 0 {
			bwc.stats.noTokenStartTime = now
		}
	}
	if buildutil.CrdbTestBuild && !admin && unaccounted != 0 {
		log.Fatalf(ctx, "unaccounted[%s]=%d delta=%d limit=%d", bwc.wc, unaccounted, delta, limit)
	}
	adjustment = bwc.tokens - before
	return adjustment, unaccounted
}

func (bwc *tokenCounterPerWorkClass) signal() {
	select {
	case bwc.signalCh <- struct{}{}: // non-blocking channel write that ensures it's topped up to 1 entry
	default:
	}
}

type WaitEndState int32

const (
	WaitSuccess WaitEndState = iota
	ContextCanceled
	RefreshWaitSignaled
)

func (bwc *tokenCounterPerWorkClass) getAndResetStats(now time.Time) deltaStats {
	stats := bwc.stats.deltaStats
	if bwc.tokens <= 0 {
		stats.noTokenDuration += now.Sub(bwc.stats.noTokenStartTime)
	}
	bwc.stats.deltaStats = deltaStats{}
	// Doesn't matter if bwc.tokens is actually > 0 since in that case we won't
	// use this value.
	bwc.stats.noTokenStartTime = now
	return stats
}

// tokenCounter holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. It's used to synchronize handoff between threads
// returning and waiting for flow tokens.
type tokenCounter struct {
	clock    *hlc.Clock
	settings *cluster.Settings

	mu struct {
		syncutil.RWMutex

		// Token limit per work class, tracking
		// kvadmission.flow_controller.{regular,elastic}_tokens_per_stream.
		limit    tokensPerWorkClass
		counters [admissionpb.NumWorkClasses]tokenCounterPerWorkClass
	}
}

var _ TokenCounter = &tokenCounter{}

func newTokenCounter(settings *cluster.Settings, clock *hlc.Clock) *tokenCounter {
	b := &tokenCounter{
		clock:    clock,
		settings: settings,
	}

	regularTokens := kvflowcontrol.Tokens(regularTokensPerStream.Get(&settings.SV))
	elasticTokens := kvflowcontrol.Tokens(elasticTokensPerStream.Get(&settings.SV))
	b.mu.limit = tokensPerWorkClass{
		regular: regularTokens,
		elastic: elasticTokens,
	}
	b.mu.counters[admissionpb.RegularWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.RegularWorkClass, b.mu.limit.regular, b.clock.PhysicalTime())
	b.mu.counters[admissionpb.ElasticWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.ElasticWorkClass, b.mu.limit.elastic, b.clock.PhysicalTime())

	onChangeFunc := func(ctx context.Context) {
		b.mu.Lock()
		defer b.mu.Unlock()

		b.mu.Lock()
		defer b.mu.Unlock()

		before := b.mu.limit
		now := tokensPerWorkClass{
			regular: kvflowcontrol.Tokens(regularTokensPerStream.Get(&settings.SV)),
			elastic: kvflowcontrol.Tokens(elasticTokensPerStream.Get(&settings.SV)),
		}
		adjustment := tokensPerWorkClass{
			regular: now.regular - before.regular,
			elastic: now.elastic - before.elastic,
		}
		b.mu.limit = now

		b.mu.counters[admissionpb.RegularWorkClass].adjustTokensLocked(
			ctx, adjustment.regular, now.regular, true /* admin */, b.clock.PhysicalTime())
		b.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(
			ctx, adjustment.elastic, now.elastic, true /* admin */, b.clock.PhysicalTime())
	}

	regularTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	elasticTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	return b
}

func (b *tokenCounter) tokens(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tokensLocked(wc)
}

func (b *tokenCounter) tokensLocked(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	return b.mu.counters[wc].tokens
}

// TokensAvailable returns true if tokens are available. If false, it returns a
// handle to use for waiting using kvflowcontroller.WaitForHandlesAndChannels.
// This is for waiting pre-evaluation.
func (b *tokenCounter) TokensAvailable(
	wc admissionpb.WorkClass,
) (available bool, handle TokenWaitingHandle) {
	if b.tokens(wc) > 0 {
		return true, nil
	}
	return false, waitHandle{wc: wc, b: b}
}

func (b *tokenCounter) TryDeduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) kvflowcontrol.Tokens {
	tokensAvailable := b.tokens(wc)

	if tokensAvailable <= 0 {
		return 0
	}

	// TODO(kvoli): Calculating the number of tokens to deduct and actually
	// deducting them is not atomic here.
	adjust := -min(tokensAvailable, tokens)
	b.adjust(ctx, wc, adjust, false /* admin */, b.clock.PhysicalTime())
	// TODO: Should we instead be using the adjusted return value here? It is
	// split across both elastic and regular classes, so perhaps its the minimum
	// of the two for regular and otherwise the elastic value
	return -adjust
}

// Deduct deducts (without blocking) flow tokens for the given priority.
func (b *tokenCounter) Deduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	b.adjust(ctx, wc, -tokens, false /* admin */, b.clock.PhysicalTime())
}

// Return returns flow tokens for the given priority.
func (b *tokenCounter) Return(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	b.adjust(ctx, wc, tokens, false /* admin */, b.clock.PhysicalTime())
}

type waitHandle struct {
	wc admissionpb.WorkClass
	b  *tokenCounter
}

var _ TokenWaitingHandle = waitHandle{}

// WaitChannel is the channel that will be signaled if tokens are possibly
// available. If signaled, the caller must call TryDeductAndUnblockNextWaiter.
func (wh waitHandle) WaitChannel() <-chan struct{} {
	return wh.b.mu.counters[wh.wc].signalCh
}

// TryDeductAndUnblockNextWaiter is called to deduct some tokens. The tokens
// parameter can be zero, when the waiter is only waiting for positive tokens
// (such as when waiting before eval). granted <= tokens and the tokens that
// have been deducted. haveTokens is true iff there are tokens available after
// this grant. When the tokens parameter is zero, granted will be zero, and
// haveTokens represents whether there were positive tokens. If the caller is
// unsatisfied with the return values, it can resume waiting using WaitChannel.
func (wh waitHandle) TryDeductAndUnblockNextWaiter(
	tokens kvflowcontrol.Tokens,
) (granted kvflowcontrol.Tokens, haveTokens bool) {
	defer func() {
		// Signal the next waiter if we have tokens available upon returning.
		if haveTokens {
			wh.b.mu.counters[wh.wc].signal()
		}
	}()

	if tokens > 0 {
		granted = wh.b.TryDeduct(context.Background(), wh.wc, tokens)
	}

	return granted, wh.b.tokens(wh.wc) > 0
}

// WaitForEval waits for a quorum of handles to be signaled and have tokens
// available, including all the required wait handles. The caller can provide a
// refresh channel, which when signaled will cause the function to return
// RefreshWaitSignaled, allowing the caller to retry waiting with updated
// handles.
//
// If the required quorum and required wait handles are signaled and have
// tokens available, the function checks the availability of tokens for all
// handles. If not all required handles have available tokens or the available
// count is less than the required quorum, RefreshWaitSignaled is also
// returned.
func WaitForEval(
	ctx context.Context,
	refreshWaitCh <-chan struct{},
	handles []tokenWaitingHandleInfo,
	requiredQuorum int,
	scratch []reflect.SelectCase,
) (state WaitEndState, scratch2 []reflect.SelectCase) {
	scratch = scratch[:0]
	if len(handles) < requiredQuorum || requiredQuorum == 0 {
		panic(errors.AssertionFailedf("invalid arguments to WaitForEval"))
	}

	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(refreshWaitCh)})

	requiredWaitCount := 0
	for _, h := range handles {
		if h.requiredWait {
			requiredWaitCount++
		}
		scratch = append(scratch,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(h.handle.WaitChannel())})
	}
	// We track the original length of the scratch slice and also the current
	// length, where after each successful iteration we decrement the current
	// length.
	m := len(scratch)
	signaledCount := 0

	// Wait for at least a quorumCount of handles to be signaled which also have
	// tokens and additionally, all of the required wait handles to have signaled
	// and have tokens available.
	for signaledCount < requiredQuorum || requiredWaitCount > 0 {
		chosen, _, _ := reflect.Select(scratch)
		switch chosen {
		case 0:
			return ContextCanceled, scratch
		case 1:
			return RefreshWaitSignaled, scratch
		default:
			handleInfo := handles[chosen-2]
			signaledCount++
			if handleInfo.requiredWait {
				requiredWaitCount--
			}
			m--
			scratch[chosen], scratch[m] = scratch[m], scratch[chosen]
			scratch = scratch[:m]
			handles[chosen-2], handles[m-2] = handles[m-2], handles[chosen-2]
		}
	}

	availableCount := 0
	allRequiredAvailable := true
	// Check whether the handle has available tokens, if not we keep the
	// select case and keep on waiting.
	// TODO(kvoli): Currently we are only trying to deduct and signal the next
	// waiter after signaling a quorum and all required waiters. If we instead
	// signaled the next waiter immediately following a signal, would that work?
	// At the moment we will hold up the next waiter for all required waiters
	// here. If we instead signaled the next waiter immediately, would it be
	// possible that we over-admit, as no tokens are actually deducted until after
	// this function returns WaitSuccess.
	for _, handleInfo := range handles {
		if _, available := handleInfo.handle.TryDeductAndUnblockNextWaiter(0 /* tokens */); available {
			availableCount++
		} else if !available && handleInfo.requiredWait {
			allRequiredAvailable = false
		}
	}

	if !allRequiredAvailable || availableCount < requiredQuorum {
		return RefreshWaitSignaled, scratch
	}

	return WaitSuccess, scratch
}

type tokenWaitingHandleInfo struct {
	handle TokenWaitingHandle
	// For regular work, this will be set for the leaseholder and leader. For
	// elastic work this will be set for the aforementioned, and all replicas
	// which are in StateReplicate.
	requiredWait bool
}

// admin is set to true when this method is called because of a settings
// change. In that case the class is interpreted narrowly as only updating the
// tokens for that class.
func (b *tokenCounter) adjust(
	ctx context.Context,
	class admissionpb.WorkClass,
	delta kvflowcontrol.Tokens,
	admin bool,
	now time.Time,
) (adjustment, unaccounted tokensPerWorkClass) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// TODO(irfansharif,aaditya): On kv0/enc=false/nodes=3/cpu=96 this mutex is
	// responsible for ~1.8% of the mutex contention. Maybe address it as part
	// of #104154. We want to effectively increment two values but cap each at
	// some limit, and when incrementing, figure out what the adjustment was.
	// What if reads always capped it at the limit? And when incrementing
	// atomically by +delta, if we're over the limit, since we tried to increase
	// the value by +delta, at most we need to adjust back down by -delta.
	// Something like it.
	//
	//	var c int64 = 0
	//	var limit int64 = rand.Int63n(10000000000)
	//	for i := 0; i < 50; i++ {
	//		go func() {
	//			for j := 0; j < 2000; j++ {
	//				delta := rand.Int63()
	//				v := atomic.AddInt64(&c, delta)
	//				if v > limit {
	//					overlimit := v - limit
	//					var adjustment int64 = overlimit
	//					if delta < overlimit {
	//						adjustment = delta
	//					}
	//					n := atomic.AddInt64(&c, -adjustment)
	//					fmt.Printf("%d > %d by %d, adjusted by %d to %d)\n",
	//						v, limit, v-limit, -adjustment, n)
	//				}
	//			}
	//		}()
	//	}

	switch class {
	case admissionpb.RegularWorkClass:
		adjustment.regular, unaccounted.regular =
			b.mu.counters[admissionpb.RegularWorkClass].adjustTokensLocked(
				ctx, delta, b.mu.limit.regular, admin, now)
		if !admin {
			// Regular {deductions,returns} also affect elastic flow tokens.
			adjustment.elastic, unaccounted.elastic =
				b.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(
					ctx, delta, b.mu.limit.elastic, admin, now)
		}
	case admissionpb.ElasticWorkClass:
		// Elastic {deductions,returns} only affect elastic flow tokens.
		adjustment.elastic, unaccounted.elastic =
			b.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(
				ctx, delta, b.mu.limit.elastic, admin, now)
	}
	return adjustment, unaccounted
}

func (b *tokenCounter) getAndResetStats(now time.Time) (regularStats, elasticStats deltaStats) {
	b.mu.Lock()
	defer b.mu.Unlock()
	regularStats = b.mu.counters[admissionpb.RegularWorkClass].getAndResetStats(now)
	elasticStats = b.mu.counters[admissionpb.ElasticWorkClass].getAndResetStats(now)
	return regularStats, elasticStats
}

func (b *tokenCounter) testingGetChannel(wc admissionpb.WorkClass) <-chan struct{} {
	return b.mu.counters[wc].signalCh
}

func (b *tokenCounter) testingSignalChannel(wc admissionpb.WorkClass) {
	b.mu.counters[wc].signal()
}

type tokensPerWorkClass struct {
	regular, elastic kvflowcontrol.Tokens
}

const (
	minTokensPerStream kvflowcontrol.Tokens = 1 << 20  // 1 MiB
	maxTokensPerStream kvflowcontrol.Tokens = 64 << 20 // 64 MiB
)

var validateTokenRange = settings.WithValidateInt(func(b int64) error {
	t := kvflowcontrol.Tokens(b)
	if t < minTokensPerStream {
		return fmt.Errorf("minimum flowed tokens allowed is %s, got %s", minTokensPerStream, t)
	}
	if t > maxTokensPerStream {
		return fmt.Errorf("maximum flow tokens allowed is %s, got %s", maxTokensPerStream, t)
	}
	return nil
})
