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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontroller"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StoreStreamsTokenCounter is one per node.
//
// TODO: use code in kvflowcontroller.Controller to implement this.
type StoreStreamsTokenCounter interface {
	EvalTokenCounterForStream(kvflowcontrol.Stream) TokenCounter
	SendTokenCounterForStream(kvflowcontrol.Stream) TokenCounter
}

// TokenCounter will be implemented by tokenCounter.
type TokenCounter interface {
	// TokensAvailable returns true if tokens are available. If false, it
	// returns a handle to use for waiting using
	// kvflowcontroller.WaitForHandlesAndChannels. This is for waiting
	// pre-evaluation.
	TokensAvailable(admissionpb.WorkClass) (available bool, tokenWaitingHandle TokenWaitingHandle)
	TryDeduct(
		context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens) kvflowcontrol.Tokens
	// Deduct deducts (without blocking) flow tokens for the given priority.
	Deduct(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
	// Return returns flow tokens for the given priority.
	Return(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
}

// TokenWaitingHandle is the interface for waiting for positive tokens.
type TokenWaitingHandle interface {
	// WaitChannel is the channel that will be signaled if tokens are possibly
	// available. If signaled, the caller must call
	// TryDeductAndUnblockNextWaiter.
	WaitChannel() <-chan struct{}
	// TryDeductAndUnblockNextWaiter is called to deduct some tokens. The tokens
	// parameter can be zero, when the waiter is only waiting for positive
	// tokens (such as when waiting before eval). granted <= tokens and the
	// tokens that have been deducted. haveTokens is true iff there are tokens
	// available after this grant. When the tokens parameter is zero, granted
	// will be zero, and haveTokens represents whether there were positive
	// tokens. If the caller is unsatisfied with the return values, it can
	// resume waiting using WaitChannel.
	TryDeductAndUnblockNextWaiter(tokens kvflowcontrol.Tokens) (granted kvflowcontrol.Tokens, haveTokens bool)
}

// StoreStreamSendTokensWatcher implements a watcher interface that will use
// at most one goroutine per SendTokenCounter and WorkClass that has no send
// tokens. Replicas (from different ranges) waiting for those tokens will call
// NotifyWhenAvailable to queue up for those send tokens.
type StoreStreamSendTokensWatcher interface {
	NotifyWhenAvailable(
		stc TokenCounter,
		wc admissionpb.WorkClass,
		tokensGrantedNotification TokenAvailableNotification,
	) StoreStreamSendTokenHandleID
	UpdateHandle(handle StoreStreamSendTokenHandleID, wc admissionpb.WorkClass)
	CancelHandle(handle StoreStreamSendTokenHandleID)
}

const InvalidStoreStreamSendTokenHandleID StoreStreamSendTokenHandleID = 0

type TokenAvailableNotification interface {
	// Notify is called when tokens are available to be granted.
	Notify()
}

type StoreStreamSendTokenHandleID int64

type StoreStreamSendTokenHandle struct {
	id           StoreStreamSendTokenHandleID
	bytesInQueue int64
	stc          TokenCounter
	wc           admissionpb.WorkClass
	notification TokenAvailableNotification
}

func NewStoreStreamSendTokensWatcher(stopper *stop.Stopper) *storeStreamSendTokensWatcher {
	ssstw := &storeStreamSendTokensWatcher{stopper: stopper}
	ssstw.mu.watchers = make(map[TokenCounter]*tokenWatcher)
	ssstw.mu.idSeq = 1
	return ssstw
}

// storeStreamSendTokensWatcher implements the StoreStreamSendTokensWatcher
// interface.
type storeStreamSendTokensWatcher struct {
	stopper *stop.Stopper

	mu struct {
		syncutil.Mutex

		// idSeq is used to generate unique IDs for each handle.
		idSeq    StoreStreamSendTokenHandleID
		watchers map[TokenCounter]*tokenWatcher
		handles  map[StoreStreamSendTokenHandleID]*StoreStreamSendTokenHandle
	}
}

type tokenWatcher struct {
	tracked map[admissionpb.WorkClass][]StoreStreamSendTokenHandleID
}

func (s *tokenWatcher) removeHandleLocked(handle StoreStreamSendTokenHandle) {
	for i := range s.tracked[handle.wc] {
		if s.tracked[handle.wc][i] == handle.id {
			s.tracked[handle.wc] = append(s.tracked[handle.wc][:i], s.tracked[handle.wc][i+1:]...)
			return
		}
	}
}

// NotifyWhenAvailable queues up for tokens for the given SendTokenCounter and
// WorkClass. When tokens are available, tokensGrantedNotification is called
// with the number of tokens granted.
func (s *storeStreamSendTokensWatcher) NotifyWhenAvailable(
	stc TokenCounter, wc admissionpb.WorkClass, tokensGrantedNotification TokenAvailableNotification,
) StoreStreamSendTokenHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	handle := StoreStreamSendTokenHandle{
		id:           s.mu.idSeq,
		stc:          stc,
		wc:           wc,
		notification: tokensGrantedNotification,
	}
	s.mu.idSeq++

	s.mu.handles[handle.id] = &handle
	watcher := s.getOrCreateTokenWatcherLocked(stc, wc)
	watcher.tracked[wc] = append(watcher.tracked[wc], handle.id)

	// This is the first token for the work class, counter pair, start watching.
	if len(watcher.tracked[handle.wc]) == 1 {
		s.watchTokens(context.Background(), handle.stc, wc, watcher)
	}

	return handle
}

// UpdateHandle updates the given handle with the new work class, removing it
// from the existing work class watcher. Note that the handle must already be
// being watched.
func (s *storeStreamSendTokensWatcher) UpdateHandle(
	handleID StoreStreamSendTokenHandleID, wc admissionpb.WorkClass,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	handle := s.mu.handles[handleID]
	if handle.wc == wc {
		// Nothing to do
		return
	}

	handle.wc = wc

	watcher := s.mu.watchers[handle.stc]
	watcher.removeHandleLocked(*handle)
	watcher.tracked[handle.wc] = append(watcher.tracked[handle.wc], handleID)

	// This is the first token for the work class, counter pair, start watching.
	if len(watcher.tracked[handle.wc]) == 1 {
		s.watchTokens(context.Background(), handle.stc, wc, watcher)
	}
}

func (s *storeStreamSendTokensWatcher) CancelHandle(handleID StoreStreamSendTokenHandleID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	handle := s.mu.handles[handleID]
	watcher := s.mu.watchers[handle.stc]
	watcher.removeHandleLocked(*handle)
	delete(s.mu.handles, handle.id)
}

func (s *storeStreamSendTokensWatcher) getOrCreateTokenWatcherLocked(
	stc TokenCounter, wc admissionpb.WorkClass,
) *tokenWatcher {
	watcher, ok := s.mu.watchers[stc]
	if !ok {
		watcher = &tokenWatcher{
			tracked: make(map[admissionpb.WorkClass][]StoreStreamSendTokenHandleID),
		}
		s.mu.watchers[stc] = watcher
	}

	return watcher
}

func (s *storeStreamSendTokensWatcher) watchTokens(
	ctx context.Context, stc TokenCounter, wc admissionpb.WorkClass, watcher *tokenWatcher,
) {
	_ = s.stopper.RunAsyncTask(context.Background(), "store-stream-token-watcher", func(ctx context.Context) {
		for {
			if exit := func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()

				return len(watcher.tracked[wc]) == 0
			}(); exit {
				return
			}

			available, handle := stc.TokensAvailable(wc)
			if !available {
				state, _ := kvflowcontroller.WaitForHandlesAndChannelsOld(ctx,
					s.stopper.ShouldQuiesce(),
					1, /* numHandles */
					[]interface{}{handle},
					nil, /* scratch */
				)
				switch state {
				case kvflowcontroller.ContextCanceled, kvflowcontroller.StopWaitSignaled:
					return
				case kvflowcontroller.WaitSuccess:
				}
			}

			s.mu.Lock()
			next := watcher.tracked[wc][0]
			watcher.tracked[wc] = watcher.tracked[wc][1:]
			watcher.tracked[wc] = append(watcher.tracked[wc], next)
			notify := s.mu.handles[next].notification.Notify
			s.mu.Unlock()

			notify()
		}
	})
}
