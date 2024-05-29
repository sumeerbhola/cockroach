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
	"math"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TODO: force-flush the leaseholder.

// TODO: kvflowcontrol and the packages it contains are sliced and diced quite
// fine, with the benefit of multiple code iterations to get to that final
// structure. We don't yet have that benefit, so we just lump things together
// for now, until most of the code is written and we know how to abstract into
// packages. There are things here that don't belong in kvflowconnectedstream,
// especially all the interfaces.

// TODO: many of the comments here are to guide the implementation. They will
// need to be cleaned up.

// TODO: we don't want force-flush entries to usually use up tokens since we
// don't want logical AC on the other side since it could cause an OOM.
// Check what we do now when we encode something with AC encoding and send it
// over -- it happens regardless of who we are sending too and whether we
// deducted tokens or not. So will do logical admission even for a node that
// has come back up. Got lucky with no OOM?
//
// TODO: two kinds of priority in the second byte. No logical admission and no
// priority override Keep these two fields separate in RaftMessageRequest.
//
// A RangeController exists when a local replica of the range is the raft
// leader. It does not have a goroutine of its own and reacts to events. No
// mutex inside RangeController should be ordered before Replica.raftMu, since
// some event notifications happen with Replica.raftMu already held. We
// consider the following events:
//
// - RaftEvent: This happens on the raftScheduler and already holds raftMu, so
//   the RangeController can be sure that any questions it asks of
//   RaftInterface are consistent with RaftEvent. A RaftEvent encompasses a
//   combination of a tick and ready -- it is possible that there is no
//   "Ready". We assume that as long as not all replicas are caught up,
//   RaftEvents are guaranteed to happen at some minimum frequency (e.g. the
//   500ms of COCKROACH_RAFT_TICK_INTERVAL).
//
// - ControllerSchedulerEvent: This happens on the raftScheduler and
//   represents some work that the RangeController had scheduled. If the
//   RangeController needs to call into RaftInterface, it must itself acquire
//   raftMu before doing so. Such events are used to dequeue raft entries from
//   the send-queue when tokens are available, or to force-flush.
//
// - SetReplicas: raftMu is already held. This ensures RaftEvent and
//   SetReplicas are serialized and the latest set of replicas provided by
//   SetReplicas is also what Raft is operating with. We will back this with a
//   data-structure under Replica.raftMu (and Replica.mu) that is updated in
//   setDescLockedRaftMuLocked. This consistency is important for multiple
//   reasons, including knowing which replicas to use when calling the various
//   methods in RaftInterface (i.e., it isn't only needed for quorum
//   calculaton as discussed on the slack thread
//   https://cockroachlabs.slack.com/archives/C06UFBJ743F/p1715692063606459?thread_ts=1715641995.372289&cid=C06UFBJ743F)
//
// - SetLeaseholder: This is not synchronized with raftMu. The callee should
//   be prepared to handle the case where the leaseholder is not even a known
//   replica, but will eventually be known. TODO: The comment may be incorrect.
//
// - ReplicaDisconnected: raftMu is not held. Informs that a replica has
//   its RaftTransport disconnected. This is necessary to prevent lossiness of
//   tokens. The aforementioned map under Replica.mu will be used to
//   ensure consistency. TODO: make it a narrower data-structure
//   mutex in Replica, so that Raft.mu is not held when calling RangeController.
//
//   Connection events are not communicated. The current state of connectivity
//   should be requested from RaftInterface when handling RaftEvent (in case
//   there are some disconnected replicas that have reconnected), SetReplicas
//   (to see if a new replica is connected). So we rely on liveness of
//   RaftEvent to notice reconnections.
//
// ======================================================================
// Aside on consistency of RangeDescriptor in Replica and the raft group conf:
//
// - handleDescResult and replicaStateMachine.maybeApplyConfChange both happen
// in ApplySideEffects which is called with raftMu held.
//
// - snapshot: processRaftSnapshotRequest is both calling RawNode.Step and
// then handleRaftReadyRaftMuLocked while holding raftMu, and the latter
// updates the desc and the former the raft group conf.
//
// - init after restart: initRaftMuLockedReplicaMuLocked does both
// initialization of the raft group and that of the descriptor.
//
// Since we are relying on this consistency, we should better document it
// ======================================================================
// Buffering and delayed transition to StateProbe:
// Replica.addUnreachableRemoteReplica causes that transition, and is caused by
// a drop in Replica.sendRaftMessageRequest. But since the
// RaftTransport has a queue, it should not be get filled up due to a very
// transient connection break and reestablishment. This is good since transitions
// to StateProbe can result in force flush of some other replica.
// ======================================================================
// Reproposal handling: We no longer do any special handling of reproposals.
// v1 was accounting before an entry emerged in Ready, so there was a higher chance
// of lossiness (it may never emerge). With v2, there is lossiness too, but less,
// and since both the proposal and reproposal are going to be persisted in the raft
// log we count them both.
// ======================================================================

type RangeController interface {
	// WaitForEval is called concurrently by all requests wanting to evaluate.
	//
	// TODO: Needs high concurrency. Use a copy-on-write scheme for whatever
	// data-structures are needed.
	WaitForEval(ctx context.Context, pri admissionpb.WorkPriority) error
	// HandleRaftEvent will be called from handleRaftReadyRaftMuLocked, including
	// the case of snapshot application.
	HandleRaftEvent(e RaftEvent) error
	// HandleControllerSchedulerEvent will be called from the raftScheduler when
	// an event the controller scheduled can be processed.
	HandleControllerSchedulerEvent() error
	// SetReplicas will be called from setDescLockedRaftMuLocked.
	//
	// A new follower here may already be in StateReplicate and have a
	// send-queue, so we should schedule a ControllerSchedulerEvent without
	// waiting for the next Ready.
	SetReplicas(replicas ReplicaSet) error
	// SetLeaseholder is called from leasePostApplyLocked.
	// TODO: I suspect raftMu is held here too.
	SetLeaseholder(replica roachpb.ReplicaID)
	// TransportDisconnected originates in RaftTransport.startProcessNewQueue.
	// To demux to the relevant ranges, the latest set of replicas for a range
	// must be known. We don't want to acquire Replica.raftMu in this iteration
	// from RaftTransport, since Replica.raftMu is held for longer than
	// Replica.mu (and so this read to demultiplex can encounter contention). We
	// will keep a map of StoreID=>ReplicaID in the Replica struct that is
	// updated when the RangeDescriptor is set (which holds both Replica.raftMu
	// and Replica.mu), and so this map can be read with either of these mutexes
	// (and we will read it with Replica.mu).
	//
	// Unlike v1, where this destroyed the connected-stream data-structure for
	// this replica, we could just return the inflight send tokens and keep the
	// rest of the state (send-queue and stats, and eval token deductions) since
	// it may still be in StateReplicate. Though if this is actually down and we
	// stay in StateReplicate, we are penalizing elastic work which will still
	// pace at rate of slowest replica (and the rate of this replica will be 0).
	// Actually, we should keep the connected-stream for a couple of ticks. This
	// will also ensure that this stream can participate in the quorum, and we
	// don't force-flush. But this will also be building up a send-queue, so we
	// may have a quorum of connected-streams with tokens, but we don't have a
	// quorum with an empty send-queue -- figure out a reasonable heuristic
	// (perhaps wait to force-flush until the non-empty send-queue has existed
	// for more than a tick).
	//
	// TODO: this is what motivates having a mutex in RangeController.
	TransportDisconnected(replica roachpb.ReplicaID)
	// Close the controller, since no longer the leader. Can be called concurrently
	// with other methods like WaitForEval. WaitForEval should unblock and return
	// without an error.
	Close()
}

// RaftEvent is an abstraction around raft.Ready, constructed in
// handleRaftReadyRaftMuLocked.
type RaftEvent interface {
	// Ready can return nil if there is no Ready.
	Ready() Ready
}

// Ready interface is an abstraction around raft.Ready, for the pull mode in
// which replication AC will operate. In this pull mode, the Raft leader will
// produce a Ready that has the new entries it knows about, but MsgApps will
// only be produced for a follower for (a) entries in (Match, Next), i.e.,
// entries that are already in-flight for that follower, and have been nacked,
// (b) empty MsgApps to ping the follower. Next is only advanced via pull,
// which is serviced via RaftInterface. In a sense, there is a NextUpperBound
// maintained by Raft, that never regresses in StateReplicate -- it advances
// whenever RaftInterface.MakeMsgApp is called. And everything in (Match,
// NextUpperBound) is the responsibility of Raft to retry. The existing notion
// of Next is protocol state internal to raft, regarding what to retry
// sending. NextUpperBound can regress if the replica transitions out of
// StateReplicate, and back into StateReplicate.
//
// The Ready described here is only the subset that is needed by replication
// AC -- heartbeats, appending to the local log etc. are not relevant.
//
// Ready must be called on every tick/ready of Raft since we cannot tolerate
// state transitions from
//
// StateReplicate => {state in all states: state != StateReplicate} =>
// StateReplicate
//
// that is not observed by RangeController. Since outgoing messages are sent
// in the tick/ready handling and a return from the intermediate state to
// StateReplicate relies on receiving responses to those outgoing messages, we
// should not miss any transitions. If stale messages received in a step can
// cause transitions out and back without observation, we can add a monotonic
// counter for each follower inside Raft (this is just local state at the
// leader), which will be incremented on every state transition and expose
// that via the RaftInterface.
type Ready interface {
	// Entries represents the new entries that are being added to the log.
	// This may be empty.
	//
	// The second byte of AC entries will encode the priority, so we will have
	// the priority, size, and index position, which is all that we need. Unlike
	// v1, where we are doing the accounting in
	// https://github.com/cockroachdb/cockroach/blob/f601b7b439ced71030bfdb0d9ba9cb4925420569/pkg/kv/kvserver/replica_proposal_buf.go#L1057-L1066
	// which is messy.
	Entries() []raftpb.Entry
	// RetransmitMsgApps returns the MsgApps that are being retransmitted, or
	// being used to ping the follower. These will never be queued by
	// replication AC, but it may adjust the priority before sending (due to
	// priority inheritance, when the Message has non-empty Entries).
	RetransmitMsgApps() []raftpb.Message
}

// RaftInterface abstracts what the RangeController needs from the raft
// package, when running at the leader. It also provides one piece of
// information that is not in the raft package -- the current connected state
// of the store, that we will use the RaftTransport to answer.
//
// NB: group membership and connectivity information is communicated to the
// RangeController via a separate channel, as in the v1 implementation, since
// it depends on information inside and outside Raft. The methods in Raft are
// the ones RangeController will call.
//
// The implementation should not need to acquire Replica.mu since we have not
// said anything about the relative lock ordering of Replica.mu and the
// internal mutexes in RangeController.
type RaftInterface interface {
	// FollowerState returns the current state of a follower. The value of
	// nextUpperBound is populated iff in StateReplicate. All entries >=
	// nextUpperBound have not yet had MsgApps constructed.
	//
	// When a follower transitions from {StateProbe,StateSnapshot} =>
	// StateReplicate, or was already in StateReplicate but was disconnected and
	// has now reconnected, we can now start trying to send MsgApps. We should
	// notice such transitions both in HandleRaftEvent and SetReplicas. We
	// should *not* construct MsgApps for a StateReplicate follower that is
	// disconnected -- there is no timeliness guarantee on how long a follower
	// will stay in StateReplicate despite it being down, and by sending such a
	// follower MsgApps that are not being received we are defeating flow
	// control (since we will have advanced nextUpperBound).
	//
	// RACv1 also cared about three other cases where the follower behaved as if
	// it were disconnected (a) paused follower, (b) follower is behind, (c)
	// follower is inactive (see
	// replicaFlowControlIntegrationImpl.notActivelyReplicatingTo). (b) and (c)
	// were needed since it paced at rate of slowest replica, while for regular
	// work we will in v2 pace at slowest in quorum (and we don't care about
	// elastic experiencing a hiccup, given it paces at rate of slowest). For
	// (a), we plan to remove follower pausing. So the v2 code will be
	// simplified.
	FollowerState(replicaID roachpb.ReplicaID) (state tracker.StateType, nextUpperBound uint64)
	FollowerTransportConnected(storeID roachpb.StoreID) bool
	// HighestEntryIndex is the highest index assigned in the log, and produced
	// in Ready.Entries(). If there have been no entries produced, since this
	// replica became the leader, this is the commit index.
	HighestEntryIndex() uint64
	// MakeMsgApp is used to construct a MsgApp for entries in [start, end).
	// REQUIRES: start == nextUpperBound and replicaID is in StateReplicate.
	// REQUIRES: maxSize > 0.
	//
	// If the sum of all entries in [start,end) are <= maxSize, all will be
	// returned. Else, entries will be returned until, and including, the first
	// entry that causes maxSize to be equaled or exceeded. This implies at
	// least one entry will be returned in the MsgApp on success.
	//
	// Returns raft.ErrCompacted error if log truncated. If no error,
	// nextUpperBound is advanced to be equal to end. If raft.ErrCompacted is
	// returned, and the replica was in StateReplicate prior to this call, it
	// will transition to StateSnapshot.
	MakeMsgApp(replicaID roachpb.ReplicaID, start, end uint64, maxSize int64) (raftpb.Message, error)
}

// Scheduler abstracts the raftScheduler to allow the RangeController to
// schedule its own internal processing.
type Scheduler interface {
	ScheduleControllerEvent(rangeID roachpb.RangeID)
}

// MessageSender abstracts Replica.sendRaftMessage. The context used is always
// Replica.raftCtx, so we do not need to pass it.
//
// REQUIRES: msg is a MsgApp. The follower is a member and is in StateReplicate.
type MessageSender interface {
	// SendRaftMessage ...
	//
	// priorityOverride can be kvserverpb.PriorityNotOverriddenForFlowControl
	//
	// Implementation:
	//
	// On the receiver Replica.stepRaftGroup is called with
	// kvserverpb.RaftMessageRequest. And we do the AdmitRaftEntry call in
	// handleRaftReadyRaftMuLocked. By then we have no access to the wrapper
	// that is RaftMessageRequest.
	//
	// We will also be mediating Raft doing retries. If it is retrying and
	// previously we had sent index i with one priority override and now send
	// with a different priority override, we don't want to change the tracker.
	// Also, we don't know which of these messages made it to the other side, so
	// when the tokens are returned there is ambiguity.
	//
	// Solution:
	// The sender will only track the original priority in tracker. The receiver will
	// know both the original and overridden priority. Will replace the second byte in entry
	// with the overridden priority before handing to stepRaftGroup. RaftAdmissionMeta has the
	// original. Will use the overridden one to admit and then the original to return tokens.
	//
	// Indices 3E, 4E, 5E, 6R, where E is elastic and R is regular. 5E, 6R sent
	// with 5R', 6R. Wnen 5R' returned, will also return 3E, 4E. So there is
	// some early return here. But this is ok since we only have 8MB of elastic
	// outstanding (across all ranges) so the send-q will be the one with most
	// of the elastic bytes. Indices 3E, 4E, 5E, ..., 99E, 100R Send-q is 5E to
	// 100R. Then we can send all of these using R.
	SendRaftMessage(
		ctx context.Context, priorityOverride admissionpb.WorkPriority, msg raftpb.Message)
}

type RangeControllerOptions struct {
	// TODO: synchronization.
	// RaftMu    *syncutil.Mutex
	RangeID  roachpb.RangeID
	TenantID roachpb.TenantID
	// LocalReplicaID is the ReplicaID of the local replica, which is the
	// leader.
	LocalReplicaID roachpb.ReplicaID

	SSTokenCounter    StoreStreamsTokenCounter
	SendTokensWatcher StoreStreamSendTokensWatcher
	RaftInterface     RaftInterface
	MessageSender     MessageSender
	Scheduler         Scheduler
}

// RangeControllerInitState is the initial state at the time of creation.
type RangeControllerInitState struct {
	// Must include RangeControllerOptions.ReplicaID.
	ReplicaSet ReplicaSet
	// Leaseholder may be set to NoReplicaID, in which case the leaseholder is
	// unknown.
	Leaseholder roachpb.ReplicaID
}

// NoReplicaID is a special value of roachpb.ReplicaID, which can never be a
// valid ID.
const NoReplicaID roachpb.ReplicaID = 0

type ReplicaSet map[roachpb.ReplicaID]roachpb.ReplicaDescriptor

type RangeControllerImpl struct {
	opts        RangeControllerOptions
	replicaSet  ReplicaSet
	leaseholder roachpb.ReplicaID

	// State for waiters. When anything in voterSets changes, voterSetRefreshCh
	// is closed, and replaced with a new channel. The voterSets is
	// copy-on-write, so waiters make a shallow copy.
	voterSets         []voterSet
	voterSetRefreshCh chan struct{}

	replicaMap map[roachpb.ReplicaID]*replicaState

	scheduledReplicas map[roachpb.ReplicaID]struct{}
}

type voterSet []voterStateForWaiters

type voterStateForWaiters struct {
	replicaID        roachpb.ReplicaID
	isLeader         bool
	isLeaseHolder    bool
	isStateReplicate bool
	evalTokenCounter TokenCounter
}

var _ RangeController = &RangeControllerImpl{}

func NewRangeControllerImpl(
	o RangeControllerOptions, init RangeControllerInitState,
) *RangeControllerImpl {
	rc := &RangeControllerImpl{
		opts:              o,
		replicaSet:        ReplicaSet{},
		leaseholder:       init.Leaseholder,
		replicaMap:        map[roachpb.ReplicaID]*replicaState{},
		scheduledReplicas: make(map[roachpb.ReplicaID]struct{}),
	}
	rc.updateReplicaSetAndMap(init.ReplicaSet)
	rc.updateVoterSets()
	return rc
}

func (rc *RangeControllerImpl) updateReplicaSetAndMap(newSet ReplicaSet) {
	prevSet := rc.replicaSet
	for r := range prevSet {
		desc, ok := newSet[r]
		if !ok {
			rs := rc.replicaMap[r]
			rs.close()
			delete(rc.replicaMap, r)
		} else {
			// It does not matter if the replica has changed from voter to non-voter
			// or vice-versa, in that we still need to replicate to it.
			rs := rc.replicaMap[r]
			rs.desc = desc
		}
	}
	for r, desc := range newSet {
		_, ok := prevSet[r]
		if ok {
			// Already handled above.
			continue
		}
		rc.replicaMap[r] = NewReplicaState(rc, desc)
	}
}

// replicaSet, replicaMap, leaseholder are up-to-date.
func (rc *RangeControllerImpl) updateVoterSets() {
	// TODO: some callers of updateVoterSets should first figure out if anything
	// has changed in the voters.

	setCount := 1
	for _, r := range rc.replicaSet {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()
		if !isOld && !isNew {
			continue
		}
		if !isOld && isNew {
			setCount++
			break
		}
	}
	var voterSets []voterSet
	for len(voterSets) < setCount {
		voterSets = append(voterSets, voterSet{})
	}
	for _, r := range rc.replicaSet {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()
		if !isOld && !isNew {
			continue
		}
		// Is a voter.
		rs := rc.replicaMap[r.ReplicaID]
		vsfw := voterStateForWaiters{
			replicaID:        r.ReplicaID,
			isLeader:         r.ReplicaID == rc.opts.LocalReplicaID,
			isLeaseHolder:    r.ReplicaID == rc.leaseholder,
			isStateReplicate: rs.replicaSendStream != nil && rs.replicaSendStream.connectedState.isStateReplicate(),
			evalTokenCounter: rs.evalTokenCounter,
		}
		if isOld {
			voterSets[0] = append(voterSets[0], vsfw)
		}
		if isNew && setCount == 2 {
			voterSets[1] = append(voterSets[1], vsfw)
		}
	}
	rc.voterSets = voterSets
	close(rc.voterSetRefreshCh)
	rc.voterSetRefreshCh = make(chan struct{})

	// TODO: go through the voters and figure out if we need to force-flush
	// something. Account for already ongoing force-flushes.
}

func (rc *RangeControllerImpl) WaitForEval(
	ctx context.Context, pri admissionpb.WorkPriority,
) error {
	wc := admissionpb.WorkClassFromPri(pri)
	waitForAllReplicateHandles := false
	if wc == admissionpb.ElasticWorkClass {
		waitForAllReplicateHandles = true
	}
	var handles []tokenWaitingHandleInfo
	var scratch []reflect.SelectCase
retry:
	// Snapshot the voterSets and voterSetRefreshCh.
	// TODO: synchronization.
	vss := rc.voterSets
	vssRefreshCh := rc.voterSetRefreshCh
	if vssRefreshCh == nil {
		// RangeControllerImpl is closed.
		return nil
	}
	for _, vs := range vss {
		quorumCount := (len(vs) + 2) / 2
		haveEvalTokensCount := 0
		handles := handles[:0]
		requiredWait := false
		for _, v := range vs {
			available, handle := v.evalTokenCounter.TokensAvailable(wc)
			if available {
				haveEvalTokensCount++
				continue
			}
			// Don't have eval tokens, and have a handle.
			handleInfo := tokenWaitingHandleInfo{
				handle: handle,
				requiredWait: v.isLeader || v.isLeaseHolder ||
					(waitForAllReplicateHandles && v.isStateReplicate),
			}
			handles = append(handles, handleInfo)
			if !requiredWait && handleInfo.requiredWait {
				requiredWait = true
			}
		}
		remainingForQuorum := quorumCount - haveEvalTokensCount
		if remainingForQuorum < 0 {
			remainingForQuorum = 0
		}
		if remainingForQuorum > 0 || requiredWait {
			var state WaitEndState
			state, scratch = WaitForEval(ctx, vssRefreshCh, handles, remainingForQuorum, scratch)
			switch state {
			case WaitSuccess:
				continue
			case ContextCanceled:
				return ctx.Err()
			case RefreshWaitSignaled:
				goto retry
			}
		}
	}
	return nil
}

func (rc *RangeControllerImpl) HandleRaftEvent(e RaftEvent) error {
	// Ensure that the replicaSendStreams are consistent with the current Raft
	// state.
	for r, rs := range rc.replicaMap {
		// The state may have changed due to events that are not observed by
		// RangeControllerImpl.
		//
		// - Transitions to StateProbe can happen via the circuit breaker, or a nack.
		//
		// - Transitions to StateReplicate can happen because a MsgAppResp was
		//   received by Raft.
		//
		// - Transitions from StateReplicate => StateSnapshot are caused by RangeControllerImpl,
		//   but there are also transitions from StateProbe => StateSnapshot (TODO: confirm) on
		//   receiving a MsgAppResp with a Match that is too far in the past.
		//
		// Transport connected => disconnected transitions will always be communicated via
		// TransportDisconnected, but the reverse transition is not.
		state, nextUB := rc.opts.RaftInterface.FollowerState(r)
		switch state {
		case tracker.StateProbe:
			if rs.replicaSendStream != nil {
				rs.replicaSendStream.close()
				rs.replicaSendStream = nil
			}
		case tracker.StateReplicate:
			if rs.replicaSendStream == nil {
				rs.createReplicaSendStream(nextUB)
			} else {
				// replicaSendStream already exists, but may be in state snapshot or
				// replicateSoftDisconnected
				switch rs.replicaSendStream.connectedState {
				case replicateConnected: // Nothing to do.
				case replicateSoftDisconnected:
					isConnected := rs.parent.opts.RaftInterface.FollowerTransportConnected(rs.desc.StoreID)
					if isConnected {
						rs.replicaSendStream.changeConnectedStateInStateReplicate(isConnected)
					}
				case snapshot:
					isConnected := rs.parent.opts.RaftInterface.FollowerTransportConnected(rs.desc.StoreID)
					rs.replicaSendStream.changeToStateReplicate(isConnected, nextUB)
				}
			}
		case tracker.StateSnapshot:
			if rs.replicaSendStream != nil && rs.replicaSendStream.connectedState != snapshot {
				rs.replicaSendStream.changeToStateSnapshot()
			}
		}
	}

	// Process ready.
	ready := e.Ready()
	if ready == nil {
		return nil
	}
	// Send the MsgApps we have been asked to send. Note that these may cause
	// queueing up in Replica.addUnreachableRemoteReplica, but those will be
	// handed to Raft later, so this act will not cause a transition from
	// StateReplicate => StateProbe.
	msgApps := ready.RetransmitMsgApps()
	for i := range msgApps {
		rc.opts.MessageSender.SendRaftMessage(
			context.TODO(), admissionpb.WorkPriority(kvserverpb.PriorityNotOverriddenForFlowControl), msgApps[i])
	}

	entries := ready.Entries()
	if len(entries) == 0 {
		return nil
	}
	// The entries are the only things we handle here for StateReplicate
	// replicas with empty send-queues. If a replica has a send-queue of
	// existing entries, they are already trying to eliminate it, and we simply
	// queue.
	for r, rs := range rc.replicaMap {
		if r == rc.opts.LocalReplicaID {
			// Local replica, which is the leader. These will have a
			// MsgStorageAppend that is not mediated here, but we do need to account
			// for tokens.
			for i := range entries {
				entryFCState := getFlowControlState(entries[i])
				if !entryFCState.usesFlowControl {
					continue
				}
				wc := admissionpb.WorkClassFromPri(entryFCState.priority)
				rs.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens)
				rs.replicaSendStream.advanceNextRaftIndexAndSent(entryFCState)
			}
			continue
		}
		if rs.replicaSendStream == nil {
			continue
		}
		if rs.replicaSendStream.connectedState != snapshot && rs.replicaSendStream.isEmptySendQueue() {
			// Consider sending.
			// If leaseholder just send. If the leaseholder has a send-queue we won't be in this
			// path, but a force-flush must be ongoing.
			isLeaseholder := r == rc.leaseholder
			from := entries[0].Index
			// [from, to) is what we will send.
			to := entries[0].Index
			toIsFinalized := false
			for i := range entries {
				entryFCState := getFlowControlState(entries[i])
				wc := admissionpb.WorkClassFromPri(entryFCState.priority)
				if toIsFinalized {
					if entries[i].Index == to && !entryFCState.usesFlowControl {
						// Include additional entries that are not subject to AC, since we
						// always have tokens for those.
						to++
					} else {
						rs.replicaSendStream.advanceNextRaftIndexAndQueued(entryFCState)
					}
					continue
				}
				// INVARIANT: !toIsFinalized.
				send := false
				if isLeaseholder {
					if entryFCState.usesFlowControl {
						rs.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens)
					}
					send = true
				} else {
					if entryFCState.usesFlowControl {
						tokens := rs.sendTokenCounter.TryDeduct(context.TODO(), wc, entryFCState.tokens)
						if tokens > 0 {
							send = true
							if tokens < entryFCState.tokens {
								toIsFinalized = true
								// Deduct the remaining for this entry.
								rs.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens-tokens)
							}
						}
						// Else send stays false.
					} else {
						send = true
					}
				}
				if send {
					to++
					rs.replicaSendStream.advanceNextRaftIndexAndSent(entryFCState)
				} else {
					toIsFinalized = true
					rs.replicaSendStream.advanceNextRaftIndexAndQueued(entryFCState)
				}
			}
			if to > from {
				// Have deducted the send tokens. Proceed to send.
				msg, err := rc.opts.RaftInterface.MakeMsgApp(r, from, to, math.MaxInt64)
				if err != nil {
					panic("in Ready.Entries, but unable to create MsgApp -- couldn't have been truncated")
				}
				rc.opts.MessageSender.SendRaftMessage(
					context.TODO(), kvserverpb.PriorityNotOverriddenForFlowControl, msg)
			}
			// Else nothing to send.
		} else {
			// In StateSnapshot, or in StateReplicate with a queue. Need to queue.
			for i := range entries {
				entryFCState := getFlowControlState(entries[i])
				rs.replicaSendStream.advanceNextRaftIndexAndSent(entryFCState)
			}
		}
	}
	return nil
}

type entryFlowControlState struct {
	pos kvflowcontrolpb.RaftLogPosition
	// usesFlowControl can be false for entries that don't use flow control.
	// This can happen if RAC is partly disabled e.g. disabled for regular work,
	// or for conf changes. In the former case the send-queue will also be
	// disabled (i.e., equivalent to RACv1).
	usesFlowControl bool
	priority        admissionpb.WorkPriority
	tokens          kvflowcontrol.Tokens
}

func getFlowControlState(entry raftpb.Entry) entryFlowControlState {
	// TODO: change the payload encoding and parsing, and delegate the priority
	// parsing to that.
	return entryFlowControlState{
		pos: kvflowcontrolpb.RaftLogPosition{
			Term:  entry.Term,
			Index: entry.Index,
		},
		usesFlowControl: false,                 // TODO:
		priority:        admissionpb.NormalPri, // TODO:
		tokens:          kvflowcontrol.Tokens(len(entry.Data)),
	}
}

func (rc *RangeControllerImpl) HandleControllerSchedulerEvent() error {
	for r := range rc.scheduledReplicas {
		rs, ok := rc.replicaMap[r]
		scheduleAgain := false
		if ok && rs.replicaSendStream != nil {
			scheduleAgain = rs.replicaSendStream.scheduled()
		}
		if !scheduleAgain {
			delete(rc.scheduledReplicas, r)
		}
	}
	if len(rc.scheduledReplicas) > 0 {
		rc.opts.Scheduler.ScheduleControllerEvent(rc.opts.RangeID)
	}
	return nil
}

func (rc *RangeControllerImpl) scheduleReplica(r roachpb.ReplicaID) {
	rc.scheduledReplicas[r] = struct{}{}
	if len(rc.scheduledReplicas) == 1 {
		rc.opts.Scheduler.ScheduleControllerEvent(rc.opts.RangeID)
	}
}

func (rc *RangeControllerImpl) SetReplicas(replicas ReplicaSet) error {
	rc.updateReplicaSetAndMap(replicas)
	rc.updateVoterSets()
	return nil
}

func (rc *RangeControllerImpl) SetLeaseholder(replica roachpb.ReplicaID) {
	if replica == rc.leaseholder {
		return
	}
	rc.leaseholder = replica
	rc.updateVoterSets()
	rs, ok := rc.replicaMap[replica]
	if !ok {
		// Ignore. Should we panic?
	}
	if rs.replicaSendStream != nil && rs.replicaSendStream.connectedState != snapshot &&
		!rs.replicaSendStream.isEmptySendQueue() {
		rs.replicaSendStream.scheduleForceFlush()
	}
}

func (rc *RangeControllerImpl) TransportDisconnected(replica roachpb.ReplicaID) {
	rs, ok := rc.replicaMap[replica]
	if ok && rs.replicaSendStream != nil && rs.replicaSendStream.connectedState == replicateConnected {
		rs.replicaSendStream.changeConnectedStateInStateReplicate(false)
	}
}

func (rc *RangeControllerImpl) Close() {
	close(rc.voterSetRefreshCh)
	rc.voterSetRefreshCh = nil
	for _, rs := range rc.replicaMap {
		rs.close()
	}
}

type replicaState struct {
	parent *RangeControllerImpl
	// stream aggregates across the streams for the same (tenant, store). This
	// is the identity that is used to deduct tokens or wait for tokens to be
	// positive.
	stream            kvflowcontrol.Stream
	evalTokenCounter  TokenCounter
	sendTokenCounter  TokenCounter
	desc              roachpb.ReplicaDescriptor
	replicaSendStream *replicaSendStream
}

func NewReplicaState(parent *RangeControllerImpl, desc roachpb.ReplicaDescriptor) *replicaState {
	stream := kvflowcontrol.Stream{TenantID: parent.opts.TenantID, StoreID: desc.StoreID}
	rs := &replicaState{
		parent:            parent,
		stream:            stream,
		evalTokenCounter:  parent.opts.SSTokenCounter.EvalTokenCounterForStream(stream),
		sendTokenCounter:  parent.opts.SSTokenCounter.SendTokenCounterForStream(stream),
		desc:              desc,
		replicaSendStream: nil,
	}
	state, nextUB := parent.opts.RaftInterface.FollowerState(desc.ReplicaID)
	if state == tracker.StateReplicate {
		rs.createReplicaSendStream(nextUB)
	}
	return rs
}

func (rs *replicaState) createReplicaSendStream(nextUpperBound uint64) {
	isConnected := rs.parent.opts.RaftInterface.FollowerTransportConnected(rs.desc.StoreID)
	rss := newReplicaSendStream(rs, replicaSendStreamInitState{
		isConnected:   isConnected,
		indexToSend:   nextUpperBound,
		nextRaftIndex: rs.parent.opts.RaftInterface.HighestEntryIndex() + 1,
		// TODO: these need to be based on some history observed by RangeControllerImpl.
		approxMaxPriority:   admissionpb.NormalPri,
		approxMeanSizeBytes: 1000,
	})
	rs.replicaSendStream = rss
	if rs.parent.leaseholder == rs.desc.ReplicaID && !rss.isEmptySendQueue() {
		rss.scheduleForceFlush()
	}
}

func (rs *replicaState) close() {
	if rs.replicaSendStream != nil {
		rs.replicaSendStream.close()
	}
}

// TODO: update.
//
// replicaSendStream is the state for replicas in StateReplicate. We need to maintain
// a send-queue for them. We may return some tokens if a connection breaks but
// still want to keep them in StateReplicate? For elastic work this may not be
// great? Well, we can close the ch, and replace it with a new channel.
// We should keep the state of priority etc. in the send-queue. Yes, this is principled.
//
// If Entries pop out and we have not checked whether the stream is connected, we may
// not construct MsgApps, even though the RaftTransport has some buffering. And the
// RaftTransport may reconnect before the next ready/tick. So it is good to have some
// buffer. Consider sending until it returns false, even if the transport is closed.
// Then consider the stream truly disconnected.
//
// So this is a replica-stream. Soft-down when disconnect notified. Up when
// known to be connected. Hard-down if soft-down and send returns false. If we
// stop evaluating because have to wait for hard-down then we have a problem
// in that may never feed something to switch to hard-down.
// So keep feeding until hard-down. And remove from elastic wait when soft-down.
// If circuit-breaker does not trip too bad. When soft-down, those messages not
// subject to AC since not tracking them on the sender.
//
// **what is the channel situation in soft-down** Close and replace channel.
//
// TODO: replicaSendStream could also encompass StateSnapshot, when transitioned
// from StateReplicate to StateSnapshot. The problem is that indexToSend can
// will need to advance. We also want to eventually use the mediation of
// tracker on when to send snapshot. So overall we should encompass
// StateSnapshot.
//
// "breaker will open in 3-6 seconds after no more TCP packets are flowing. If
// we assume 6 seconds, then that is ~1600 messages/second."
type replicaSendStream struct {
	parent *replicaState
	// TODO: synchronization.
	mu syncutil.Mutex

	connectedState connectedState

	// indexToSendInitial is the indexToSend when this transitioned to
	// StateReplicate. Only indices >= indexToSendInitial are tracked.
	indexToSendInitial uint64
	tracker            Tracker

	// nextRaftIndexInitial is the value of nextRaftIndex when this transitioned
	// to StateReplicate.
	nextRaftIndexInitial uint64

	sendQueue struct {
		// State of send-queue. [indexToSend, nextRaftIndex) have not been sent.
		indexToSend   uint64
		nextRaftIndex uint64

		// Approximate stats for send-queue. For indices < nextRaftIndexInitial.
		approxMaxPriority   admissionpb.WorkPriority
		approxMeanSizeBytes kvflowcontrol.Tokens

		// Precise stats for send-queue. For indices >= nextRaftIndexInitial.
		priorityCount map[admissionpb.WorkPriority]int64
		sizeSum       kvflowcontrol.Tokens

		// watcherHandleID, deductedForScheduler, forceFlushScheduled are only
		// relevant when connectedState != snapshot, and the send-queue is
		// non-empty.
		//
		// If watcherHandleID != InvalidStoreStreamSendTokenHandleID, i.e., we have
		// registered a handle to watch for send tokens to become available. In this
		// case deductedForScheduler.tokens == 0 and !forceFlushScheduled.
		//
		// If watcherHandleID == InvalidStoreStreamSendTokenHandleID, we have
		// either deducted some tokens that we have not used, i.e.,
		// deductedForScheduler.tokens > 0, or forceFlushScheduled (i.e., we don't
		// need tokens). Both can be true, i.e. deductedForScheduler.tokens > 0
		// and forceFlushScheduled. In this case, we are waiting to be scheduled
		// in the raftScheduler to do the sending.
		watcherHandleID      StoreStreamSendTokenHandleID
		deductedForScheduler struct {
			pri    admissionpb.WorkPriority
			tokens kvflowcontrol.Tokens
		}
		// Only relevant when connectedState != snapshot.
		forceFlushScheduled bool
	}
	// Eval state.
	eval struct {
		// Only for indices >= nextRaftIndexInitial. These are either in the
		// send-queue, or in the tracker.
		tokensDeducted [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
	}

	closed bool
}

// Initial state provided to constructor of replicaSendStream.
type replicaSendStreamInitState struct {
	isConnected bool
	// [indexToSend, nextRaftIndex) are known to the (local) leader and need to
	// be sent to this replica. This is the initial send-queue.
	//
	// INVARIANT: is-local-replica => indexToSend == nextRaftIndex, i.e., no
	// send-queue.
	indexToSend   uint64
	nextRaftIndex uint64

	// Approximate stats for the initial send-queue.
	approxMaxPriority   admissionpb.WorkPriority
	approxMeanSizeBytes kvflowcontrol.Tokens
}

func newReplicaSendStream(
	parent *replicaState, init replicaSendStreamInitState,
) *replicaSendStream {
	// Must be in StateReplicate on creation.
	connectedState := replicateConnected
	if !init.isConnected {
		connectedState = replicateSoftDisconnected
	}
	rss := &replicaSendStream{
		parent:               parent,
		connectedState:       connectedState,
		indexToSendInitial:   init.indexToSend,
		nextRaftIndexInitial: init.nextRaftIndex,
	}
	rss.tracker.Init(parent.stream)
	rss.sendQueue.indexToSend = init.indexToSend
	rss.sendQueue.nextRaftIndex = init.nextRaftIndex
	rss.sendQueue.approxMaxPriority = init.approxMaxPriority
	rss.sendQueue.approxMeanSizeBytes = init.approxMeanSizeBytes
	rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	return rss
}

func (rss *replicaSendStream) close() {
	if rss.connectedState != snapshot {
		// Will cause all tokens to be returned etc.
		rss.changeToStateSnapshot()
	}
	rss.closed = true
}

// An entry is being sent that was never in the send-queue.
func (rss *replicaSendStream) advanceNextRaftIndexAndSent(state entryFlowControlState) {
	if rss.connectedState == snapshot {
		panic("")
	}
	if state.pos.Index != rss.sendQueue.indexToSend {
		panic("")
	}
	if state.pos.Index != rss.sendQueue.nextRaftIndex {
		panic("")
	}
	rss.sendQueue.indexToSend++
	rss.sendQueue.nextRaftIndex++
	if !state.usesFlowControl {
		return
	}
	rss.tracker.Track(context.TODO(), state.priority, state.tokens, state.pos)
	rss.parent.evalTokenCounter.Deduct(
		context.TODO(), admissionpb.WorkClassFromPri(state.priority), state.tokens)
}

func (rss *replicaSendStream) scheduleForceFlush() {
	if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.sendQueue.forceFlushScheduled = true
	rss.parent.parent.scheduleReplica(rss.parent.desc.ReplicaID)
}

func (rss *replicaSendStream) scheduled() (scheduleAgain bool) {
	// 5MB.
	const MaxBytesToSend kvflowcontrol.Tokens = 5 << 20
	bytesToSend := MaxBytesToSend
	if !rss.sendQueue.forceFlushScheduled {
		bytesToSend = rss.sendQueue.deductedForScheduler.tokens
	}
	if bytesToSend == 0 {
		return false
	}
	msg, err := rss.parent.parent.opts.RaftInterface.MakeMsgApp(
		rss.parent.desc.ReplicaID, rss.sendQueue.indexToSend, rss.sendQueue.nextRaftIndex,
		int64(bytesToSend))
	if err != nil {
		if !errors.Is(err, raft.ErrCompacted) {
			panic(err)
		}
		rss.changeToStateSnapshot()
		return
	}
	rss.dequeueFromQueueAndSend(msg)
	isEmpty := rss.isEmptySendQueue()
	if isEmpty {
		if rss.sendQueue.forceFlushScheduled {
			rss.sendQueue.forceFlushScheduled = false
		}
		rss.returnDeductedFromSchedulerTokens()
		return false
	}
	// INVARIANT: !isEmpty.
	if rss.sendQueue.forceFlushScheduled || rss.sendQueue.deductedForScheduler.tokens > 0 {
		return true
	} else {
		pri := rss.queuePriority()
		rss.sendQueue.watcherHandleID = rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
			rss.parent.sendTokenCounter, admissionpb.WorkClassFromPri(pri), rss)
		return false
	}
}

func (rss *replicaSendStream) dequeueFromQueueAndSend(msg raftpb.Message) {
	remainingTokens := rss.sendQueue.deductedForScheduler.tokens
	priAlreadyDeducted := kvserverpb.PriorityNotOverriddenForFlowControl
	noRemainingTokens := false
	if remainingTokens > 0 {
		priAlreadyDeducted = rss.sendQueue.deductedForScheduler.pri
	} else {
		noRemainingTokens = true
	}
	wcAlreadyDeducted := admissionpb.WorkClassFromPri(priAlreadyDeducted)
	// It is possible that the Notify raced with an enqueue (or scheduled with
	// an enqueue) and the send-queue has some regular work now, and the
	// priAlreadyDeducted was of a lower priority corresponding to elastic work.
	// We just consume these tokens and apply the override corresponding to
	// priAlreadyDeducted. It is considered harmless for regular work to consume
	// elastic tokens. If that delays their logical admission, it will not harm
	// later arriving regular work, that will use regular tokens.

	for _, entry := range msg.Entries {
		if rss.sendQueue.indexToSend != entry.Index {
			panic("")
		}
		rss.sendQueue.indexToSend++
		entryFCState := getFlowControlState(entry)
		if !entryFCState.usesFlowControl {
			continue
		}
		rss.sendQueue.sizeSum -= entryFCState.tokens
		rss.sendQueue.priorityCount[entryFCState.priority]--
		wc := wcAlreadyDeducted
		if priAlreadyDeducted == kvserverpb.PriorityNotOverriddenForFlowControl {
			wc = admissionpb.WorkClassFromPri(entryFCState.priority)
		}
		if noRemainingTokens {
			rss.parent.sendTokenCounter.Deduct(context.TODO(), wc, entryFCState.tokens)
		} else {
			remainingTokens -= entryFCState.tokens
			if remainingTokens <= 0 {
				noRemainingTokens = true
				rss.parent.sendTokenCounter.Deduct(context.TODO(), wc, -remainingTokens)
				remainingTokens = 0
			}
		}
		// TODO: Also track priAlreadyDeducted as the override.
		rss.tracker.Track(context.TODO(), entryFCState.priority, entryFCState.tokens,
			kvflowcontrolpb.RaftLogPosition{Term: entry.Term, Index: entry.Index})
	}
	rss.parent.parent.opts.MessageSender.SendRaftMessage(context.TODO(), priAlreadyDeducted, msg)
	rss.sendQueue.deductedForScheduler.tokens = remainingTokens
	if remainingTokens == 0 {
		rss.sendQueue.deductedForScheduler.pri = kvserverpb.PriorityNotOverriddenForFlowControl
	}
}

// TODO: **resume here **
func (rss *replicaSendStream) advanceNextRaftIndexAndQueued(entry entryFlowControlState) {
	if entry.pos.Index != rss.sendQueue.nextRaftIndex {
		panic("")
	}
	wasEmpty := rss.isEmptySendQueue()
	rss.sendQueue.nextRaftIndex++
	rss.sendQueue.sizeSum += entry.tokens
	if entry.usesFlowControl {
		var priority admissionpb.WorkPriority
		if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
			// May need to update it.
			priority = rss.queuePriority()
		}
		rss.sendQueue.priorityCount[entry.priority]++
		if rss.connectedState == snapshot {
			// Do not deduct eval-tokens in StateSnapshot, since there is no
			// guarantee these will be returned.
			return
		}
		wcChanged := false
		entryWC := admissionpb.WorkClassFromPri(entry.priority)
		if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID &&
			entry.priority > priority {
			existingWC := admissionpb.WorkClassFromPri(priority)
			if existingWC != entryWC {
				wcChanged = true
			}
		}
		rss.eval.tokensDeducted[entryWC] += entry.tokens
		rss.parent.evalTokenCounter.Deduct(context.TODO(), entryWC, entry.tokens)
		if wasEmpty {
			// Register notification.
			rss.sendQueue.watcherHandleID = rss.parent.parent.opts.SendTokensWatcher.NotifyWhenAvailable(
				rss.parent.sendTokenCounter, entryWC, rss)
		} else if wcChanged {
			// Update notification
			rss.parent.parent.opts.SendTokensWatcher.UpdateHandle(rss.sendQueue.watcherHandleID, entryWC)
		}
	}
}

// Notify implements TokenAvailableNotification.
func (rss *replicaSendStream) Notify() {
	// TODO: concurrency. raftMu is not held, and not being called from raftScheduler.
	if rss.closed || rss.connectedState == snapshot {
		// Must have canceled the handle and the cancellation raced with the
		// notification.
		return
	}
	pri := rss.queuePriority()
	wc := admissionpb.WorkClassFromPri(pri)
	queueSize := rss.queueSize()
	tokens := rss.parent.sendTokenCounter.TryDeduct(context.TODO(), wc, queueSize)
	if tokens > 0 {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.sendQueue.deductedForScheduler.pri = pri
	rss.sendQueue.deductedForScheduler.tokens = tokens
	// TODO: tell RangeController to schedule rss on raftScheduler.
}

func (rss *replicaSendStream) isEmptySendQueue() bool {
	return rss.sendQueue.indexToSend == rss.sendQueue.nextRaftIndex
}

// REQUIRES: send-queue is not empty.
func (rss *replicaSendStream) queuePriority() admissionpb.WorkPriority {
	initialized := false
	var maxPri admissionpb.WorkPriority
	if rss.sendQueue.indexToSend < rss.nextRaftIndexInitial {
		maxPri = rss.sendQueue.approxMaxPriority
		initialized = true
	}
	for pri, count := range rss.sendQueue.priorityCount {
		if count > 0 && (!initialized || pri > maxPri) {
			maxPri = pri
		}
	}
	return maxPri
}

// REQUIRES: send-queue is not empty.
func (rss *replicaSendStream) queueSize() kvflowcontrol.Tokens {
	var size kvflowcontrol.Tokens
	countWithApproxStats := rss.nextRaftIndexInitial - rss.sendQueue.indexToSend
	if countWithApproxStats > 0 {
		size = kvflowcontrol.Tokens(countWithApproxStats) * rss.sendQueue.approxMeanSizeBytes
	}
	size += rss.sendQueue.sizeSum
	return size
}

func (rss *replicaSendStream) changeConnectedStateInStateReplicate(isConnected bool) {
	if isConnected {
		if rss.connectedState != replicateSoftDisconnected {
			panic("")
		}
		rss.connectedState = replicateConnected
	}
	if !isConnected {
		if rss.connectedState != replicateConnected {
			panic("")
		}
		rss.connectedState = replicateSoftDisconnected
	}
}

func (rss *replicaSendStream) changeToStateSnapshot() {
	rss.connectedState = snapshot
	// The tracker must only contain entries in < rss.sendQueue.indexToSend.
	// These may not have been received by the replica and will not get resent
	// by Raft, so we have no guarantee those tokens will be returned. So return
	// all tokens in the tracker.
	rss.tracker.UntrackAll(context.TODO(), func(pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens) {
		rss.parent.sendTokenCounter.Return(context.TODO(), admissionpb.WorkClassFromPri(pri), tokens)
	})
	// For the same reason, return all eval tokens deducted.
	for wc := range rss.eval.tokensDeducted {
		if rss.eval.tokensDeducted[wc] > 0 {
			rss.parent.evalTokenCounter.Return(context.TODO(), admissionpb.WorkClass(wc), rss.eval.tokensDeducted[wc])
			rss.eval.tokensDeducted[wc] = 0
		}
	}
	if rss.sendQueue.watcherHandleID != InvalidStoreStreamSendTokenHandleID {
		rss.parent.parent.opts.SendTokensWatcher.CancelHandle(rss.sendQueue.watcherHandleID)
		rss.sendQueue.watcherHandleID = InvalidStoreStreamSendTokenHandleID
	}
	rss.returnDeductedFromSchedulerTokens()
	rss.sendQueue.forceFlushScheduled = false
}

func (rss *replicaSendStream) returnDeductedFromSchedulerTokens() {
	if rss.sendQueue.deductedForScheduler.tokens > 0 {
		wc := admissionpb.WorkClassFromPri(rss.sendQueue.deductedForScheduler.pri)
		rss.parent.sendTokenCounter.Return(context.TODO(), wc, rss.sendQueue.deductedForScheduler.tokens)
		rss.sendQueue.deductedForScheduler.tokens = 0
	}
}

func (rss *replicaSendStream) changeToStateReplicate(isConnected bool, indexToSend uint64) {
	if rss.sendQueue.nextRaftIndex < indexToSend {
		panic("")
	}
	if rss.sendQueue.indexToSend > indexToSend {
		panic("")
	}
	if rss.connectedState != snapshot {
		panic("")
	}
	state := replicateConnected
	if !isConnected {
		state = replicateSoftDisconnected
	}
	rss.connectedState = state
	// INVARIANT: rss.sendQueue.indexToSend <= indexToSend <=
	// rss.sendQueue.nextRaftIndex. Typically, both will be <, since we
	// transitioned to StateSnapshot since rss.sendQueue.indexToSend was
	// truncated, and there have likely been some proposed entries since the
	// snapshot was applied. So we will start off with some entries in the
	// send-queue.

	// NB: the tracker entries have already been returned in
	// changeToStateSnapshot. And so have the eval tokens. We have partially or
	// fully emptied the send-queue and we don't want to iterate over the
	// remaining members to precisely figure out what to deduct from
	// eval-tokens, since that may require reading from storage.

	rss.indexToSendInitial = indexToSend
	rss.sendQueue.indexToSend = indexToSend
	totalCount := int64(0)
	var maxPri admissionpb.WorkPriority
	for pri, count := range rss.sendQueue.priorityCount {
		if count > 0 {
			if totalCount == 0 || pri > maxPri {
				maxPri = pri
			}
			totalCount += count
			rss.sendQueue.priorityCount[pri] = 0
		}
	}
	meanSizeBytes := kvflowcontrol.Tokens(0)
	if totalCount > 0 {
		meanSizeBytes = rss.sendQueue.sizeSum / kvflowcontrol.Tokens(totalCount)
	}
	if rss.nextRaftIndexInitial > rss.sendQueue.indexToSend {
		// The approx stats are still relevant.
		if rss.sendQueue.approxMaxPriority > maxPri {
			maxPri = rss.sendQueue.approxMaxPriority
		}
		if totalCount == 0 {
			meanSizeBytes = rss.sendQueue.approxMeanSizeBytes
		} else {
			meanSizeBytes = kvflowcontrol.Tokens(0.9*float64(meanSizeBytes) + 0.1*float64(rss.sendQueue.approxMeanSizeBytes))
		}
	}
	rss.sendQueue.approxMaxPriority = maxPri
	rss.sendQueue.approxMeanSizeBytes = meanSizeBytes
	rss.sendQueue.sizeSum = 0
	rss.nextRaftIndexInitial = rss.sendQueue.nextRaftIndex
	if !rss.isEmptySendQueue() {
		rss.Notify()
		if rss.sendQueue.deductedForScheduler.tokens == 0 {
			// Weren't able to deduct any tokens.
			// TODO: register for watcher
		} else {
			for {
				rss.scheduled()
				if rss.sendQueue.deductedForScheduler.tokens == 0 {
					// TODO: register for watcher if send-queue is non-empty.
				} else {
					// TODO: actually schedule.
				}
			}
		}
	}
}

// Message is received to return flow tokens for pri, for all positions <= upto.
// Return send-tokens and eval-tokens.
func (rss *replicaSendStream) flowTokensReturn(
	pri admissionpb.WorkPriority, upto kvflowcontrolpb.RaftLogPosition,
) kvflowcontrol.Tokens {
	// TODO
	return 0
}

type connectedState uint32

// Additional connectivity state in StateReplicate.
//
// Local replicas are always in state connected.
//
// Initial state for a replicaSendStream is either connected or
// softDisconnected, depending on whether FollowerTransportConnected returns
// true or false. Transport stream closure is immediately notified
// (TransportDisconnected), but the reconnection is only learnt about when
// polling FollowerTransportConnected (which happens in a HandleRaftEvent).
// The transport stream closure transitions connected => softDisconnected.
// FollowerTransportConnected polling transitions from softDisconnected state
// to connected. In softDisconnected state, and if the send-queue was already
// empty, we continue to send MsgApps if send-tokens are available, since
// there is buffering capacity in the RaftTransport, which allows for some
// buffering and immediate sending when the RaftTransport reconnects (which
// may happen before the next HandleRaftEvent), which is desirable.
// Unfortunately we cannot do any flow token accounting in this state, since
// those tokens may be lossy. The first false return value from
// SendRaftMessage in softDisconnected will also trigger a notification to
// Raft that the replica is unreachable (see Replica.sendRaftMessage calling
// Replica.addUnreachableRemoteReplica), and that raftpb.MsgUnreachable will
// cause the transition out of StateReplicate to StateProbe. The false return
// value happens either when the (generous) RaftTransport buffer is full, or
// when the circuit breaker opens. The circuit breaker opens 3-6s after no
// more TCP packets are flowing, so we can send about 6s of messages without
// flow control accounting, which is considered acceptable.
//
// In state softDisconnected, we do not wait on eval tokens for this stream
// for elastic work. If we waited for eval tokens and the eval tokens are
// negative due to some other replica, we may not evaluate new work, which
// means we would not call SendRaftMessage hence never triggering the
// transition to StateProbe. That would unnecessarily stop elastic work when a
// node is down.
//
// We call this state softDisconnected since MsgApps are still being generated.
//
// Initial states: {connected, softDisconnected}
// State transitions:
//
//	connected => softDisconnected
//	softDisconnected => connected
//	* => replicaSendStream closed
const (
	replicateConnected connectedState = iota
	replicateSoftDisconnected
	snapshot
)

func (cs connectedState) isStateReplicate() bool {
	return cs == replicateConnected || cs == replicateSoftDisconnected
}
