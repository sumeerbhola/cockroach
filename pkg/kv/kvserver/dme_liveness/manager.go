// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package dme_liveness

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/dme_liveness/dme_livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type localStoreState struct {
	storeID         dme_livenesspb.StoreIdentifier
	persistentState StorageProvider

	epoch                  int64
	heartbeatAndSupportMap map[dme_livenesspb.StoreIdentifier]*heartbeatAndSupport

	upperBoundHeartbeatEndTimeIsGCed hlc.ClockTimestamp
	parent                           *managerImpl
}

func newLocalStoreState(ls localStore, parent *managerImpl) (*localStoreState, error) {
	epoch, err := ls.StorageProvider.readCurrentEpoch()
	if err != nil {
		return nil, err
	}
	epoch++
	if err := ls.StorageProvider.updateCurrentEpoch(epoch); err != nil {
		return nil, err
	}
	heartbeatAndSupportMap := map[dme_livenesspb.StoreIdentifier]*heartbeatAndSupport{}
	err = ls.StorageProvider.readCurrentSupport(
		func(store dme_livenesspb.StoreIdentifier, support dme_livenesspb.Support) {
			hbs := newHeartbeatAndSupportStruct(store, epoch, support)
			heartbeatAndSupportMap[store] = hbs
		})
	if err != nil {
		return nil, err
	}
	lss := &localStoreState{
		storeID: dme_livenesspb.StoreIdentifier{
			NodeID:  parent.o.nodeID,
			StoreID: ls.StoreID,
		},
		persistentState:        ls.StorageProvider,
		epoch:                  epoch,
		heartbeatAndSupportMap: heartbeatAndSupportMap,
		parent:                 parent,
	}
	for _, hbs := range heartbeatAndSupportMap {
		hbs.start(lss)
	}
	return lss, nil
}

func (lss *localStoreState) leaseExpiry(
	isLeaseHolder bool,
	now hlc.ClockTimestamp,
	epoch int64,
	startTime hlc.ClockTimestamp,
	replicaSets [][]dme_livenesspb.StoreIdentifier,
) (kvserverpb.DistributedEpochLeaseLiveness, error) {
	if isLeaseHolder {
		if now.Less(lss.upperBoundHeartbeatEndTimeIsGCed) {
			return kvserverpb.DistributedEpochLeaseLiveness{},
				errors.Errorf("now is too old to evaluate lease expiry")
		}
		// Need to find end time for a quorum
		var accumulatedEndTime hlc.Timestamp
		for i := range replicaSets {
			replicas := replicaSets[i]
			quorumCount := (len(replicas) + 2) / 2
			var endTimes []hlc.Timestamp
			for _, r := range replicas {
				if r.StoreID == lss.storeID.StoreID {
					// Always supports itself.
					//
					// TODO: if restarts, should it be supporting itself? It should be
					// shedding leases. Do we handle this here or outside this subsystem.
					// The current lss.epoch may have advanced due to reasons other than
					// restart, so need to distinguish between that current value and the
					// epoch initialized after restart.
					quorumCount--
				}
				hbs := lss.heartbeatAndSupportMap[r]
				if hbs == nil {
					continue
				}
				if hbs.myHeartbeat.EndTime == (hlc.Timestamp{}) || hbs.myHeartbeat.Epoch > epoch {
					// Not supporting epoch. Cannot have gc'd now, so know that never supported now.
				} else {
					// Supporting epoch.
					if hbs.myHeartbeat.StartTime.LessEq(now) {
						endTimes = append(endTimes, hbs.myHeartbeat.EndTime)
					}
				}
			}
			slices.SortFunc(endTimes, func(a, b hlc.Timestamp) int {
				return a.Compare(b)
			})
			if len(endTimes) < quorumCount {
				return kvserverpb.DistributedEpochLeaseLiveness{Expiration: hlc.Timestamp(startTime)}, nil
			}
			index := len(endTimes) - quorumCount
			endTime := endTimes[index]
			if accumulatedEndTime == (hlc.Timestamp{}) {
				accumulatedEndTime = endTime
			} else if endTime.Less(accumulatedEndTime) {
				accumulatedEndTime = endTime
			}
		}
		return kvserverpb.DistributedEpochLeaseLiveness{Expiration: accumulatedEndTime}, nil
	}
	// TODO:
	return kvserverpb.DistributedEpochLeaseLiveness{}, nil
}

func (lss *localStoreState) addRemoteStore(storeID dme_livenesspb.StoreIdentifier) {
	hbs, ok := lss.heartbeatAndSupportMap[storeID]
	if ok {
		return
	}
	hbs = newHeartbeatAndSupportStruct(storeID, lss.epoch, dme_livenesspb.Support{})
	lss.heartbeatAndSupportMap[storeID] = hbs
	hbs.start(lss)
}

func (lss *localStoreState) removeRemoteStore(storeID dme_livenesspb.StoreIdentifier) {
	hbs, ok := lss.heartbeatAndSupportMap[storeID]
	if !ok {
		return
	}
	delete(lss.heartbeatAndSupportMap, storeID)
	// TODO: also update upperBoundHeartbeatEndTimeIsGCed.
	hbs.stop()
}

func (lss *localStoreState) handleHeartbeat(
	from dme_livenesspb.StoreIdentifier, msg dme_livenesspb.Heartbeat,
) error {
	hbs, ok := lss.heartbeatAndSupportMap[from]
	if !ok {
		return errors.Errorf("unknown store s%s", from.StoreID)
	}
	return hbs.handleHeartbeat(msg)
}

func (lss *localStoreState) handleSupportState(
	from dme_livenesspb.StoreIdentifier, msg dme_livenesspb.SupportState,
) error {
	hbs, ok := lss.heartbeatAndSupportMap[from]
	if !ok {
		return errors.Errorf("unknown store s%s", from.StoreID)
	}
	return hbs.handleSupportState(msg)
}

func (lss *localStoreState) handleWithdrawSupport(
	from dme_livenesspb.StoreIdentifier, msg dme_livenesspb.WithdrawSupport,
) error {
	hbs, ok := lss.heartbeatAndSupportMap[msg.Store]
	if !ok {
		return errors.Errorf("unknown store s%s", msg.Store.StoreID)
	}
	return hbs.handleWithdrawSupport(from, msg)
}

func (lss *localStoreState) sendSupportState(to dme_livenesspb.StoreIdentifier) {
	var msg dme_livenesspb.SupportState
	for _, hbs := range lss.heartbeatAndSupportMap {
		msg.Support = append(msg.Support, dme_livenesspb.SupportWithStoreID{
			Store:   hbs.storeID,
			Support: hbs.mySupportForStoreID,
		})
	}
	lss.parent.o.messageSender.SendSupportState(dme_livenesspb.Header{
		From: lss.storeID,
		To:   to,
	}, msg)
}

func (lss *localStoreState) incrementEpoch(minEpoch int64) error {
	if minEpoch < lss.epoch {
		return nil
	}
	err := lss.persistentState.updateCurrentEpoch(minEpoch)
	if err != nil {
		return err
	}
	lss.epoch = minEpoch
	return nil
}

type heartbeatAndSupport struct {
	storeID dme_livenesspb.StoreIdentifier
	// TODO(sumeer): make this a history of 2, and place a upper bound hlc.Timestamp
	// corresponding to Support.EndTime's that could have been GC'd.

	// If Support.EndTime is empty, it indicates no support. In that case
	// Support.StartTime is the StartTime used in the first heartbeat sent for
	// that epoch.
	myHeartbeat    dme_livenesspb.Support
	heartbeatTimer *timeutil.Timer

	// If Support.EndTime is empty, it indicates no support. In that case
	// Support.StartTime is (best-effort) the EndTime of the last epoch that was
	// supported. This best-effort allows for some limited memory, that can be
	// used to upper-bound when support was withdrawn and help choose a
	// lower-bound for a new lease. Since support is withdrawn based on
	// hlc.Clock advancing past the EndTime, moving a value that is a
	// hlc.Timestamp to a value that is a hlc.ClockTimestamp is safe.
	mySupportForStoreID dme_livenesspb.Support
	supportTimer        *timeutil.Timer

	// This map does not include parent.StoreIdentifier.
	storeIDSupportForOtherStores map[dme_livenesspb.StoreIdentifier]dme_livenesspb.Support

	parent *localStoreState
}

// Only creates the struct.
func newHeartbeatAndSupportStruct(
	storeID dme_livenesspb.StoreIdentifier, epoch int64, mySupportForStoreID dme_livenesspb.Support,
) *heartbeatAndSupport {
	return &heartbeatAndSupport{
		storeID: storeID,
		myHeartbeat: dme_livenesspb.Support{
			Epoch: epoch,
		},
		heartbeatTimer:               timeutil.NewTimer(),
		mySupportForStoreID:          mySupportForStoreID,
		supportTimer:                 timeutil.NewTimer(),
		storeIDSupportForOtherStores: nil,
		parent:                       nil,
	}
}

func (hbs *heartbeatAndSupport) start(parent *localStoreState) {
	hbs.parent = parent
	stopper := parent.parent.o.stopper
	stopper.RunAsyncTask(context.Background(), "dme_liveness.heartbeat", func(_ context.Context) {
		select {
		case <-hbs.heartbeatTimer.C:
			hbs.heartbeatTimer.Read = true
			hbs.heartbeatTimerExpiry()
		case <-stopper.ShouldQuiesce():
			return
		}
	})
	stopper.RunAsyncTask(context.Background(), "dme_liveness.support", func(_ context.Context) {
		select {
		case <-hbs.supportTimer.C:
			hbs.supportTimer.Read = true
			hbs.supportTimerExpiry()
		case <-stopper.ShouldQuiesce():
			return
		}
	})
	hbs.startHeartbeatingEpoch()
	if hbs.mySupportForStoreID.EndTime != (hlc.Timestamp{}) {
		// Give some grace period.
		dur := hbs.parent.parent.o.livenessExpiryInterval()
		hbs.supportTimer.Reset(dur)
	}
}

func (hbs *heartbeatAndSupport) stop() {
	hbs.heartbeatTimer.Stop()
	hbs.supportTimer.Stop()
	// TODO: need to stop the goroutines.
}

// The epoch has never been heartbeated. Used when node starts or when increment
// heartbeat epoch due to support withdrawal.
func (hbs *heartbeatAndSupport) startHeartbeatingEpoch() {
	now := hbs.parent.parent.o.clock.NowAsClockTimestamp()
	hbs.myHeartbeat.StartTime = now
	hbs.sendHeartbeat(now)
}

func (hbs *heartbeatAndSupport) heartbeatTimerExpiry() {
	hbs.sendHeartbeat(hbs.parent.parent.o.clock.NowAsClockTimestamp())
}

func (hbs *heartbeatAndSupport) supportTimerExpiry() {
	if hbs.mySupportForStoreID.EndTime == (hlc.Timestamp{}) {
		// Not supporting. Ignore.
		return
	}
	now := hbs.parent.parent.o.clock.NowAsClockTimestamp()
	if hlc.Timestamp(now).LessEq(hbs.mySupportForStoreID.EndTime) {
		// Expired. Withdraw support.
		err := hbs.withdrawSupport(hbs.mySupportForStoreID.Epoch + 1)
		if err != nil {
			// TODO(sumeer): retry N times?
			panic(err)
		}
	} else {
		dur := hbs.mySupportForStoreID.EndTime.WallTime - now.WallTime
		hbs.supportTimer.Reset(time.Duration(dur) + time.Millisecond)
	}
}

func (hbs *heartbeatAndSupport) sendHeartbeat(now hlc.ClockTimestamp) {
	dur := hbs.parent.parent.o.livenessExpiryInterval()
	endTime := hlc.Timestamp(now).AddDuration(dur)
	hbs.parent.parent.o.messageSender.SendHeartbeat(
		dme_livenesspb.Header{
			From: hbs.parent.storeID,
			To:   hbs.storeID,
		}, dme_livenesspb.Heartbeat{
			Epoch:      hbs.myHeartbeat.Epoch,
			Now:        now,
			Expiration: endTime,
		})
	// Divide by 2 is a temporary hack.
	hbs.heartbeatTimer.Reset(dur / 2)
}

func (hbs *heartbeatAndSupport) withdrawSupport(nextEpoch int64) error {
	withdrawTime := hbs.mySupportForStoreID.EndTime
	if withdrawTime == (hlc.Timestamp{}) {
		// Never supported hbs.mySupportForStoreID.Epoch.
		withdrawTime = hbs.mySupportForStoreID.EndTime
		if withdrawTime == (hlc.Timestamp{}) {
			// Never supported any epoch.
			withdrawTime = hlc.Timestamp{WallTime: 1}
		}
	}
	supportProto := dme_livenesspb.SupportWithStoreID{
		Store: hbs.storeID,
		Support: dme_livenesspb.Support{
			Epoch:     nextEpoch,
			StartTime: hlc.ClockTimestamp(withdrawTime),
			EndTime:   hlc.Timestamp{},
		},
	}
	err := hbs.parent.persistentState.updateSupport(supportProto)
	if err != nil {
		return err
	}
	hbs.mySupportForStoreID = supportProto.Support
	return nil
}

func (hbs *heartbeatAndSupport) handleHeartbeat(msg dme_livenesspb.Heartbeat) error {
	err := hbs.parent.parent.o.clock.UpdateAndCheckMaxOffset(context.Background(), msg.Now)
	if err != nil {
		return err
	}
	if msg.Epoch < hbs.mySupportForStoreID.Epoch ||
		(msg.Epoch == hbs.mySupportForStoreID.Epoch && msg.Expiration.LessEq(hbs.mySupportForStoreID.EndTime)) {
		// Stale message. Ignore
	} else {
		supportProto := dme_livenesspb.SupportWithStoreID{
			Store: hbs.storeID,
			Support: dme_livenesspb.Support{
				Epoch:     msg.Epoch,
				StartTime: msg.Now,
				EndTime:   msg.Expiration,
			},
		}
		usedMsg := true
		// Already have support, then use that StartTime.
		if hbs.mySupportForStoreID.Epoch == msg.Epoch && !(hbs.myHeartbeat.EndTime == (hlc.Timestamp{})) {
			usedMsg = false
			if hbs.myHeartbeat.StartTime.Less(supportProto.Support.StartTime) {
				supportProto.Support.StartTime = hbs.mySupportForStoreID.StartTime
			} else {
				usedMsg = true
			}
			if supportProto.Support.EndTime.Less(hbs.myHeartbeat.EndTime) {
				supportProto.Support.EndTime = hbs.myHeartbeat.EndTime
			} else {
				usedMsg = true
			}
		}
		if usedMsg {
			err := hbs.parent.persistentState.updateSupport(supportProto)
			if err != nil {
				return err
			}
			hbs.mySupportForStoreID = supportProto.Support
			dur := hbs.mySupportForStoreID.EndTime.WallTime -
				hbs.parent.parent.o.clock.NowAsClockTimestamp().WallTime
			if dur < 0 {
				dur = 0
			}
			hbs.supportTimer.Reset(time.Duration(dur) + time.Millisecond)
		}
	}
	hbs.sendSupportState()
	return nil
}

func (hbs *heartbeatAndSupport) handleSupportState(msg dme_livenesspb.SupportState) error {
	// storeIDSupportForOtherStores doesn't need to be monotonic, so we simply
	// clear the existing state and replace it.
	for k := range hbs.storeIDSupportForOtherStores {
		delete(hbs.storeIDSupportForOtherStores, k)
	}
	for _, s := range msg.Support {
		if err := hbs.parent.parent.o.clock.UpdateAndCheckMaxOffset(context.Background(), s.Support.StartTime); err != nil {
			return err
		}
		if s.Store == hbs.parent.storeID {
			if hbs.myHeartbeat.Epoch == s.Support.Epoch && s.Support.EndTime != (hlc.Timestamp{}) {
				// On current epoch and providing support.
				if hbs.myHeartbeat.EndTime == (hlc.Timestamp{}) {
					// Did not have support.
					hbs.myHeartbeat = s.Support
				} else {
					// Had support. Try to extend it.
					if s.Support.StartTime.Less(hbs.myHeartbeat.StartTime) {
						hbs.myHeartbeat.StartTime = s.Support.StartTime
					}
					if hbs.myHeartbeat.EndTime.Less(s.Support.EndTime) {
						hbs.myHeartbeat.EndTime = s.Support.EndTime
					}
				}
			} else if hbs.myHeartbeat.Epoch < s.Support.Epoch {
				// Withdrawal.
				if hbs.myHeartbeat.EndTime != (hlc.Timestamp{}) {
					// Had support. Since s.Support.StartTime is a hlc.ClockTimestamp,
					// it must be reflected in the local clock, AND, s.Support.StartTime
					// >= hbs.myHeartbeat.EndTime. So hbs.myHeartbeat.EndTime is not in
					// the future.
					endClockTime := hlc.ClockTimestamp(hbs.myHeartbeat.EndTime)
					if hbs.parent.upperBoundHeartbeatEndTimeIsGCed.Less(endClockTime) {
						hbs.parent.upperBoundHeartbeatEndTimeIsGCed = endClockTime
					}
				}
				if s.Support.Epoch > hbs.parent.epoch {
					if err := hbs.parent.incrementEpoch(s.Support.Epoch); err != nil {
						panic(err)
					}
				}
				hbs.myHeartbeat.Epoch = s.Support.Epoch
				hbs.startHeartbeatingEpoch()
			}
			// Else stale. Ignore
		}
		hbs.storeIDSupportForOtherStores[s.Store] = s.Support
	}
	return nil
}

// Must only be called if hbs.mySupportForStoreID.EndTime is either (a) empty,
// or (b) in the past.
func (hbs *heartbeatAndSupport) handleWithdrawSupport(
	from dme_livenesspb.StoreIdentifier, msg dme_livenesspb.WithdrawSupport,
) error {
	if msg.MinSupportEpoch > hbs.mySupportForStoreID.Epoch &&
		hbs.mySupportForStoreID.EndTime == (hlc.Timestamp{}) {
		err := hbs.withdrawSupport(msg.MinSupportEpoch)
		if err != nil {
			return err
		}
	}
	hbs.parent.sendSupportState(from)
	return nil
}

func (hbs *heartbeatAndSupport) sendSupportState() {
	hbs.parent.sendSupportState(hbs.storeID)
}

type managerImpl struct {
	o           Options
	localStores map[roachpb.StoreID]*localStoreState
}

func newManager(o Options) (Manager, error) {
	localStores := map[roachpb.StoreID]*localStoreState{}
	m := &managerImpl{
		o:           o,
		localStores: localStores,
	}
	for _, ls := range o.stores {
		lss, err := newLocalStoreState(ls, m)
		if err != nil {
			return nil, err
		}
		localStores[ls.StoreID] = lss
	}
	return m, nil
}

func (m *managerImpl) AddRemoteStore(storeID dme_livenesspb.StoreIdentifier) {
	for _, lss := range m.localStores {
		lss.addRemoteStore(storeID)
	}
}

func (m *managerImpl) RemoveRemoteStore(storeID dme_livenesspb.StoreIdentifier) {
	for _, lss := range m.localStores {
		lss.removeRemoteStore(storeID)
	}
}

func (m *managerImpl) HandleHeartbeat(
	header dme_livenesspb.Header, msg dme_livenesspb.Heartbeat,
) error {
	lss, ok := m.localStores[header.To.StoreID]
	if !ok {
		return errors.Errorf("store s%s not found", header.To.StoreID.String())
	}
	return lss.handleHeartbeat(header.From, msg)
}

func (m *managerImpl) HandleSupportState(
	header dme_livenesspb.Header, msg dme_livenesspb.SupportState,
) error {
	lss, ok := m.localStores[header.To.StoreID]
	if !ok {
		return errors.Errorf("store s%s not found", header.To.StoreID.String())
	}
	return lss.handleSupportState(header.From, msg)
}

func (m *managerImpl) HandleWithdrawSupport(
	header dme_livenesspb.Header, msg dme_livenesspb.WithdrawSupport,
) error {
	lss, ok := m.localStores[header.To.StoreID]
	if !ok {
		return errors.Errorf("store s%s not found", header.To.StoreID.String())
	}
	return lss.handleWithdrawSupport(header.From, msg)
}

func (m *managerImpl) FillLeaseProposal(
	start hlc.ClockTimestamp,
	prevLease roachpb.Lease,
	nextLeaseHolder roachpb.ReplicaDescriptor,
	transfer bool,
	descriptor roachpb.RangeDescriptor,
) (*roachpb.DistributedEpochLease, error) {
	// TODO:
	return nil, nil
}

// TODO: cache the result for a RangeDescriptor so that don't need to ask again for same
// RangeDescriptor, Lease pair as long as now is less than the previously returned timestamp.

func (m *managerImpl) LeaseExpiry(
	now hlc.ClockTimestamp, lease roachpb.Lease, descriptor roachpb.RangeDescriptor,
) (kvserverpb.DistributedEpochLeaseLiveness, error) {
	// descriptor is the latest applied descriptor before lease was applied.
	if descriptor.Generation != lease.DistributedEpoch.RangeGeneration {
		return kvserverpb.DistributedEpochLeaseLiveness{},
			errors.Errorf("lease cannot have applied for this RangeDescriptor")
	}
	// Assume that it applied, i.e., DistributedEpochLease.{Support,SupportWithdrawal} was
	// verified.
	// At this point in time we are interested in whether there is adequate support from
	// the replicas in RangeDescriptor.
	epoch := lease.DistributedEpoch.Epoch
	startTime := lease.Start
	if now.Less(startTime) {
		return kvserverpb.DistributedEpochLeaseLiveness{}, errors.Errorf("now is too old")
	}
	leaseHolderStoreID := dme_livenesspb.StoreIdentifier{
		NodeID:  lease.Replica.NodeID,
		StoreID: lease.Replica.StoreID,
	}
	isLeaseHolder := leaseHolderStoreID.NodeID == m.o.nodeID
	replicas, myStoreID := quorumsNeeded(m.o.nodeID, descriptor)
	lss, ok := m.localStores[myStoreID]
	if !ok {
		return kvserverpb.DistributedEpochLeaseLiveness{},
			errors.Errorf("don't have a replica for this range")
	}
	return lss.leaseExpiry(isLeaseHolder, now, epoch, startTime, replicas)
}

func quorumsNeeded(
	nodeID roachpb.NodeID, descriptor roachpb.RangeDescriptor,
) (replicas [][]dme_livenesspb.StoreIdentifier, myStoreID roachpb.StoreID) {
	// TODO:
	return nil, 0
}
