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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/dme_liveness/dme_livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Interfaces implemented by the distributed multi-epoch range lease
// sub-system, for integrating with the rest of kvserver.

// Inputs/outputs need to include ReplicaDescriptors, Lease, LeaseStatus, etc.

type RangeLeaseHelper interface {
	// FillLeaseProposal ...
	//
	// If prevLease is empty, there has never been a leaseholder.
	//
	// If transfer is true, this is a cooperative transfer being proposed by the
	// prevLease leaseholder.
	//
	// descriptor is the latest applied RangeDescriptor known to the node that is proposing
	// this lease.
	FillLeaseProposal(
		start hlc.ClockTimestamp,
		prevLease roachpb.Lease,
		nextLeaseHolder roachpb.ReplicaDescriptor,
		transfer bool,
		descriptor roachpb.RangeDescriptor,
	) (*roachpb.DistributedEpochLease, error)

	// LeaseExpiry gives us liveness information about the lease. Only two kinds
	// of replicas are permitted to call this: a replica that had the lease, and
	// wants to know if it has the lease at now; a replica that thinks it is the
	// leader, and wants to know if the lease if the lease is valid at now. In
	// the latter case it is possible:
	// - that the lease is not valid, but the leader continues to think it is
	//   valid because the leaseholder's criteria to step down is stricter.
	//
	// - this replica is no longer the leader, and has stale RangeDescriptor
	//   information, and incorrectly thinks the lease is no longer valid. When
	//   it tries to propose a new lease (say it becomes the leader by then, so
	//   this hazard is not eliminated simply by having only the leader
	//   propose), it must use the same RangeDescriptor information in
	//   FillLeaseProposal (because the start parameter to FillLeaseProposal was
	//   the now value here). This will ensure that the lease proposal will also
	//   be stale and not get applied.
	//
	// If this is called at the non-leaseholder, and the lease should be
	// challenged, the callee will also use MessageSender.SendWithdrawSupport to
	// try to challenge the lease. TODO(sumeer): will the caller be calling this
	// frequently; is there some periodicity; can we influence the period --
	// ideally we want the caller to call this after the expected RTT with the
	// nodes we are asking to withdraw support.
	LeaseExpiry(
		now hlc.ClockTimestamp,
		lease roachpb.Lease,
		descriptor roachpb.RangeDescriptor,
	) (kvserverpb.DistributedEpochLeaseLiveness, error)
}

// TODO(sumeer): remove this hack.
var DistEpochLeaseHelperSingleton RangeLeaseHelper

type localStore struct {
	roachpb.StoreID
	StorageProvider
}

type Options struct {
	nodeID roachpb.NodeID
	stores []localStore
	clock  hlc.Clock

	// Allowed to change (since backed by cluster setting).
	livenessExpiryInterval func() time.Duration

	messageSender MessageSender
	stopper       *stop.Stopper
}

// MessageSender ...
//
// All message are one-way.
//
// The MessageSender is responsible for repeating a message until it is
// certain that the receiver has received the message. It can do backoff if it
// suspects the receiver is down.
//
// Logical messages are Heartbeat, HeartbeatResponse, SupportState,
// WithdrawSupport. These all have a Header, though the header is not
// physically included in these message declarations (since many of these
// logical messages can be bundled into a single physical message).
//
// MessageSender is responsible for retransmitting the latest Heartbeat,
// HeartbeatResponse, SupportState for the given Header until it is known to
// have been received (backoff is allowed). Since the number of distinct
// Headers is O(num-stores), the state remembered in the MessageSender is
// small.
//
// MessageSender is responsible for retransmitting the latest WithdrawSupport
// for the given (Header, WithdrawSupport.Store). In the worst-case this is
// O(num-stores^2) of space.
//
// Messages can be delivered out of order, and it is the receiver's
// responsibility to ignore stale messages.
//
// Implementation note: use regular RPCs where the request can bundle all the
// unacked messages. The empty response serves as an ack.
//
// None of these have error return values since transient errors need to be
// handled internally. If a store is known by the MessageSender to be
// permanently removed, it can drop the messages (it is possible that
// Manager.RemoveRemoteStore has not yet been called, which is ok).
// TODO(sumeer): implement.
type MessageSender interface {
	SendHeartbeat(header dme_livenesspb.Header, msg dme_livenesspb.Heartbeat)
	SendSupportState(header dme_livenesspb.Header, msg dme_livenesspb.SupportState)
	SendWithdrawSupport(header dme_livenesspb.Header, msg dme_livenesspb.WithdrawSupport)
}

// Don't do any batching. Pebble already batches WAL writes. And everything else is not
// real batching. So make the interface synchronous.

// StorageProvider is an interface to write to and retrieve persistent state
// for distributed multi-epoch liveness in a local store. It contains
// information about support this local store has from other stores, and the
// support this local store is giving to other stores.
type StorageProvider interface {
	// readCurrentEpoch returns the current epoch for this local store. Used at startup. If no
	// value is found, 0 is returned.
	readCurrentEpoch() (int64, error)
	// updateCurrentEpoch persists a new epoch for this local store.
	updateCurrentEpoch(epoch int64) error
	// Iterates through all persistent support state for other stores. Used at startup.
	readCurrentSupport(func(store dme_livenesspb.StoreIdentifier, support dme_livenesspb.Support)) error
	// updateSupport updates the support state this store gives to another store.
	updateSupport(support dme_livenesspb.SupportWithStoreID) error
}

type Manager interface {
	AddRemoteStore(store dme_livenesspb.StoreIdentifier)
	RemoveRemoteStore(store dme_livenesspb.StoreIdentifier)
	HandleHeartbeat(header dme_livenesspb.Header, msg dme_livenesspb.Heartbeat) error
	HandleSupportState(header dme_livenesspb.Header, msg dme_livenesspb.SupportState) error
	HandleWithdrawSupport(header dme_livenesspb.Header, msg dme_livenesspb.WithdrawSupport) error
	RangeLeaseHelper
}

// TODO:
// - integrate with kvserver to ensure can be integrated.
// - finish Manager implementation.
// - unit test
// - integration testing.
