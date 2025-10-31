// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import "github.com/cockroachdb/pebble"

// TODO(irfansharif): This comment is a bit stale with replication admission
// control where admission is asynchronous. AC is informed of the write when
// it's being physically done, so we know its size then. We don't need upfront
// estimates anymore. The AdmittedWorkDone interface and surrounding types
// (StoreWorkDoneInfo for ex.) are no longer central.
//
// The logic in this file deals with token estimation for a store write in two
// situations: (a) at admission time, (b) when the admitted work is done. At
// (a) we have no information provided about the work size (NB: this choice is
// debatable, since for ingests we could extract some information pre-request
// evaluation). At (b) we have the information in StoreWorkDoneInfo, which
// gets aggregated by the StoreWorkQueue into storeAdmissionStats.
//
// Both kinds of token estimation are guessing the tokens that a request
// should rightfully consume, based on models that are "trained" on actual
// resource consumption observed, and reported work sizes, in the past.
//
// We use models in which the "actual tokens" are computed as a linear
// function of the bytes claimed in StoreWorkDoneInfo (which we call
// accounted_bytes), i.e. actual_tokens = a*accounted_bytes + b, and the result
// of this computation (which can only be done after completion of the work)
// is used to acquire (without blocking) additional tokens. The model thus
// tries to make sure that one token reflects one byte of work. For example,
// if the model is initially (a=1, b=0) and each incoming request acquires
// 1000 tokens but ends up writing 2000 bytes, the model should update to
// roughly (a=2, b=0), and future requests will, upon completion, acquire an
// additional 1000 tokens to even the score. The actual fitting is performed
// on aggregates over recent requests, and the more work is done "outside" of
// admission control, the less useful the multiplier becomes; the model will
// degrade into one with a larger constant term and is expected to perform
// poorly.
//
// We now justify the use of a linear model. A model with only a constant term
// (the b term above) is not able to handle multiple simultaneous workloads
// executing on a node, since they can have very different work sizes. We
// desire workload agnostic token estimation so a mix of various workloads can
// share the same token estimation model. A model with
// actual_tokens=accounted_bytes is also not suitable for 2 reasons:
// - For writes (that are accomplished via the memtable) we only have the size
//   of the raft log entry in accounted_bytes and not the size of the later
//   state machine application.
//
// - For ingests, we also need to fit a model where accounted_bytes is the
//   size of the ingest, and actual_tokens is the size that landed in L0.
//
// We note that a multiplier term (the a term above) can accomplish both goals.
// The first bullet can be handled by a multiplier that is approximately 2.
// Ingests have some fraction that get ingested into L0, i.e., a multiplier
// <= 1.
//
// One complication with ingests is range snapshot application. They happen
// infrequently and can add a very large number of bytes, which are often
// ingested below L0. We don't want to skew our ingest models based on these
// range snapshots, so we explicitly ignore them in modeling.
//
// So now that we've justified the a term, one question arises is whether we
// need a b term. Historically we have had sources of error that are due to
// lack of integration with admission control, and we do not want to skew the
// a term significantly. So the fitting approach has a b term, but attempts to
// minimize the b term while keeping the a term within some configured bounds.
// The [min,max] bounds on the a term prevent wild fluctuations and are set
// based on what we know about the system.
//
// The estimation of a and b is done by tokensLinearModelFitter. It is used
// to fit the following models.
//
// TODO: update comments.
// - [writeToWALLM]
// - [walToL0LM]
// - [l0WriteLM] Mapping the write accounted bytes to bytes added to L0: We
//   expect the multiplier a to be close to 2, due to the subsequent
//   application to the state machine. So it would be reasonable to constrain
//   a to [1, 2]. However, in experiments we've seen inconsistencies between
//   Pebble stats and admission control stats, due to choppiness in work
//   getting done, which is better modeled by allowing multiplier a to be less
//   constrained. So we use [0.5, 3].
//
// - [l0IngestLM] Mapping the ingest accounted bytes (which is the total bytes
//   in the ingest, and not just to L0), to the bytes added to L0: We expect
//   the multiplier a to be <= 1, since some fraction of the ingest goes into
//   L0. So it would be reasonable to constrain a to [0, 1]. For the same
//   reason as the previous bullet, we use [0.001, 1.5]. This lower-bound of
//   0.001 is debatable, since it will cause some token consumption even if
//   all ingested bytes are going to levels below L0.
//   TODO(sumeer): consider lowering the lower bound, after experimentation.
//
// - [ingestLM] Mapping the ingest accounted bytes to the total ingested bytes
//   added to the LSM. We can expect a multiplier of 1. For now, we use bounds
//   of [0.5, 1.5].
//
// - [WriteAmpLM] Estimates the "write amplification", that for the purposes
//   of admission control is the ratio of disk writes to the bytes initially
//   written to sstables (flushed to L0 + ingested) due to writes incoming to
//   the LSM. We use this model to deduct from disk write tokens from
//   disk_bandwidth.go.
//
// NB: these linear models will be workload agnostic if most of the bytes are
// modeled via the a.x term, and not via the b term, since workloads are
// likely (at least for regular writes) to vary significantly in x.

// See the comment above for the justification of these constants.
const writeToWALMultiplierMin = 0.5
const writeToWALMultiplierMax = 3.0
const walToL0MultiplierMin = 0.01
const walToL0MultiplierMax = 1.2
const l0IngestMultiplierMin = 0.001
const l0IngestMultiplierMax = 1.5
const ingestMultiplierMin = 0.5
const ingestMultiplierMax = 1.5
const writeAmpMultiplierMin = 1.0
const writeAmpMultiplierMax = 100.0

type storePerWorkTokenEstimator struct {
	atAdmissionWorkTokens int64

	// NB: The linear model fitters below are used to determine how many tokens
	// to consume once the size of the work is known.

	atDoneWriteToWALTokensLinearModel tokensLinearModelFitter
	atDoneWALToL0TokensLinearModel    tokensLinearModelFitter
	atDoneL0IngestTokensLinearModel   tokensLinearModelFitter
	// Unlike the models above that model bytes into L0, this model computes all
	// ingested bytes into the LSM.
	atDoneIngestTokensLinearModel tokensLinearModelFitter
	// This model is used to estimate the write amplification due to asynchronous
	// compactions after bytes are written to L0. It models the relationship
	// between ingests (not including range snapshots) plus incoming L0 bytes and
	// total disk write throughput in a given interval. We ignore range snapshots
	// here, since they land into lower levels (usually L6) of the LSM.
	atDoneWriteAmpLinearModel tokensLinearModelFitter

	cumStoreAdmissionStats  storeAdmissionStats
	cumWALWriteBytes        uint64
	cumWALFlushedWriteBytes uint64
	cumL0WriteBytes         uint64
	cumL0IngestedBytes      uint64
	cumLSMIngestedBytes     uint64
	cumDiskWrites           uint64

	// Tracked for logging and copied out of here.
	aux perWorkTokensAux
}

// perWorkTokensAux encapsulates auxiliary (informative) numerical state that
// helps in understanding the behavior of storePerWorkTokenEstimator.
type perWorkTokensAux struct {
	intWorkCount              int64
	intWALWriteBytes          int64
	intWALFlushedWriteBytes   int64
	intL0WriteBytes           int64
	intL0IngestedBytes        int64
	intLSMIngestedBytes       int64
	intL0WriteAccountedBytes  int64
	intIngestedAccountedBytes int64
	intWriteToWALLinearModel  tokensLinearModel
	intWALToL0LinearModel     tokensLinearModel
	intL0IngestedLinearModel  tokensLinearModel
	intIngestedLinearModel    tokensLinearModel
	intWriteAmpLinearModel    tokensLinearModel

	// The bypassed count and bytes are also included in the overall interval
	// stats.
	intBypassedWorkCount              int64
	intL0WriteBypassedAccountedBytes  int64
	intIngestedBypassedAccountedBytes int64

	// These ignored bytes are included in {intWALWriteBytes, intL0WriteBytes},
	// and may even be higher than those values because these are from a
	// different source.
	intWALIgnoredWriteBytes int64
	intL0IgnoredWriteBytes  int64

	// These ignored bytes are included in intL0IngestedBytes, and in
	// intLSMIngestedBytes, and may even be higher than that value because these
	// are from a different source.
	intL0IgnoredIngestedBytes int64

	// These are used for write amplification estimation. intAdjustedLSMWrites
	// represent the accounted LSM writes (ingestion + regular writes).
	// intAdjustedDiskWriteBytes represent the total write bytes for the interval,
	// excluding ignored bytes.
	intAdjustedLSMWrites      int64
	intAdjustedDiskWriteBytes int64
}

func makeStorePerWorkTokenEstimator() storePerWorkTokenEstimator {
	return storePerWorkTokenEstimator{
		atAdmissionWorkTokens: 1,
		atDoneWriteToWALTokensLinearModel: makeTokensLinearModelFitter(
			writeToWALMultiplierMin, writeToWALMultiplierMax, false),
		atDoneWALToL0TokensLinearModel: makeTokensLinearModelFitter(
			walToL0MultiplierMin, walToL0MultiplierMax, false),
		atDoneL0IngestTokensLinearModel: makeTokensLinearModelFitter(
			l0IngestMultiplierMin, l0IngestMultiplierMax, true),
		atDoneIngestTokensLinearModel: makeTokensLinearModelFitter(
			ingestMultiplierMin, ingestMultiplierMax, false),
		atDoneWriteAmpLinearModel: makeTokensLinearModelFitter(
			writeAmpMultiplierMin, writeAmpMultiplierMax, false),
	}
}

// NB: first call to updateEstimates only initializes the cumulative values.
func (e *storePerWorkTokenEstimator) updateEstimates(
	l0Metrics pebble.LevelMetrics,
	cumWALBytesWritten uint64,
	cumLSMIngestedBytes uint64,
	cumDiskWrite uint64,
	admissionStats storeAdmissionStats,
	unflushedMemTableTooLarge bool,
) {
	if e.cumL0WriteBytes == 0 {
		e.cumStoreAdmissionStats = admissionStats
		e.cumWALWriteBytes = cumWALBytesWritten
		e.cumWALFlushedWriteBytes = l0Metrics.TableBytesIn
		e.cumL0WriteBytes = l0Metrics.TablesFlushed.Bytes + l0Metrics.BlobBytesFlushed
		e.cumL0IngestedBytes = l0Metrics.TablesIngested.Bytes + l0Metrics.BlobBytesFlushed
		e.cumLSMIngestedBytes = cumLSMIngestedBytes
		e.cumDiskWrites = cumDiskWrite
		return
	}
	// Interval stats related to bytes flushed or that will be flushed.
	intL0WriteBytes := int64(l0Metrics.TablesFlushed.Bytes+l0Metrics.BlobBytesFlushed) - int64(e.cumL0WriteBytes)
	intL0IgnoredWriteBytes := int64(admissionStats.statsToIgnore.writeBytes.BytesAsSSTable) -
		int64(e.cumStoreAdmissionStats.statsToIgnore.writeBytes.BytesAsSSTable)
	// The intL0IgnoredWriteBytes may not yet be flushed. If we are currently in the mode
	// of only counting WAL bytes, we need the batch size. That can be fixed.
	// Subtract the batch size from the WAL bytes.
	adjustedIntL0WriteBytes := intL0WriteBytes - intL0IgnoredWriteBytes
	if adjustedIntL0WriteBytes < 0 {
		adjustedIntL0WriteBytes = 0
	}
	intWALWriteBytes := int64(cumWALBytesWritten) - int64(e.cumWALWriteBytes)
	intWALIgnoredWriteBytes := int64(admissionStats.statsToIgnore.writeBytes.RawBytes) -
		int64(e.cumStoreAdmissionStats.statsToIgnore.writeBytes.RawBytes)
	adjustedIntWALWriteBytes := intWALWriteBytes - intWALIgnoredWriteBytes
	if adjustedIntWALWriteBytes < 0 {
		adjustedIntWALWriteBytes = 0
	}
	intWALFlushedWriteBytes := int64(l0Metrics.TableBytesIn) - int64(e.cumWALFlushedWriteBytes)
	if intWALFlushedWriteBytes < 0 {
		intWALFlushedWriteBytes = 0
	}

	// Interval stats related to ingests into L0.
	intL0IngestedBytes := int64(l0Metrics.TablesIngested.Bytes) - int64(e.cumL0IngestedBytes)
	intL0IgnoredIngestedBytes := int64(admissionStats.statsToIgnore.ingestStats.ApproxIngestedIntoL0Bytes) -
		int64(e.cumStoreAdmissionStats.statsToIgnore.ingestStats.ApproxIngestedIntoL0Bytes)
	adjustedIntL0IngestedBytes := intL0IngestedBytes - intL0IgnoredIngestedBytes
	if adjustedIntL0IngestedBytes < 0 {
		adjustedIntL0IngestedBytes = 0
	}

	// Work stats as reported to AC.
	intWorkCount := int64(admissionStats.workCount) -
		int64(e.cumStoreAdmissionStats.workCount)
	intL0WriteAccountedBytes :=
		int64(admissionStats.writeAccountedBytes) - int64(e.cumStoreAdmissionStats.writeAccountedBytes)
	// Note that these are not L0 ingested bytes, since we don't know how
	// many did go to L0.
	intIngestedAccountedBytes := int64(admissionStats.ingestedAccountedBytes) -
		int64(e.cumStoreAdmissionStats.ingestedAccountedBytes)

	// TODO(sumeer): now that we are modeling the transformation of
	// intL0WriteAccountedBytes into bytes in L0 in two steps, i.e., first into
	// WAL bytes and then WAL => flushed bytes, we don't need to use
	// unflushedMemTableTooLarge. It was introduced due to the uncertainty
	// introduced by WAL failover, since we didn't know how many of the written
	// bytes got flushed, and were naively assuming all were, except when this
	// boolean was true.
	if !unflushedMemTableTooLarge {
		// Fit models for bytes flushed or that will be flushed.
		e.atDoneWriteToWALTokensLinearModel.updateModelUsingIntervalStats(
			intL0WriteAccountedBytes, adjustedIntWALWriteBytes, intWorkCount)
		// If we knew how many of the intWALFlushedWriteBytes were unaccounted, we
		// could use that as the first parameter, and adjustedIntL0WriteBytes as
		// the second parameter. But there can be a lag (especially during WAL
		// failover) on when WAL bytes get flushed, and this lag can be many
		// adjustmentIntervals. So we don't exclude the unaccounted bytes. This
		// model is accounting for two factors (a) raft log truncation, (b)
		// compression. The unaccounted bytes don't experience (a) since they are
		// incoming range snapshots applied as normal batches, so if unaccounted
		// bytes are large, this model is estimate a higher coefficient.
		e.atDoneWALToL0TokensLinearModel.updateModelUsingIntervalStats(
			intWALFlushedWriteBytes, intL0WriteBytes, intWorkCount)
	}
	// Fit models for ingests into L0.
	e.atDoneL0IngestTokensLinearModel.updateModelUsingIntervalStats(
		intIngestedAccountedBytes, adjustedIntL0IngestedBytes, intWorkCount)

	// Fit the ingest across all levels model.
	intLSMIngestedBytes := int64(cumLSMIngestedBytes) - int64(e.cumLSMIngestedBytes)
	intIgnoredIngestedBytes :=
		int64(admissionStats.statsToIgnore.ingestStats.Bytes) -
			int64(e.cumStoreAdmissionStats.statsToIgnore.ingestStats.Bytes)
	adjustedIntLSMIngestedBytes := intLSMIngestedBytes - intIgnoredIngestedBytes
	if adjustedIntLSMIngestedBytes < 0 {
		adjustedIntLSMIngestedBytes = 0
	}
	e.atDoneIngestTokensLinearModel.updateModelUsingIntervalStats(
		intIngestedAccountedBytes, adjustedIntLSMIngestedBytes, intWorkCount)

	// Fit the write amplification model.
	intDiskWrite := int64(cumDiskWrite - e.cumDiskWrites)
	adjustedIntLSMWrites := adjustedIntL0WriteBytes + adjustedIntLSMIngestedBytes
	adjustedIntDiskWrites := intDiskWrite - intIgnoredIngestedBytes - intL0IgnoredWriteBytes
	if adjustedIntDiskWrites < 0 {
		adjustedIntDiskWrites = 0
	}
	e.atDoneWriteAmpLinearModel.updateModelUsingIntervalStats(adjustedIntLSMWrites, adjustedIntDiskWrites, intWorkCount)

	intL0TotalBytes := adjustedIntL0WriteBytes + adjustedIntL0IngestedBytes
	intAboveRaftWorkCount := int64(admissionStats.aboveRaftStats.workCount) -
		int64(e.cumStoreAdmissionStats.aboveRaftStats.workCount)
	intAboveRaftL0WriteAccountedBytes := int64(admissionStats.aboveRaftStats.writeAccountedBytes) -
		int64(e.cumStoreAdmissionStats.aboveRaftStats.writeAccountedBytes)
	intAboveRaftIngestedAccountedBytes := int64(admissionStats.aboveRaftStats.ingestedAccountedBytes) -
		int64(e.cumStoreAdmissionStats.aboveRaftStats.ingestedAccountedBytes)
	// TODO(sumeer): remove unflushedMemTableTooLarge for the reasons stated in
	// the other to do.
	if intAboveRaftWorkCount > 1 && intL0TotalBytes > 0 && !unflushedMemTableTooLarge {
		// We simply apply the linear models to the stats we have and then use the
		// modeled bytes to split across the work count.
		aboveRaftEstimatedBytes :=
			e.atDoneWALToL0TokensLinearModel.smoothedLinearModel.applyLinearModel(
				e.atDoneWriteToWALTokensLinearModel.smoothedLinearModel.applyLinearModel(
					intAboveRaftL0WriteAccountedBytes)) +
				e.atDoneL0IngestTokensLinearModel.smoothedLinearModel.applyLinearModel(
					intAboveRaftIngestedAccountedBytes)
		if aboveRaftEstimatedBytes > 0 {
			// Update the atAdmissionWorkTokens. NB: this is only used for requests
			// that don't use replication flow control.
			intAtAdmissionWorkTokens := aboveRaftEstimatedBytes / intAboveRaftWorkCount
			const alpha = 0.5
			e.atAdmissionWorkTokens = int64(alpha*float64(intAtAdmissionWorkTokens) +
				(1-alpha)*float64(e.atAdmissionWorkTokens))
			e.atAdmissionWorkTokens = max(1, e.atAdmissionWorkTokens)
		}
	}
	e.aux = perWorkTokensAux{
		intWorkCount:              intWorkCount,
		intWALWriteBytes:          intWALWriteBytes,
		intWALFlushedWriteBytes:   intWALFlushedWriteBytes,
		intL0WriteBytes:           intL0WriteBytes,
		intL0IngestedBytes:        intL0IngestedBytes,
		intLSMIngestedBytes:       intLSMIngestedBytes,
		intL0WriteAccountedBytes:  intL0WriteAccountedBytes,
		intIngestedAccountedBytes: intIngestedAccountedBytes,
		intWriteToWALLinearModel:  e.atDoneWriteToWALTokensLinearModel.intLinearModel,
		intWALToL0LinearModel:     e.atDoneWALToL0TokensLinearModel.intLinearModel,
		intL0IngestedLinearModel:  e.atDoneL0IngestTokensLinearModel.intLinearModel,
		intIngestedLinearModel:    e.atDoneIngestTokensLinearModel.intLinearModel,
		intWriteAmpLinearModel:    e.atDoneWriteAmpLinearModel.intLinearModel,
		intBypassedWorkCount: int64(admissionStats.aux.bypassedCount) -
			int64(e.cumStoreAdmissionStats.aux.bypassedCount),
		intL0WriteBypassedAccountedBytes: int64(admissionStats.aux.writeBypassedAccountedBytes) -
			int64(e.cumStoreAdmissionStats.aux.writeBypassedAccountedBytes),
		intIngestedBypassedAccountedBytes: int64(admissionStats.aux.ingestedBypassedAccountedBytes) -
			int64(e.cumStoreAdmissionStats.aux.ingestedBypassedAccountedBytes),
		intWALIgnoredWriteBytes:   intWALIgnoredWriteBytes,
		intL0IgnoredWriteBytes:    intL0IgnoredWriteBytes,
		intL0IgnoredIngestedBytes: intL0IgnoredIngestedBytes,
		intAdjustedDiskWriteBytes: adjustedIntDiskWrites,
		intAdjustedLSMWrites:      adjustedIntLSMWrites,
	}
	// Store the latest cumulative values.
	e.cumStoreAdmissionStats = admissionStats
	e.cumL0WriteBytes = l0Metrics.TablesFlushed.Bytes + l0Metrics.BlobBytesFlushed
	e.cumL0IngestedBytes = l0Metrics.TablesIngested.Bytes
	e.cumLSMIngestedBytes = cumLSMIngestedBytes
	e.cumDiskWrites = cumDiskWrite
}

func (e *storePerWorkTokenEstimator) getStoreRequestEstimatesAtAdmission() storeRequestEstimates {
	return storeRequestEstimates{writeTokens: e.atAdmissionWorkTokens}
}

func (e *storePerWorkTokenEstimator) getModelsAtDone() (
	writeToWALLM tokensLinearModel,
	walToL0LM tokensLinearModel,
	l0IngestLM tokensLinearModel,
	ingestLM tokensLinearModel,
	writeAmpLM tokensLinearModel,
) {
	return e.atDoneWriteToWALTokensLinearModel.smoothedLinearModel,
		e.atDoneWALToL0TokensLinearModel.smoothedLinearModel,
		e.atDoneL0IngestTokensLinearModel.smoothedLinearModel,
		e.atDoneIngestTokensLinearModel.smoothedLinearModel,
		e.atDoneWriteAmpLinearModel.smoothedLinearModel
}
