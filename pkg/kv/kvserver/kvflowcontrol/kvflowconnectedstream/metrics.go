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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

// Aliases to make the code below slightly easier to read.
const regular, elastic = admissionpb.RegularWorkClass, admissionpb.ElasticWorkClass

var (
	flowTokensAvailable = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_tokens_available",
		Help:        "Flow %s tokens available for %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	flowTokensDeducted = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_tokens_deducted",
		Help:        "Flow %s tokens deducted by %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	flowTokensReturned = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_tokens_returned",
		Help:        "Flow %s tokens returned by %s requests, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	flowTokensUnaccounted = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_tokens_unaccounted",
		Help:        "Flow %s tokens returned by %s requests that were unaccounted for, across all replication streams",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	requestsWaiting = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_requests_waiting",
		Help:        "Number of %s %s requests waiting for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	requestsAdmitted = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_requests_admitted",
		Help:        "Number of %s %s requests admitted by the flow controller",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	requestsErrored = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_requests_errored",
		Help:        "Number of %s %s requests that errored out while waiting for flow tokens",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	requestsBypassed = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_requests_bypassed",
		Help:        "Number of %s %s waiting requests that bypassed the flow controller due to disconnecting streams",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}

	waitDuration = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_wait_duration",
		Help:        "Latency histogram for time %s %s requests spent waiting for flow tokens",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	totalStreamCount = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_stream_count",
		Help:        "Total number of %s replication streams for %s requests",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}

	blockedStreamCount = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.%s.%s_blocked_stream_count",
		Help:        "Number of %s replication streams with no flow tokens available for %s requests",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
	}

	sendQueueSize = metric.Metadata{
		Name:        "kvadmission.flow_controller_v2.send_queue_size",
		Help:        "Total size of all send queue items",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
)

// annotateMetricTemplateWithWorkClass uses the given metric template to build
// one suitable for the specific work class.
func annotateMetricTemplateWithWorkClassAndType(
	wc admissionpb.WorkClass, tmpl metric.Metadata, t flowControlMetricType,
) metric.Metadata {
	rv := tmpl
	rv.Name = fmt.Sprintf(tmpl.Name, t, wc)
	rv.Help = fmt.Sprintf(tmpl.Help, t, wc)
	return rv
}

type flowControlMetricType int

const (
	flowControlEvalMetricType flowControlMetricType = iota
	flowControlSendMetricType
)

func (f flowControlMetricType) String() string {
	switch f {
	case flowControlEvalMetricType:
		return "eval"
	case flowControlSendMetricType:
		return "send"
	default:
		panic("")
	}
}

type FlowControlMetrics struct {
	SendQueueSize          *metric.Gauge
	EvalFlowControlMetrics *flowControlMetrics
	SendFlowControlMetrics *flowControlMetrics
}

var _ metric.Struct = &FlowControlMetrics{}

type flowControlMetrics struct {
	ElasticFlowTokensDeducted    *metric.Counter
	ElasticFlowTokensReturned    *metric.Counter
	ElasticFlowTokensUnaccounted *metric.Counter
	RegularFlowTokensDeducted    *metric.Counter
	RegularFlowTokensReturned    *metric.Counter
	RegularFlowTokensUnaccounted *metric.Counter
	FlowTokensAvailable          [admissionpb.NumWorkClasses]*metric.Gauge
	RequestsWaiting              [admissionpb.NumWorkClasses]*metric.Gauge
	RequestsAdmitted             [admissionpb.NumWorkClasses]*metric.Counter
	RequestsErrored              [admissionpb.NumWorkClasses]*metric.Counter
	RequestsBypassed             [admissionpb.NumWorkClasses]*metric.Counter
	WaitDuration                 [admissionpb.NumWorkClasses]metric.IHistogram
	TotalStreamCount             [admissionpb.NumWorkClasses]*metric.Gauge
	BlockedStreamCount           [admissionpb.NumWorkClasses]*metric.Gauge
}

var _ metric.Struct = &flowControlMetrics{}

type SendQueuesSizeCounter interface {
	SendQueuesSize() kvflowcontrol.Tokens
}

type sendQueuesSizeCounterForTesting struct{}

func (s sendQueuesSizeCounterForTesting) SendQueuesSize() kvflowcontrol.Tokens {
	return 0
}

func NewMetrics() *FlowControlMetrics {
	return &FlowControlMetrics{}
}

func (m *FlowControlMetrics) Init(
	ssTokenCounter StoreStreamsTokenCounter,
	sendQueuesSizeCounter SendQueuesSizeCounter,
	clock *hlc.Clock,
) {
	m.EvalFlowControlMetrics = newTypeMetrics(ssTokenCounter, clock, flowControlEvalMetricType)
	m.SendFlowControlMetrics = newTypeMetrics(ssTokenCounter, clock, flowControlSendMetricType)
	m.SendQueueSize = metric.NewFunctionalGauge(
		sendQueueSize, func() int64 {
			return int64(sendQueuesSizeCounter.SendQueuesSize())
		},
	)
}

// MetricStruct implements the metric.Struct interface.
func (m *FlowControlMetrics) MetricStruct() {}

func newTypeMetrics(
	ssTokenCounter StoreStreamsTokenCounter, clock *hlc.Clock, t flowControlMetricType,
) *flowControlMetrics {
	m := &flowControlMetrics{}
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		wc := wc // copy loop variable
		m.FlowTokensAvailable[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, flowTokensAvailable, t),
			func() int64 {
				sum := int64(0)
				var tokenCounters map[kvflowcontrol.Stream]TokenCounter
				if t == flowControlSendMetricType {
					tokenCounters = ssTokenCounter.GetSendTokenCounters()
				} else {
					tokenCounters = ssTokenCounter.GetEvalTokenCounters()
				}
				for _, tc := range tokenCounters {
					sum += int64(tc.Tokens(wc))
				}
				return sum
			},
		)
		if wc == regular {
			m.RegularFlowTokensDeducted = metric.NewCounter(
				annotateMetricTemplateWithWorkClassAndType(wc, flowTokensDeducted, t),
			)
			m.RegularFlowTokensReturned = metric.NewCounter(
				annotateMetricTemplateWithWorkClassAndType(wc, flowTokensReturned, t),
			)
			m.RegularFlowTokensUnaccounted = metric.NewCounter(
				annotateMetricTemplateWithWorkClassAndType(wc, flowTokensUnaccounted, t),
			)
		} else {
			m.ElasticFlowTokensDeducted = metric.NewCounter(
				annotateMetricTemplateWithWorkClassAndType(wc, flowTokensDeducted, t),
			)
			m.ElasticFlowTokensReturned = metric.NewCounter(
				annotateMetricTemplateWithWorkClassAndType(wc, flowTokensReturned, t),
			)
			m.ElasticFlowTokensUnaccounted = metric.NewCounter(
				annotateMetricTemplateWithWorkClassAndType(wc, flowTokensUnaccounted, t),
			)
		}
		m.RequestsWaiting[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, requestsWaiting, t),
		)
		m.RequestsAdmitted[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, requestsAdmitted, t),
		)
		m.RequestsBypassed[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, requestsBypassed, t),
		)
		m.RequestsErrored[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClassAndType(wc, requestsErrored, t),
		)
		m.WaitDuration[wc] = metric.NewHistogram(
			metric.HistogramOptions{
				Metadata:     annotateMetricTemplateWithWorkClassAndType(wc, waitDuration, t),
				Duration:     base.DefaultHistogramWindowInterval(),
				BucketConfig: metric.IOLatencyBuckets,
				Mode:         metric.HistogramModePrometheus,
			},
		)
		m.TotalStreamCount[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, totalStreamCount, t),
			func() int64 {
				if t == flowControlSendMetricType {
					return int64(len(ssTokenCounter.GetSendTokenCounters()))
				} else {
					return int64(len(ssTokenCounter.GetEvalTokenCounters()))
				}
			},
		)

		// blockedStreamLogger controls periodic logging of blocked streams in
		// WorkClass wc.
		var blockedStreamLogger = log.Every(30 * time.Second)
		var buf strings.Builder
		m.BlockedStreamCount[wc] = metric.NewFunctionalGauge(
			annotateMetricTemplateWithWorkClassAndType(wc, blockedStreamCount, t),
			func() int64 {
				shouldLogBlocked := blockedStreamLogger.ShouldLog()
				// count is the metric value.
				count := int64(0)

				streamStatsCount := 0
				// TODO(sumeer): this cap is not ideal. Consider dynamically reducing
				// the logging frequency to maintain a mean of 400 log entries/10min.
				const streamStatsCountCap = 20

				var tokenCounters map[kvflowcontrol.Stream]TokenCounter
				if t == flowControlSendMetricType {
					tokenCounters = ssTokenCounter.GetSendTokenCounters()
				} else {
					tokenCounters = ssTokenCounter.GetEvalTokenCounters()
				}

				for stream, b := range tokenCounters {
					if b.Tokens(wc) <= 0 {
						count++

						if shouldLogBlocked {
							// TODO(sumeer): this cap is not ideal.
							const blockedStreamCountCap = 100
							if count == 1 {
								buf.Reset()
								buf.WriteString(stream.String())
							} else if count <= blockedStreamCountCap {
								buf.WriteString(", ")
								buf.WriteString(stream.String())
							} else if count == blockedStreamCountCap+1 {
								buf.WriteString(" omitted some due to overflow")
							}
						}
					}
					// Log stats, which reflect both elastic and regular, when handling
					// the elastic metric. The choice of wc == elastic is arbitrary.
					// Every 30s this predicate will evaluate to true, and we will log
					// all the streams (elastic and regular) that experienced some
					// blocking since the last time such logging was done. If a
					// high-enough log verbosity is specified, shouldLogBacked will
					// always be true, but since this method executes at the frequency
					// of scraping the metric, we will still log at a reasonable rate.
					if shouldLogBlocked {
						// Get and reset stats regardless of whether we will log this
						// stream or not. We want stats to reflect only the last metric
						// interval.
						regularStats, elasticStats := b.GetAndResetStats(clock.PhysicalTime())
						logStream := false
						if regularStats.NoTokenDuration > 0 || elasticStats.NoTokenDuration > 0 {
							logStream = true
							streamStatsCount++
						}
						if logStream {
							if streamStatsCount <= streamStatsCountCap {
								var b strings.Builder
								fmt.Fprintf(&b, "stream %s %s was blocked: durations:", t, stream.String())
								if regularStats.NoTokenDuration > 0 {
									fmt.Fprintf(&b, " regular %s", regularStats.NoTokenDuration.String())
								}
								if elasticStats.NoTokenDuration > 0 {
									fmt.Fprintf(&b, " elastic %s", elasticStats.NoTokenDuration.String())
								}
								regularDelta := regularStats.TokensReturned - regularStats.TokensDeducted
								elasticDelta := elasticStats.TokensReturned - elasticStats.TokensDeducted
								fmt.Fprintf(&b, " tokens delta: regular %s (%s - %s) elastic %s (%s - %s)",
									humanize.IBytes(uint64(regularDelta)),
									humanize.IBytes(uint64(regularStats.TokensReturned)),
									humanize.IBytes(uint64(regularStats.TokensDeducted)),
									humanize.IBytes(uint64(elasticDelta)),
									humanize.IBytes(uint64(elasticStats.TokensReturned)),
									humanize.IBytes(uint64(elasticStats.TokensDeducted)))
								log.Infof(context.Background(), "%s", redact.SafeString(b.String()))
							} else if streamStatsCount == streamStatsCountCap+1 {
								log.Infof(context.Background(), "skipped logging some streams that were blocked")
							}
						}
					}
				}
				if shouldLogBlocked && count > 0 {
					log.Warningf(context.Background(), "%d blocked %s %s replication stream(s): %s",
						count, t, wc, redact.SafeString(buf.String()))
				}
				return count
			},
		)
	}
	return m
}

func (m *flowControlMetrics) onWaiting(class admissionpb.WorkClass) {
	m.RequestsWaiting[class].Inc(1)
}

func (m *flowControlMetrics) onAdmitted(class admissionpb.WorkClass, dur time.Duration) {
	m.RequestsAdmitted[class].Inc(1)
	m.RequestsWaiting[class].Dec(1)
	m.WaitDuration[class].RecordValue(dur.Nanoseconds())
}

func (m *flowControlMetrics) onBypassed(class admissionpb.WorkClass, dur time.Duration) {
	m.RequestsBypassed[class].Inc(1)
	m.RequestsWaiting[class].Dec(1)
	m.WaitDuration[class].RecordValue(dur.Nanoseconds())
}

func (m *flowControlMetrics) onErrored(class admissionpb.WorkClass, dur time.Duration) {
	m.RequestsErrored[class].Inc(1)
	m.RequestsWaiting[class].Dec(1)
	m.WaitDuration[class].RecordValue(dur.Nanoseconds())
}

func (m *flowControlMetrics) onTokenAdjustment(adjustment tokensPerWorkClass) {
	if adjustment.regular < 0 {
		m.RegularFlowTokensDeducted.Inc(-int64(adjustment.regular))
	} else {
		m.RegularFlowTokensReturned.Inc(int64(adjustment.regular))
	}
	if adjustment.elastic < 0 {
		m.ElasticFlowTokensDeducted.Inc(-int64(adjustment.elastic))
	} else {
		m.ElasticFlowTokensReturned.Inc(int64(adjustment.elastic))
	}
}

func (m *flowControlMetrics) onUnaccounted(unaccounted tokensPerWorkClass) {
	m.RegularFlowTokensUnaccounted.Inc(int64(unaccounted.regular))
	m.ElasticFlowTokensUnaccounted.Inc(int64(unaccounted.elastic))
}

// MetricStruct implements the metric.Struct interface.
func (m *flowControlMetrics) MetricStruct() {}
