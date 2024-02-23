# Generated by genbzl

PROTOBUF_SRCS = [
    "//pkg/acceptance/cluster:cluster_go_proto",
    "//pkg/blobs/blobspb:blobspb_go_proto",
    "//pkg/build/bazel/bes/command_line:command_line_go_proto",
    "//pkg/build/bazel/bes/failure_details:failure_details_go_proto",
    "//pkg/build/bazel/bes/invocation_policy:blaze_invocation_policy_go_proto",
    "//pkg/build/bazel/bes/option_filters:options_go_proto",
    "//pkg/build/bazel/bes:build_event_stream_go_proto",
    "//pkg/build:build_go_proto",
    "//pkg/ccl/backupccl/backuppb:backuppb_go_proto",
    "//pkg/ccl/baseccl:baseccl_go_proto",
    "//pkg/ccl/changefeedccl/changefeedpb:changefeedpb_go_proto",
    "//pkg/ccl/sqlproxyccl/tenant:tenant_go_proto",
    "//pkg/ccl/storageccl/engineccl/enginepbccl:enginepbccl_go_proto",
    "//pkg/ccl/utilccl/licenseccl:licenseccl_go_proto",
    "//pkg/cloud/cloudpb:cloudpb_go_proto",
    "//pkg/cloud/externalconn/connectionpb:connectionpb_go_proto",
    "//pkg/clusterversion:clusterversion_go_proto",
    "//pkg/cmd/roachprod/upgrade:upgrade_go_proto",
    "//pkg/config/zonepb:zonepb_go_proto",
    "//pkg/config:config_go_proto",
    "//pkg/geo/geopb:geopb_go_proto",
    "//pkg/gossip:gossip_go_proto",
    "//pkg/inspectz/inspectzpb:inspectzpb_go_proto",
    "//pkg/jobs/jobspb:jobspb_go_proto",
    "//pkg/keyvisualizer/keyvispb:keyvispb_go_proto",
    "//pkg/kv/bulk/bulkpb:bulkpb_go_proto",
    "//pkg/kv/kvnemesis:kvnemesis_go_proto",
    "//pkg/kv/kvpb:kvpb_go_proto",
    "//pkg/kv/kvserver/closedts/ctpb:ctpb_go_proto",
    "//pkg/kv/kvserver/concurrency/isolation:isolation_go_proto",
    "//pkg/kv/kvserver/concurrency/lock:lock_go_proto",
    "//pkg/kv/kvserver/concurrency/poison:poison_go_proto",
    "//pkg/kv/kvserver/dme_liveness/dme_livenesspb:dme_livenesspb_go_proto",
    "//pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb:kvflowcontrolpb_go_proto",
    "//pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb:kvflowinspectpb_go_proto",
    "//pkg/kv/kvserver/kvserverpb:kvserverpb_go_proto",
    "//pkg/kv/kvserver/liveness/livenesspb:livenesspb_go_proto",
    "//pkg/kv/kvserver/loqrecovery/loqrecoverypb:loqrecoverypb_go_proto",
    "//pkg/kv/kvserver/protectedts/ptpb:ptpb_go_proto",
    "//pkg/kv/kvserver/protectedts/ptstorage:ptstorage_go_proto",
    "//pkg/kv/kvserver/rangelog/internal/rangelogtestpb:rangelogtestpb_go_proto",
    "//pkg/kv/kvserver/readsummary/rspb:rspb_go_proto",
    "//pkg/kv/kvserver:kvserver_go_proto",
    "//pkg/multitenant/mtinfopb:mtinfopb_go_proto",
    "//pkg/multitenant/tenantcapabilities/tenantcapabilitiespb:tenantcapabilitiespb_go_proto",
    "//pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1:v1_go_proto",
    "//pkg/obsservice/obspb/opentelemetry-proto/common/v1:v1_go_proto",
    "//pkg/obsservice/obspb/opentelemetry-proto/logs/v1:v1_go_proto",
    "//pkg/obsservice/obspb/opentelemetry-proto/resource/v1:v1_go_proto",
    "//pkg/obsservice/obspb:obspb_go_proto",
    "//pkg/repstream/streampb:streampb_go_proto",
    "//pkg/roachpb:roachpb_go_proto",
    "//pkg/rpc/rpcpb:rpcpb_go_proto",
    "//pkg/rpc:rpc_go_proto",
    "//pkg/server/autoconfig/autoconfigpb:autoconfigpb_go_proto",
    "//pkg/server/diagnostics/diagnosticspb:diagnosticspb_go_proto",
    "//pkg/server/serverpb:serverpb_go_proto",
    "//pkg/server/status/statuspb:statuspb_go_proto",
    "//pkg/settings:settings_go_proto",
    "//pkg/sql/appstatspb:appstatspb_go_proto",
    "//pkg/sql/catalog/catenumpb:catenumpb_go_proto",
    "//pkg/sql/catalog/catpb:catpb_go_proto",
    "//pkg/sql/catalog/descpb:descpb_go_proto",
    "//pkg/sql/catalog/fetchpb:fetchpb_go_proto",
    "//pkg/sql/catalog/schematelemetry/schematelemetrycontroller:schematelemetrycontroller_go_proto",
    "//pkg/sql/contentionpb:contentionpb_go_proto",
    "//pkg/sql/execinfrapb:execinfrapb_go_proto",
    "//pkg/sql/inverted:inverted_go_proto",
    "//pkg/sql/lex:lex_go_proto",
    "//pkg/sql/pgwire/pgerror:pgerror_go_proto",
    "//pkg/sql/protoreflect/test:protoreflecttest_go_proto",
    "//pkg/sql/rowenc/rowencpb:rowencpb_go_proto",
    "//pkg/sql/schemachanger/scpb:scpb_go_proto",
    "//pkg/sql/sem/semenumpb:semenumpb_go_proto",
    "//pkg/sql/sessiondatapb:sessiondatapb_go_proto",
    "//pkg/sql/sqlstats/insights:insights_go_proto",
    "//pkg/sql/sqlstats/persistedsqlstats:persistedsqlstats_go_proto",
    "//pkg/sql/stats:stats_go_proto",
    "//pkg/sql/types:types_go_proto",
    "//pkg/storage/enginepb:enginepb_go_proto",
    "//pkg/testutils/grpcutils:grpcutils_go_proto",
    "//pkg/ts/catalog:catalog_go_proto",
    "//pkg/ts/tspb:tspb_go_proto",
    "//pkg/util/admission/admissionpb:admissionpb_go_proto",
    "//pkg/util/duration:duration_go_proto",
    "//pkg/util/hlc:hlc_go_proto",
    "//pkg/util/log/eventpb:eventpb_go_proto",
    "//pkg/util/log/logpb:logpb_go_proto",
    "//pkg/util/metric:metric_go_proto",
    "//pkg/util/optional:optional_go_proto",
    "//pkg/util/protoutil:protoutil_go_proto",
    "//pkg/util/timeutil/pgdate:pgdate_go_proto",
    "//pkg/util/tracing/tracingpb:tracingpb_go_proto",
    "//pkg/util/tracing/tracingservicepb:tracingservicepb_go_proto",
    "//pkg/util:util_go_proto",
]
