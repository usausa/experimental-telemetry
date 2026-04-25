using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Google.Protobuf;
using Grpc.Net.Client;
using OpenTelemetry.Proto.Collector.Logs.V1;
using OpenTelemetry.Proto.Collector.Metrics.V1;
using OpenTelemetry.Proto.Collector.Trace.V1;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;

AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var endpoint = args.Length > 0 ? args[0] : "http://localhost:4317";
var count = args.Length > 1 && int.TryParse(args[1], CultureInfo.InvariantCulture, out var n) ? n : 3;

Console.WriteLine($"TelemetryClient -> {endpoint} (count={count})");

using var channel = GrpcChannel.ForAddress(endpoint);

var resource = new Resource
{
    Attributes =
    {
        new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "telemetry-test-client" } },
        new KeyValue { Key = "host.name", Value = new AnyValue { StringValue = Environment.MachineName } },
        new KeyValue { Key = "service.version", Value = new AnyValue { StringValue = "1.0.0" } },
    },
};

await SendMetricsAsync(channel, resource, count);
await SendLogsAsync(channel, resource, count);
await SendTracesAsync(channel, resource, count);

Console.WriteLine("Done.");

static async Task SendMetricsAsync(GrpcChannel channel, Resource resource, int count)
{
    var client = new MetricsService.MetricsServiceClient(channel);
    var req = new ExportMetricsServiceRequest();
    var rm = new ResourceMetrics { Resource = resource };
    var sm = new ScopeMetrics { Scope = new InstrumentationScope { Name = "TelemetryClient.Metrics" } };

    var sumMetric = new Metric
    {
        Name = "telemetry_client.requests",
        Unit = "1",
        Sum = new Sum
        {
            IsMonotonic = true,
            AggregationTemporality = AggregationTemporality.Cumulative,
        },
    };
    for (var i = 0; i < count; i++)
    {
        sumMetric.Sum.DataPoints.Add(new NumberDataPoint
        {
            TimeUnixNano = NowUnixNano(),
            AsInt = Random.Shared.Next(1, 100),
            Attributes = { new KeyValue { Key = "endpoint", Value = new AnyValue { StringValue = "/api/v1/items" } } },
        });
    }
    sm.Metrics.Add(sumMetric);

    var gaugeMetric = new Metric
    {
        Name = "telemetry_client.cpu_usage",
        Unit = "%",
        Gauge = new Gauge(),
    };
    for (var i = 0; i < count; i++)
    {
        gaugeMetric.Gauge.DataPoints.Add(new NumberDataPoint
        {
            TimeUnixNano = NowUnixNano(),
            AsDouble = Math.Round(Random.Shared.NextDouble() * 100, 2),
            Attributes = { new KeyValue { Key = "core", Value = new AnyValue { IntValue = i } } },
        });
    }
    sm.Metrics.Add(gaugeMetric);

    rm.ScopeMetrics.Add(sm);
    req.ResourceMetrics.Add(rm);
    await client.ExportAsync(req);
    Console.WriteLine($"  metrics sent (sum points={count}, gauge points={count})");
}

static async Task SendLogsAsync(GrpcChannel channel, Resource resource, int count)
{
    var client = new LogsService.LogsServiceClient(channel);
    var req = new ExportLogsServiceRequest();
    var rl = new ResourceLogs { Resource = resource };
    var sl = new ScopeLogs { Scope = new InstrumentationScope { Name = "TelemetryClient.Logs" } };

    var severities = new[] { SeverityNumber.Debug, SeverityNumber.Info, SeverityNumber.Warn, SeverityNumber.Error };
    for (var i = 0; i < count; i++)
    {
        var sev = severities[i % severities.Length];
        sl.LogRecords.Add(new LogRecord
        {
            TimeUnixNano = NowUnixNano(),
            SeverityText = sev.ToString().ToUpperInvariant(),
            SeverityNumber = sev,
            Body = new AnyValue { StringValue = $"test log message #{i + 1} from telemetry client" },
            Attributes =
            {
                new KeyValue { Key = "iteration", Value = new AnyValue { IntValue = i } },
                new KeyValue { Key = "user.id", Value = new AnyValue { StringValue = $"u-{i:D4}" } },
            },
        });
    }
    rl.ScopeLogs.Add(sl);
    req.ResourceLogs.Add(rl);
    await client.ExportAsync(req);
    Console.WriteLine($"  logs sent (count={count})");
}

static async Task SendTracesAsync(GrpcChannel channel, Resource resource, int count)
{
    var client = new TraceService.TraceServiceClient(channel);
    var req = new ExportTraceServiceRequest();
    var rs = new ResourceSpans { Resource = resource };
    var ss = new ScopeSpans { Scope = new InstrumentationScope { Name = "TelemetryClient.Traces" } };

    for (var i = 0; i < count; i++)
    {
        var traceId = RandomBytes(16);
        var spanId = RandomBytes(8);
        var startNano = NowUnixNano();
        var endNano = startNano + (ulong)Random.Shared.Next(1_000_000, 500_000_000);
        var isError = Random.Shared.Next(0, 10) >= 8;

        ss.Spans.Add(new Span
        {
            TraceId = ByteString.CopyFrom(traceId),
            SpanId = ByteString.CopyFrom(spanId),
            Name = $"test-span-{i + 1}",
            Kind = Span.Types.SpanKind.Server,
            StartTimeUnixNano = startNano,
            EndTimeUnixNano = endNano,
            Status = new Status
            {
                Code = isError ? Status.Types.StatusCode.Error : Status.Types.StatusCode.Ok,
                Message = isError ? "sample error" : string.Empty,
            },
            Attributes =
            {
                new KeyValue { Key = "http.method", Value = new AnyValue { StringValue = "GET" } },
                new KeyValue { Key = "http.route", Value = new AnyValue { StringValue = "/api/v1/items" } },
                new KeyValue { Key = "http.status_code", Value = new AnyValue { IntValue = isError ? 500 : 200 } },
            },
        });
    }
    rs.ScopeSpans.Add(ss);
    req.ResourceSpans.Add(rs);
    await client.ExportAsync(req);
    Console.WriteLine($"  traces sent (count={count})");
}

static ulong NowUnixNano() =>
    (ulong)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L);

[SuppressMessage("Security", "CA5394:Do not use insecure randomness", Justification = "Test data generation.")]
static byte[] RandomBytes(int length)
{
    var buf = new byte[length];
    Random.Shared.NextBytes(buf);
    return buf;
}
