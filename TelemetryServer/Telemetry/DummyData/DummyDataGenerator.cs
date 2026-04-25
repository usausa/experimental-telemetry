namespace TelemetryServer.Telemetry.DummyData;

using System.Diagnostics.CodeAnalysis;
using TelemetryServer.Telemetry.Models;
using TelemetryServer.Telemetry.Storage;

public sealed class DummyDataGenerator
{
    private static readonly string[] Services = ["claude-code", "checkout-api", "payments-worker", "frontend-web"];
    private static readonly string[] Hosts = ["host-a", "host-b", "host-c"];
    private static readonly string[] MetricNames =
    [
        "http.server.request.count",
        "http.server.request.duration",
        "process.cpu.utilization",
        "process.memory.usage",
        "claude_code.session.count",
    ];
    private static readonly string[] LogBodies =
    [
        "user_prompt_submitted",
        "session_started",
        "request completed",
        "cache miss for key",
        "throttled outbound request",
        "task scheduled",
    ];
    private static readonly string[] RootSpanNames =
    [
        "GET /api/v1/items",
        "POST /api/v1/orders",
        "GET /api/v1/users/{id}",
        "POST /api/v1/checkout",
    ];
    private static readonly string[] ChildSpanNames =
    [
        "db.query.users",
        "db.query.orders",
        "cache.get.session",
        "http.client.payments",
        "kafka.publish.order_created",
        "render.dashboard",
    ];
    private static readonly string[] SeverityTexts = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];
    private static readonly int[] SeverityNumbers = [1, 5, 9, 13, 17];

    private readonly ITelemetryStore store;

    public DummyDataGenerator(ITelemetryStore store)
    {
        this.store = store;
    }

    public int InjectMetrics(int count)
    {
        var now = DateTimeOffset.UtcNow;
        var rng = Random.Shared;
        var points = new List<MetricPoint>(count);
        for (var i = 0; i < count; i++)
        {
            var serviceName = Services[rng.Next(Services.Length)];
            var resource = BuildResource(rng, serviceName);
            var name = MetricNames[rng.Next(MetricNames.Length)];
            points.Add(new MetricPoint(
                now,
                resource,
                "TelemetryServer.DummyData",
                name,
                $"Dummy metric {name}",
                UnitFor(name),
                KindFor(name),
                now.AddSeconds(-rng.Next(0, 60)),
                ValueFor(name, rng),
                [
                    new KeyValueAttr("env", rng.Next(0, 2) == 0 ? "dev" : "prod"),
                    new KeyValueAttr("region", rng.Next(0, 2) == 0 ? "ap-northeast-1" : "us-east-1"),
                ]));
        }
        store.AddMetrics(points);
        return points.Count;
    }

    public int InjectLogs(int count)
    {
        var now = DateTimeOffset.UtcNow;
        var rng = Random.Shared;
        var entries = new List<LogEntry>(count);
        for (var i = 0; i < count; i++)
        {
            var resource = BuildResource(rng, Services[rng.Next(Services.Length)]);
            var sevIdx = rng.Next(SeverityNumbers.Length);
            entries.Add(new LogEntry(
                now,
                resource,
                "TelemetryServer.DummyData",
                now.AddSeconds(-rng.Next(0, 120)),
                SeverityTexts[sevIdx],
                SeverityNumbers[sevIdx],
                LogBodies[rng.Next(LogBodies.Length)] + " #" + rng.Next(1000, 9999),
                ToHexLower(RandomBytes(16)),
                ToHexLower(RandomBytes(8)),
                [
                    new KeyValueAttr("user.id", "u-" + rng.Next(1, 9999)),
                    new KeyValueAttr("request.id", Guid.NewGuid().ToString("N")[..16]),
                ]));
        }
        store.AddLogs(entries);
        return entries.Count;
    }

    public int InjectSpans(int count)
    {
        var now = DateTimeOffset.UtcNow;
        var rng = Random.Shared;
        var spans = new List<SpanEntry>(count * 4);
        for (var i = 0; i < count; i++)
        {
            spans.AddRange(BuildTrace(now, rng));
        }
        store.AddSpans(spans);
        return spans.Count;
    }

    public DummyInjectionResult InjectAll(int count) =>
        new(InjectMetrics(count), InjectLogs(count), InjectSpans(count));

    private static IEnumerable<SpanEntry> BuildTrace(DateTimeOffset now, Random rng)
    {
        var traceId = ToHexLower(RandomBytes(16));
        var rootSpanId = ToHexLower(RandomBytes(8));
        var rootService = Services[rng.Next(Services.Length)];
        var rootResource = BuildResource(rng, rootService);
        var rootStart = now.AddMilliseconds(-rng.Next(50, 5000));
        var rootDuration = TimeSpan.FromMilliseconds(rng.Next(60, 1500));
        var rootEnd = rootStart + rootDuration;
        var rootError = rng.Next(0, 10) >= 8;
        var rootStatus = rootError ? "ERROR" : "OK";
        var rootName = RootSpanNames[rng.Next(RootSpanNames.Length)];

        yield return new SpanEntry(
            now,
            rootResource,
            "TelemetryServer.DummyData",
            traceId,
            rootSpanId,
            null,
            rootName,
            "SPAN_KIND_SERVER",
            rootStart,
            rootEnd,
            rootStatus,
            rootError ? "Sample error" : null,
            [
                new KeyValueAttr("http.method", rng.Next(0, 2) == 0 ? "GET" : "POST"),
                new KeyValueAttr("http.route", "/api/v1/items"),
                new KeyValueAttr("http.status_code", rootError ? "500" : "200"),
            ]);

        var childCount = rng.Next(1, 5);
        for (var j = 0; j < childCount; j++)
        {
            var childService = rng.Next(0, 3) == 0
                ? Services[rng.Next(Services.Length)]
                : rootService;
            var childResource = BuildResource(rng, childService);
            var maxOffset = (int)Math.Max(2, rootDuration.TotalMilliseconds - 4);
            var offset = rng.Next(1, maxOffset);
            var childStart = rootStart.AddMilliseconds(offset);
            var maxDur = (int)Math.Max(1, (rootEnd - childStart).TotalMilliseconds - 1);
            var childDuration = TimeSpan.FromMilliseconds(rng.Next(1, maxDur + 1));
            var childError = rootError && j == childCount - 1;
            yield return new SpanEntry(
                now,
                childResource,
                "TelemetryServer.DummyData",
                traceId,
                ToHexLower(RandomBytes(8)),
                rootSpanId,
                ChildSpanNames[rng.Next(ChildSpanNames.Length)],
                "SPAN_KIND_CLIENT",
                childStart,
                childStart + childDuration,
                childError ? "ERROR" : "OK",
                childError ? "Sample error" : null,
                [
                    new KeyValueAttr("peer.service", childService),
                ]);
        }
    }

    private static MetricKind KindFor(string name)
    {
        if (name.EndsWith(".count", StringComparison.Ordinal))
        {
            return MetricKind.Sum;
        }
        if (name.EndsWith(".duration", StringComparison.Ordinal))
        {
            return MetricKind.Histogram;
        }
        return MetricKind.Gauge;
    }

    private static string UnitFor(string name)
    {
        if (name.EndsWith(".duration", StringComparison.Ordinal))
        {
            return "ms";
        }
        if (name.Contains("memory", StringComparison.OrdinalIgnoreCase))
        {
            return "By";
        }
        if (name.Contains("utilization", StringComparison.OrdinalIgnoreCase))
        {
            return "%";
        }
        return "1";
    }

    private static double ValueFor(string name, Random rng)
    {
        if (name.EndsWith(".duration", StringComparison.Ordinal))
        {
            return Math.Round(rng.NextDouble() * 500, 2);
        }
        if (name.Contains("utilization", StringComparison.OrdinalIgnoreCase))
        {
            return Math.Round(rng.NextDouble() * 100, 2);
        }
        if (name.Contains("memory", StringComparison.OrdinalIgnoreCase))
        {
            return rng.Next(50_000_000, 500_000_000);
        }
        return rng.Next(1, 100);
    }

    private static ResourceInfo BuildResource(Random rng, string serviceName) =>
        new(
        [
            new KeyValueAttr("service.name", serviceName),
            new KeyValueAttr("host.name", Hosts[rng.Next(Hosts.Length)]),
            new KeyValueAttr("service.version", "1.0." + rng.Next(0, 50)),
        ]);

    private static byte[] RandomBytes(int length)
    {
        var buf = new byte[length];
        Random.Shared.NextBytes(buf);
        return buf;
    }

    [SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase", Justification = "OTEL specification mandates lowercase hex for trace and span identifiers.")]
    private static string ToHexLower(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();
}

public sealed record DummyInjectionResult(int Metrics, int Logs, int Spans);
