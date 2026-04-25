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
    private static readonly string[] SpanNames =
    [
        "GET /api/v1/items",
        "POST /api/v1/orders",
        "db.query.users",
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
            var resource = BuildResource(rng);
            var name = MetricNames[rng.Next(MetricNames.Length)];
            var kind = (MetricKind)rng.Next(0, 3);
            var value = name.Contains("duration", StringComparison.OrdinalIgnoreCase)
                ? Math.Round(rng.NextDouble() * 500, 2)
                : Math.Round(rng.NextDouble() * 100, 2);
            points.Add(new MetricPoint(
                now,
                resource,
                "TelemetryServer.DummyData",
                name,
                $"Dummy metric {name}",
                name.Contains("duration", StringComparison.OrdinalIgnoreCase) ? "ms" : "1",
                kind,
                now.AddSeconds(-rng.Next(0, 60)),
                value,
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
            var resource = BuildResource(rng);
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
        var spans = new List<SpanEntry>(count);
        for (var i = 0; i < count; i++)
        {
            var resource = BuildResource(rng);
            var start = now.AddMilliseconds(-rng.Next(50, 5000));
            var duration = TimeSpan.FromMilliseconds(rng.Next(1, 500));
            var statusCode = rng.Next(0, 10) < 8 ? "OK" : "ERROR";
            spans.Add(new SpanEntry(
                now,
                resource,
                "TelemetryServer.DummyData",
                ToHexLower(RandomBytes(16)),
                ToHexLower(RandomBytes(8)),
                rng.Next(0, 2) == 0 ? null : ToHexLower(RandomBytes(8)),
                SpanNames[rng.Next(SpanNames.Length)],
                rng.Next(0, 2) == 0 ? "SPAN_KIND_SERVER" : "SPAN_KIND_CLIENT",
                start,
                start + duration,
                statusCode,
                statusCode == "ERROR" ? "Sample error" : null,
                [
                    new KeyValueAttr("http.method", rng.Next(0, 2) == 0 ? "GET" : "POST"),
                    new KeyValueAttr("http.status_code", statusCode == "ERROR" ? "500" : "200"),
                ]));
        }
        store.AddSpans(spans);
        return spans.Count;
    }

    public DummyInjectionResult InjectAll(int count) =>
        new(InjectMetrics(count), InjectLogs(count), InjectSpans(count));

    private static ResourceInfo BuildResource(Random rng) =>
        new(
        [
            new KeyValueAttr("service.name", Services[rng.Next(Services.Length)]),
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
