namespace TelemetryServer.Telemetry.Storage;

public sealed record TelemetrySummary(
    int MetricCount,
    int LogCount,
    int SpanCount,
    DateTimeOffset? LastReceivedAt);
