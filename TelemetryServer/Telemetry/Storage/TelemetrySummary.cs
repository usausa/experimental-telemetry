namespace TelemetryServer.Telemetry.Storage;

public sealed record TelemetrySummary(
    int ServiceCount,
    int MetricCount,
    int LogCount,
    int SpanCount,
    int TraceCount,
    DateTimeOffset? LastReceivedAt);
