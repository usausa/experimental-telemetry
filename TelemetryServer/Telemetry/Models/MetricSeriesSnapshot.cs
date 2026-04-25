namespace TelemetryServer.Telemetry.Models;

public sealed record MetricSeriesSnapshot(
    string ServiceName,
    string MetricName,
    string? Unit,
    string? Description,
    MetricKind Kind,
    IReadOnlyList<MetricPoint> Points);
