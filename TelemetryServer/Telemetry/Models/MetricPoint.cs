namespace TelemetryServer.Telemetry.Models;

public enum MetricKind
{
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

public sealed record MetricPoint(
    DateTimeOffset ReceivedAt,
    ResourceInfo Resource,
    string ScopeName,
    string Name,
    string? Description,
    string? Unit,
    MetricKind Kind,
    DateTimeOffset Timestamp,
    double Value,
    IReadOnlyList<KeyValueAttr> Attributes);
