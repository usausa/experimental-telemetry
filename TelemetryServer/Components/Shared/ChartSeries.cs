namespace TelemetryServer.Components.Shared;

public sealed record ChartSeries(string Label, IReadOnlyList<ChartPoint> Points);
