#pragma warning disable CA1716
namespace TelemetryServer.Components.Shared;
#pragma warning restore CA1716

public sealed record ChartPoint(DateTimeOffset Time, double Value);
