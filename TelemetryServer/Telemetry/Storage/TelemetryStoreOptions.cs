namespace TelemetryServer.Telemetry.Storage;

public sealed class TelemetryStoreOptions
{
    public string DatabasePath { get; set; } = "App_Data/telemetry.db";

    public int MaxPointsPerMetricSeries { get; set; } = 500;

    public int MaxLogsPerService { get; set; } = 2000;

    public int MaxTraces { get; set; } = 500;

    public int MaxSpansPerTrace { get; set; } = 200;
}
