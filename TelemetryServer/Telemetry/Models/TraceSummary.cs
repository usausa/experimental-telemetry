namespace TelemetryServer.Telemetry.Models;

public sealed record TraceSummary(
    string TraceId,
    string PrimaryService,
    IReadOnlyList<string> Services,
    string RootSpanName,
    DateTimeOffset StartTime,
    DateTimeOffset EndTime,
    int SpanCount,
    int ErrorCount)
{
    public TimeSpan Duration => EndTime - StartTime;
}
