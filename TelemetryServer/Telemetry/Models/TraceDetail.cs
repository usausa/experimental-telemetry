namespace TelemetryServer.Telemetry.Models;

public sealed record TraceDetail(
    string TraceId,
    string PrimaryService,
    IReadOnlyList<string> Services,
    DateTimeOffset StartTime,
    DateTimeOffset EndTime,
    int ErrorCount,
    IReadOnlyList<SpanEntry> Spans)
{
    public TimeSpan Duration => EndTime - StartTime;
}
