namespace TelemetryServer.Telemetry.Storage;

using TelemetryServer.Telemetry.Models;

public interface ITelemetryStore
{
    void AddMetrics(IEnumerable<MetricPoint> points);
    void AddLogs(IEnumerable<LogEntry> logs);
    void AddSpans(IEnumerable<SpanEntry> spans);

    IReadOnlyList<MetricPoint> GetMetrics(int maxCount = 500);
    IReadOnlyList<LogEntry> GetLogs(int maxCount = 500);
    IReadOnlyList<SpanEntry> GetSpans(int maxCount = 500);

    TelemetrySummary GetSummary();

    void Clear();

    event EventHandler<EventArgs>? Changed;
}
