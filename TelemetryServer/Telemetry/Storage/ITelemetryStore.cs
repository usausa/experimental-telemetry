namespace TelemetryServer.Telemetry.Storage;

using TelemetryServer.Telemetry.Models;

public interface ITelemetryStore
{
    void AddMetrics(IEnumerable<MetricPoint> points);

    void AddLogs(IEnumerable<LogEntry> logs);

    void AddSpans(IEnumerable<SpanEntry> spans);

    IReadOnlyList<string> GetServiceNames();

    IReadOnlyList<string> GetMetricNames(string serviceName);

    MetricSeriesSnapshot? GetMetricSeries(string serviceName, string metricName, int maxPoints = 500);

    IReadOnlyList<LogEntry> GetLogs(string? serviceName = null, int maxCount = 500);

    IReadOnlyList<TraceSummary> GetTraces(string? serviceName = null, int maxCount = 200);

    TraceDetail? GetTrace(string traceId);

    TelemetrySummary GetSummary();

    void Clear();

    event EventHandler<EventArgs>? Changed;
}
