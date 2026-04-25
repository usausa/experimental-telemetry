namespace TelemetryServer.Telemetry.Storage;

using TelemetryServer.Telemetry.Models;

public sealed class InMemoryTelemetryStore : ITelemetryStore
{
    private readonly int capacity;
    private readonly Lock sync = new();
    private readonly LinkedList<MetricPoint> metrics = new();
    private readonly LinkedList<LogEntry> logs = new();
    private readonly LinkedList<SpanEntry> spans = new();
    private DateTimeOffset? lastReceivedAt;

    public event EventHandler<EventArgs>? Changed;

    public InMemoryTelemetryStore(int capacity = 5000)
    {
        this.capacity = capacity;
    }

    public void AddMetrics(IEnumerable<MetricPoint> points) =>
        Append(metrics, points);

    public void AddLogs(IEnumerable<LogEntry> logs) =>
        Append(this.logs, logs);

    public void AddSpans(IEnumerable<SpanEntry> spans) =>
        Append(this.spans, spans);

    private void Append<T>(LinkedList<T> list, IEnumerable<T> items)
    {
        var added = false;
        lock (sync)
        {
            foreach (var item in items)
            {
                list.AddLast(item);
                while (list.Count > capacity)
                {
                    list.RemoveFirst();
                }
                added = true;
            }
            if (added)
            {
                lastReceivedAt = DateTimeOffset.UtcNow;
            }
        }
        if (added)
        {
            Changed?.Invoke(this, EventArgs.Empty);
        }
    }

    public IReadOnlyList<MetricPoint> GetMetrics(int maxCount = 500) =>
        Snapshot(metrics, maxCount);

    public IReadOnlyList<LogEntry> GetLogs(int maxCount = 500) =>
        Snapshot(logs, maxCount);

    public IReadOnlyList<SpanEntry> GetSpans(int maxCount = 500) =>
        Snapshot(spans, maxCount);

    private List<T> Snapshot<T>(LinkedList<T> list, int maxCount)
    {
        lock (sync)
        {
            var total = list.Count;
            var take = Math.Min(total, maxCount);
            var result = new List<T>(take);
            var node = list.Last;
            while (node is not null && result.Count < take)
            {
                result.Add(node.Value);
                node = node.Previous;
            }
            return result;
        }
    }

    public TelemetrySummary GetSummary()
    {
        lock (sync)
        {
            return new TelemetrySummary(
                metrics.Count,
                logs.Count,
                spans.Count,
                lastReceivedAt);
        }
    }

    public void Clear()
    {
        lock (sync)
        {
            metrics.Clear();
            logs.Clear();
            spans.Clear();
            lastReceivedAt = null;
        }
        Changed?.Invoke(this, EventArgs.Empty);
    }
}
