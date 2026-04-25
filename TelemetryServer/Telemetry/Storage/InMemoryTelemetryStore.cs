namespace TelemetryServer.Telemetry.Storage;

using System.Collections.Generic;
using TelemetryServer.Telemetry.Models;

public sealed class InMemoryTelemetryStore : ITelemetryStore
{
    private readonly TelemetryStoreOptions options;
    private readonly Lock sync = new();
    private readonly Dictionary<string, ServiceBucket> services = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TraceBucket> traces = new(StringComparer.Ordinal);
    private DateTimeOffset? lastReceivedAt;

    public event EventHandler<EventArgs>? Changed;

    public InMemoryTelemetryStore(TelemetryStoreOptions options)
    {
        this.options = options;
    }

    public void AddMetrics(IEnumerable<MetricPoint> points)
    {
        var added = false;
        lock (sync)
        {
            foreach (var p in points)
            {
                var bucket = GetOrCreateService(p.Resource.ServiceName);
                if (!bucket.MetricSeries.TryGetValue(p.Name, out var series))
                {
                    series = new MetricSeriesBucket(p.Unit, p.Description, p.Kind);
                    bucket.MetricSeries[p.Name] = series;
                }
                else
                {
                    if (string.IsNullOrEmpty(series.Unit) && !string.IsNullOrEmpty(p.Unit))
                    {
                        series.Unit = p.Unit;
                    }
                    if (string.IsNullOrEmpty(series.Description) && !string.IsNullOrEmpty(p.Description))
                    {
                        series.Description = p.Description;
                    }
                    series.Kind = p.Kind;
                }
                series.Points.AddLast(p);
                while (series.Points.Count > options.MaxPointsPerMetricSeries)
                {
                    series.Points.RemoveFirst();
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

    public void AddLogs(IEnumerable<LogEntry> logs)
    {
        var added = false;
        lock (sync)
        {
            foreach (var entry in logs)
            {
                var bucket = GetOrCreateService(entry.Resource.ServiceName);
                bucket.Logs.AddLast(entry);
                while (bucket.Logs.Count > options.MaxLogsPerService)
                {
                    bucket.Logs.RemoveFirst();
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

    public void AddSpans(IEnumerable<SpanEntry> spans)
    {
        var added = false;
        lock (sync)
        {
            foreach (var span in spans)
            {
                if (!traces.TryGetValue(span.TraceId, out var trace))
                {
                    trace = new TraceBucket(span.TraceId);
                    traces[span.TraceId] = trace;
                }
                trace.AddSpan(span, options.MaxSpansPerTrace);
                var bucket = GetOrCreateService(span.Resource.ServiceName);
                bucket.SpanCount++;
                added = true;
            }
            if (added)
            {
                EvictTracesIfNeeded();
                lastReceivedAt = DateTimeOffset.UtcNow;
            }
        }
        if (added)
        {
            Changed?.Invoke(this, EventArgs.Empty);
        }
    }

    public IReadOnlyList<string> GetServiceNames()
    {
        lock (sync)
        {
            return services.Keys
                .OrderBy(k => k, StringComparer.Ordinal)
                .ToList();
        }
    }

    public IReadOnlyList<string> GetMetricNames(string serviceName)
    {
        lock (sync)
        {
            return services.TryGetValue(serviceName, out var bucket)
                ? bucket.MetricSeries.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList()
                : [];
        }
    }

    public MetricSeriesSnapshot? GetMetricSeries(string serviceName, string metricName, int maxPoints = 500)
    {
        lock (sync)
        {
            if (!services.TryGetValue(serviceName, out var bucket))
            {
                return null;
            }
            if (!bucket.MetricSeries.TryGetValue(metricName, out var series))
            {
                return null;
            }
            var points = series.Points.Count <= maxPoints
                ? series.Points.ToList()
                : series.Points.Skip(series.Points.Count - maxPoints).ToList();
            return new MetricSeriesSnapshot(serviceName, metricName, series.Unit, series.Description, series.Kind, points);
        }
    }

    public IReadOnlyList<LogEntry> GetLogs(string? serviceName = null, int maxCount = 500)
    {
        lock (sync)
        {
            if (serviceName is not null)
            {
                return services.TryGetValue(serviceName, out var bucket)
                    ? TakeNewest(bucket.Logs, maxCount)
                    : [];
            }
            var combined = new List<LogEntry>();
            foreach (var bucket in services.Values)
            {
                combined.AddRange(bucket.Logs);
            }
            combined.Sort((a, b) => b.Timestamp.CompareTo(a.Timestamp));
            if (combined.Count > maxCount)
            {
                combined.RemoveRange(maxCount, combined.Count - maxCount);
            }
            return combined;
        }
    }

    public IReadOnlyList<TraceSummary> GetTraces(string? serviceName = null, int maxCount = 200)
    {
        lock (sync)
        {
            IEnumerable<TraceBucket> source = traces.Values;
            if (serviceName is not null)
            {
                source = source.Where(t => t.Services.Contains(serviceName));
            }
            return source
                .OrderByDescending(t => t.LastUpdated)
                .Take(maxCount)
                .Select(t => t.ToSummary())
                .ToList();
        }
    }

    public TraceDetail? GetTrace(string traceId)
    {
        lock (sync)
        {
            return traces.TryGetValue(traceId, out var trace) ? trace.ToDetail() : null;
        }
    }

    public TelemetrySummary GetSummary()
    {
        lock (sync)
        {
            var metricCount = 0;
            var logCount = 0;
            var spanCount = 0;
            foreach (var bucket in services.Values)
            {
                foreach (var series in bucket.MetricSeries.Values)
                {
                    metricCount += series.Points.Count;
                }
                logCount += bucket.Logs.Count;
                spanCount += bucket.SpanCount;
            }
            return new TelemetrySummary(services.Count, metricCount, logCount, spanCount, traces.Count, lastReceivedAt);
        }
    }

    public void Clear()
    {
        lock (sync)
        {
            services.Clear();
            traces.Clear();
            lastReceivedAt = null;
        }
        Changed?.Invoke(this, EventArgs.Empty);
    }

    private void EvictTracesIfNeeded()
    {
        if (traces.Count <= options.MaxTraces)
        {
            return;
        }
        var toRemove = traces.Values
            .OrderBy(t => t.LastUpdated)
            .Take(traces.Count - options.MaxTraces)
            .Select(t => t.TraceId)
            .ToList();
        foreach (var id in toRemove)
        {
            traces.Remove(id);
        }
    }

    private ServiceBucket GetOrCreateService(string name)
    {
        if (!services.TryGetValue(name, out var bucket))
        {
            bucket = new ServiceBucket(name);
            services[name] = bucket;
        }
        return bucket;
    }

    private static List<T> TakeNewest<T>(LinkedList<T> list, int maxCount)
    {
        var take = Math.Min(list.Count, maxCount);
        var result = new List<T>(take);
        var node = list.Last;
        while (node is not null && result.Count < take)
        {
            result.Add(node.Value);
            node = node.Previous;
        }
        return result;
    }

    private sealed class ServiceBucket
    {
        public ServiceBucket(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public Dictionary<string, MetricSeriesBucket> MetricSeries { get; } = new(StringComparer.Ordinal);

        public LinkedList<LogEntry> Logs { get; } = new();

        public int SpanCount { get; set; }
    }

    private sealed class MetricSeriesBucket
    {
        public MetricSeriesBucket(string? unit, string? description, MetricKind kind)
        {
            Unit = unit;
            Description = description;
            Kind = kind;
        }

        public string? Unit { get; set; }

        public string? Description { get; set; }

        public MetricKind Kind { get; set; }

        public LinkedList<MetricPoint> Points { get; } = new();
    }

    private sealed class TraceBucket
    {
        public TraceBucket(string traceId)
        {
            TraceId = traceId;
            StartTime = DateTimeOffset.MaxValue;
            EndTime = DateTimeOffset.MinValue;
            LastUpdated = DateTimeOffset.UtcNow;
        }

        public string TraceId { get; }

        public List<SpanEntry> Spans { get; } = [];

        public HashSet<string> Services { get; } = new(StringComparer.Ordinal);

        public DateTimeOffset StartTime { get; private set; }

        public DateTimeOffset EndTime { get; private set; }

        public DateTimeOffset LastUpdated { get; private set; }

        public void AddSpan(SpanEntry span, int maxSpans)
        {
            Spans.Add(span);
            Services.Add(span.Resource.ServiceName);
            if (span.StartTime < StartTime)
            {
                StartTime = span.StartTime;
            }
            if (span.EndTime > EndTime)
            {
                EndTime = span.EndTime;
            }
            LastUpdated = DateTimeOffset.UtcNow;

            if (Spans.Count > maxSpans)
            {
                Spans.Sort((a, b) => a.StartTime.CompareTo(b.StartTime));
                Spans.RemoveRange(0, Spans.Count - maxSpans);
            }
        }

        public TraceSummary ToSummary()
        {
            var (rootName, primary) = ComputeRoot();
            return new TraceSummary(
                TraceId,
                primary,
                Services.OrderBy(s => s, StringComparer.Ordinal).ToList(),
                rootName,
                StartTime,
                EndTime,
                Spans.Count,
                CountErrors());
        }

        public TraceDetail ToDetail() =>
            new(
                TraceId,
                ComputeRoot().Primary,
                Services.OrderBy(s => s, StringComparer.Ordinal).ToList(),
                StartTime,
                EndTime,
                CountErrors(),
                Spans.OrderBy(s => s.StartTime).ToList());

        private (string Name, string Primary) ComputeRoot()
        {
            if (Spans.Count == 0)
            {
                return (string.Empty, string.Empty);
            }
            var spanIds = new HashSet<string>(Spans.Select(s => s.SpanId), StringComparer.Ordinal);
            var root = Spans.FirstOrDefault(s => string.IsNullOrEmpty(s.ParentSpanId) || !spanIds.Contains(s.ParentSpanId));
            var picked = root ?? Spans.OrderBy(s => s.StartTime).First();
            return (picked.Name, picked.Resource.ServiceName);
        }

        private int CountErrors() =>
            Spans.Count(s => string.Equals(s.StatusCode, "ERROR", StringComparison.OrdinalIgnoreCase));
    }
}
