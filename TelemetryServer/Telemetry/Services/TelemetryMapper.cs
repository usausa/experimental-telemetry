namespace TelemetryServer.Telemetry.Services;

using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using TelemetryServer.Telemetry.Models;

internal static class TelemetryMapper
{
    public static DateTimeOffset FromUnixNano(ulong unixNano)
    {
        if (unixNano == 0)
        {
            return DateTimeOffset.UtcNow;
        }
        var unixMs = (long)(unixNano / 1_000_000UL);
        return DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
    }

    public static ResourceInfo MapResource(Resource? resource)
    {
        if (resource is null)
        {
            return new ResourceInfo([]);
        }
        var attrs = resource.Attributes
            .Select(kv => new KeyValueAttr(kv.Key, AnyValueToString(kv.Value)))
            .ToList();
        return new ResourceInfo(attrs);
    }

    public static IReadOnlyList<KeyValueAttr> MapAttributes(IEnumerable<KeyValue> src) =>
        src.Select(kv => new KeyValueAttr(kv.Key, AnyValueToString(kv.Value))).ToList();

    public static string? AnyValueToString(AnyValue? value)
    {
        if (value is null)
        {
            return null;
        }
        return value.ValueCase switch
        {
            AnyValue.ValueOneofCase.StringValue => value.StringValue,
            AnyValue.ValueOneofCase.BoolValue => value.BoolValue ? "true" : "false",
            AnyValue.ValueOneofCase.IntValue => value.IntValue.ToString(CultureInfo.InvariantCulture),
            AnyValue.ValueOneofCase.DoubleValue => value.DoubleValue.ToString("G", CultureInfo.InvariantCulture),
            AnyValue.ValueOneofCase.BytesValue => Convert.ToHexString(value.BytesValue.Span),
            AnyValue.ValueOneofCase.ArrayValue => "[" + string.Join(",",
                value.ArrayValue.Values.Select(AnyValueToString)) + "]",
            AnyValue.ValueOneofCase.KvlistValue => "{" + string.Join(",",
                value.KvlistValue.Values.Select(kv => $"{kv.Key}={AnyValueToString(kv.Value)}")) + "}",
            _ => null,
        };
    }

    public static IEnumerable<MetricPoint> MapMetrics(IEnumerable<ResourceMetrics> resourceMetrics, DateTimeOffset receivedAt)
    {
        foreach (var rm in resourceMetrics)
        {
            var resource = MapResource(rm.Resource);
            foreach (var scope in rm.ScopeMetrics)
            {
                var scopeName = scope.Scope?.Name ?? string.Empty;
                foreach (var metric in scope.Metrics)
                {
                    foreach (var p in ExpandMetric(metric, receivedAt, resource, scopeName))
                    {
                        yield return p;
                    }
                }
            }
        }
    }

    private static IEnumerable<MetricPoint> ExpandMetric(Metric metric, DateTimeOffset receivedAt, ResourceInfo resource, string scopeName)
    {
        switch (metric.DataCase)
        {
            case Metric.DataOneofCase.Gauge:
                foreach (var dp in metric.Gauge.DataPoints)
                {
                    yield return BuildPoint(metric, MetricKind.Gauge, dp.TimeUnixNano, GetNumberValue(dp), MapAttributes(dp.Attributes), receivedAt, resource, scopeName);
                }
                break;
            case Metric.DataOneofCase.Sum:
                foreach (var dp in metric.Sum.DataPoints)
                {
                    yield return BuildPoint(metric, MetricKind.Sum, dp.TimeUnixNano, GetNumberValue(dp), MapAttributes(dp.Attributes), receivedAt, resource, scopeName);
                }
                break;
            case Metric.DataOneofCase.Histogram:
                foreach (var dp in metric.Histogram.DataPoints)
                {
                    var val = dp.HasSum ? dp.Sum : dp.Count;
                    yield return BuildPoint(metric, MetricKind.Histogram, dp.TimeUnixNano, val, MapAttributes(dp.Attributes), receivedAt, resource, scopeName);
                }
                break;
            case Metric.DataOneofCase.ExponentialHistogram:
                foreach (var dp in metric.ExponentialHistogram.DataPoints)
                {
                    var val = dp.HasSum ? dp.Sum : dp.Count;
                    yield return BuildPoint(metric, MetricKind.ExponentialHistogram, dp.TimeUnixNano, val, MapAttributes(dp.Attributes), receivedAt, resource, scopeName);
                }
                break;
            case Metric.DataOneofCase.Summary:
                foreach (var dp in metric.Summary.DataPoints)
                {
                    yield return BuildPoint(metric, MetricKind.Summary, dp.TimeUnixNano, dp.Sum, MapAttributes(dp.Attributes), receivedAt, resource, scopeName);
                }
                break;
            default:
                break;
        }
    }

    private static double GetNumberValue(NumberDataPoint dp) => dp.ValueCase switch
    {
        NumberDataPoint.ValueOneofCase.AsDouble => dp.AsDouble,
        NumberDataPoint.ValueOneofCase.AsInt => dp.AsInt,
        _ => 0d,
    };

    private static MetricPoint BuildPoint(Metric metric, MetricKind kind, ulong ts, double value, IReadOnlyList<KeyValueAttr> attrs, DateTimeOffset receivedAt, ResourceInfo resource, string scopeName) =>
        new(
            receivedAt,
            resource,
            scopeName,
            metric.Name,
            metric.Description,
            metric.Unit,
            kind,
            FromUnixNano(ts),
            value,
            attrs);

    public static IEnumerable<LogEntry> MapLogs(IEnumerable<ResourceLogs> resourceLogs, DateTimeOffset receivedAt)
    {
        foreach (var rl in resourceLogs)
        {
            var resource = MapResource(rl.Resource);
            foreach (var scope in rl.ScopeLogs)
            {
                var scopeName = scope.Scope?.Name ?? string.Empty;
                foreach (var record in scope.LogRecords)
                {
                    yield return new LogEntry(
                        receivedAt,
                        resource,
                        scopeName,
                        FromUnixNano(record.TimeUnixNano != 0 ? record.TimeUnixNano : record.ObservedTimeUnixNano),
                        string.IsNullOrEmpty(record.SeverityText) ? record.SeverityNumber.ToString() : record.SeverityText,
                        (int)record.SeverityNumber,
                        AnyValueToString(record.Body) ?? string.Empty,
                        ByteStringToHex(record.TraceId),
                        ByteStringToHex(record.SpanId),
                        MapAttributes(record.Attributes));
                }
            }
        }
    }

    public static IEnumerable<SpanEntry> MapSpans(IEnumerable<ResourceSpans> resourceSpans, DateTimeOffset receivedAt)
    {
        foreach (var rs in resourceSpans)
        {
            var resource = MapResource(rs.Resource);
            foreach (var scope in rs.ScopeSpans)
            {
                var scopeName = scope.Scope?.Name ?? string.Empty;
                foreach (var span in scope.Spans)
                {
                    yield return new SpanEntry(
                        receivedAt,
                        resource,
                        scopeName,
                        ByteStringToHex(span.TraceId) ?? string.Empty,
                        ByteStringToHex(span.SpanId) ?? string.Empty,
                        ByteStringToHex(span.ParentSpanId),
                        span.Name,
                        span.Kind.ToString(),
                        FromUnixNano(span.StartTimeUnixNano),
                        FromUnixNano(span.EndTimeUnixNano),
                        span.Status?.Code.ToString() ?? "UNSET",
                        span.Status?.Message,
                        MapAttributes(span.Attributes));
                }
            }
        }
    }

    [SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase", Justification = "OTEL specification mandates lowercase hex for trace and span identifiers.")]
    private static string? ByteStringToHex(ByteString? bytes)
    {
        if (bytes is null || bytes.Length == 0)
        {
            return null;
        }
        return Convert.ToHexString(bytes.Span).ToLowerInvariant();
    }
}
