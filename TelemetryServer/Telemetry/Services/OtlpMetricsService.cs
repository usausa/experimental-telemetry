namespace TelemetryServer.Telemetry.Services;

using Grpc.Core;
using OpenTelemetry.Proto.Collector.Metrics.V1;
using TelemetryServer.Telemetry.Storage;

public sealed class OtlpMetricsService : MetricsService.MetricsServiceBase
{
    private readonly ITelemetryStore store;
    private readonly ILogger<OtlpMetricsService> logger;

    public OtlpMetricsService(ITelemetryStore store, ILogger<OtlpMetricsService> logger)
    {
        this.store = store;
        this.logger = logger;
    }

    public override Task<ExportMetricsServiceResponse> Export(
        ExportMetricsServiceRequest request,
        ServerCallContext context)
    {
        var now = DateTimeOffset.UtcNow;
        var points = TelemetryMapper.MapMetrics(request.ResourceMetrics, now).ToList();
        store.AddMetrics(points);
        logger.LogDebug("Received {Count} metric points", points.Count);
        return Task.FromResult(new ExportMetricsServiceResponse());
    }
}
