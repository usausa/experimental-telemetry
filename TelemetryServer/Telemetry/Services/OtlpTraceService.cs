namespace TelemetryServer.Telemetry.Services;

using Grpc.Core;
using OpenTelemetry.Proto.Collector.Trace.V1;
using TelemetryServer.Telemetry.Storage;

public sealed class OtlpTraceService : TraceService.TraceServiceBase
{
    private readonly ITelemetryStore store;
    private readonly ILogger<OtlpTraceService> logger;

    public OtlpTraceService(ITelemetryStore store, ILogger<OtlpTraceService> logger)
    {
        this.store = store;
        this.logger = logger;
    }

    public override Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        var now = DateTimeOffset.UtcNow;
        var spans = TelemetryMapper.MapSpans(request.ResourceSpans, now).ToList();
        store.AddSpans(spans);
        logger.LogDebug("Received {Count} spans", spans.Count);
        return Task.FromResult(new ExportTraceServiceResponse());
    }
}
