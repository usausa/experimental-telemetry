namespace TelemetryServer.Telemetry.Services;

using Grpc.Core;
using OpenTelemetry.Proto.Collector.Logs.V1;
using TelemetryServer.Telemetry.Storage;

public sealed class OtlpLogsService : LogsService.LogsServiceBase
{
    private readonly ITelemetryStore store;
    private readonly ILogger<OtlpLogsService> logger;

    public OtlpLogsService(ITelemetryStore store, ILogger<OtlpLogsService> logger)
    {
        this.store = store;
        this.logger = logger;
    }

    public override Task<ExportLogsServiceResponse> Export(
        ExportLogsServiceRequest request,
        ServerCallContext context)
    {
        var now = DateTimeOffset.UtcNow;
        var logs = TelemetryMapper.MapLogs(request.ResourceLogs, now).ToList();
        store.AddLogs(logs);
        logger.LogDebug("Received {Count} log records", logs.Count);
        return Task.FromResult(new ExportLogsServiceResponse());
    }
}
