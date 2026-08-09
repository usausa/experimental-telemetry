namespace TelemetryServer.Telemetry.Services;

using TelemetryServer.Telemetry.Storage;

public sealed class TelemetryRetentionService : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromHours(1);

    private readonly ILogger<TelemetryRetentionService> log;

    private readonly ITelemetryStore store;

    private readonly TelemetryStoreOptions options;

    public TelemetryRetentionService(
        ILogger<TelemetryRetentionService> log,
        TelemetryStoreOptions options
        ITelemetryStore store)
    {
        this.log = log;
        this.options = options;
        this.store = store;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (options.RetentionDays <= 0)
        {
            log.InfoRetentionDisabled();
            return;
        }

        using var timer = new PeriodicTimer(Interval);
        try
        {
            do
            {
                var deleted = store.PurgeExpired();
                if (deleted > 0)
                {
                    log.InfoTelemetryPurged(deleted, options.RetentionDays);
                }
            }
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException)
        {
            // Shutdown
        }
    }
}

internal static partial class Log
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Telemetry retention is disabled.")]
    public static partial void InfoRetentionDisabled(this ILogger log);

    [LoggerMessage(Level = LogLevel.Information, Message = "Telemetry purged. rows=[{Rows}], retentionDays=[{RetentionDays}]")]
    public static partial void InfoTelemetryPurged(this ILogger log, int rows, int retentionDays);
}
