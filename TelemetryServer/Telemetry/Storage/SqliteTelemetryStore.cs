namespace TelemetryServer.Telemetry.Storage;

using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Hosting;
using TelemetryServer.Telemetry.Models;

public sealed class SqliteTelemetryStore : ITelemetryStore
{
    private const string LastReceivedAtKey = "last_received_at";
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly string connectionString;
    private readonly TelemetryStoreOptions options;
    private readonly Lock sync = new();

    public SqliteTelemetryStore(TelemetryStoreOptions options, IHostEnvironment hostEnvironment)
    {
        this.options = options;

        var databasePath = ResolveDatabasePath(options.DatabasePath, hostEnvironment.ContentRootPath);
        var databaseDirectory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrEmpty(databaseDirectory))
        {
            Directory.CreateDirectory(databaseDirectory);
        }

        connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Cache = SqliteCacheMode.Shared,
            Mode = SqliteOpenMode.ReadWriteCreate,
        }.ToString();

        InitializeDatabase();
    }

    public event EventHandler<EventArgs>? Changed;

    public void AddMetrics(IEnumerable<MetricPoint> points)
    {
        var materializedPoints = points.ToList();
        if (materializedPoints.Count == 0)
        {
            return;
        }

        var changed = false;
        lock (sync)
        {
            using var connection = OpenConnection();
            using var transaction = connection.BeginTransaction();
            using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText =
                """
                INSERT INTO metrics (
                    service_name,
                    received_at_utc,
                    resource_json,
                    scope_name,
                    metric_name,
                    description,
                    unit,
                    kind,
                    timestamp_utc,
                    value,
                    attributes_json
                )
                VALUES (
                    $service_name,
                    $received_at_utc,
                    $resource_json,
                    $scope_name,
                    $metric_name,
                    $description,
                    $unit,
                    $kind,
                    $timestamp_utc,
                    $value,
                    $attributes_json
                );
                """;

            var serviceNameParameter = insert.Parameters.Add("$service_name", SqliteType.Text);
            var receivedAtParameter = insert.Parameters.Add("$received_at_utc", SqliteType.Text);
            var resourceJsonParameter = insert.Parameters.Add("$resource_json", SqliteType.Text);
            var scopeNameParameter = insert.Parameters.Add("$scope_name", SqliteType.Text);
            var metricNameParameter = insert.Parameters.Add("$metric_name", SqliteType.Text);
            var descriptionParameter = insert.Parameters.Add("$description", SqliteType.Text);
            var unitParameter = insert.Parameters.Add("$unit", SqliteType.Text);
            var kindParameter = insert.Parameters.Add("$kind", SqliteType.Integer);
            var timestampParameter = insert.Parameters.Add("$timestamp_utc", SqliteType.Text);
            var valueParameter = insert.Parameters.Add("$value", SqliteType.Real);
            var attributesJsonParameter = insert.Parameters.Add("$attributes_json", SqliteType.Text);

            var touchedSeries = new HashSet<MetricSeriesKey>();
            foreach (var point in materializedPoints)
            {
                serviceNameParameter.Value = point.Resource.ServiceName;
                receivedAtParameter.Value = FormatDateTime(point.ReceivedAt);
                resourceJsonParameter.Value = SerializeAttributes(point.Resource.Attributes);
                scopeNameParameter.Value = point.ScopeName;
                metricNameParameter.Value = point.Name;
                descriptionParameter.Value = (object?)point.Description ?? DBNull.Value;
                unitParameter.Value = (object?)point.Unit ?? DBNull.Value;
                kindParameter.Value = (int)point.Kind;
                timestampParameter.Value = FormatDateTime(point.Timestamp);
                valueParameter.Value = point.Value;
                attributesJsonParameter.Value = SerializeAttributes(point.Attributes);
                insert.ExecuteNonQuery();

                touchedSeries.Add(new MetricSeriesKey(point.Resource.ServiceName, point.Name));
                changed = true;
            }

            foreach (var key in touchedSeries)
            {
                TrimMetricSeries(connection, transaction, key.ServiceName, key.MetricName);
            }

            UpdateLastReceivedAt(connection, transaction, DateTimeOffset.UtcNow);
            transaction.Commit();
        }

        if (changed)
        {
            Changed?.Invoke(this, EventArgs.Empty);
        }
    }

    public void AddLogs(IEnumerable<LogEntry> logs)
    {
        var materializedLogs = logs.ToList();
        if (materializedLogs.Count == 0)
        {
            return;
        }

        var changed = false;
        lock (sync)
        {
            using var connection = OpenConnection();
            using var transaction = connection.BeginTransaction();
            using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText =
                """
                INSERT INTO logs (
                    service_name,
                    received_at_utc,
                    resource_json,
                    scope_name,
                    timestamp_utc,
                    severity_text,
                    severity_number,
                    body,
                    trace_id,
                    span_id,
                    attributes_json
                )
                VALUES (
                    $service_name,
                    $received_at_utc,
                    $resource_json,
                    $scope_name,
                    $timestamp_utc,
                    $severity_text,
                    $severity_number,
                    $body,
                    $trace_id,
                    $span_id,
                    $attributes_json
                );
                """;

            var serviceNameParameter = insert.Parameters.Add("$service_name", SqliteType.Text);
            var receivedAtParameter = insert.Parameters.Add("$received_at_utc", SqliteType.Text);
            var resourceJsonParameter = insert.Parameters.Add("$resource_json", SqliteType.Text);
            var scopeNameParameter = insert.Parameters.Add("$scope_name", SqliteType.Text);
            var timestampParameter = insert.Parameters.Add("$timestamp_utc", SqliteType.Text);
            var severityTextParameter = insert.Parameters.Add("$severity_text", SqliteType.Text);
            var severityNumberParameter = insert.Parameters.Add("$severity_number", SqliteType.Integer);
            var bodyParameter = insert.Parameters.Add("$body", SqliteType.Text);
            var traceIdParameter = insert.Parameters.Add("$trace_id", SqliteType.Text);
            var spanIdParameter = insert.Parameters.Add("$span_id", SqliteType.Text);
            var attributesJsonParameter = insert.Parameters.Add("$attributes_json", SqliteType.Text);

            var touchedServices = new HashSet<string>(StringComparer.Ordinal);
            foreach (var log in materializedLogs)
            {
                serviceNameParameter.Value = log.Resource.ServiceName;
                receivedAtParameter.Value = FormatDateTime(log.ReceivedAt);
                resourceJsonParameter.Value = SerializeAttributes(log.Resource.Attributes);
                scopeNameParameter.Value = log.ScopeName;
                timestampParameter.Value = FormatDateTime(log.Timestamp);
                severityTextParameter.Value = log.SeverityText;
                severityNumberParameter.Value = log.SeverityNumber;
                bodyParameter.Value = log.Body;
                traceIdParameter.Value = (object?)log.TraceId ?? DBNull.Value;
                spanIdParameter.Value = (object?)log.SpanId ?? DBNull.Value;
                attributesJsonParameter.Value = SerializeAttributes(log.Attributes);
                insert.ExecuteNonQuery();

                touchedServices.Add(log.Resource.ServiceName);
                changed = true;
            }

            foreach (var serviceName in touchedServices)
            {
                TrimLogs(connection, transaction, serviceName);
            }

            UpdateLastReceivedAt(connection, transaction, DateTimeOffset.UtcNow);
            transaction.Commit();
        }

        if (changed)
        {
            Changed?.Invoke(this, EventArgs.Empty);
        }
    }

    public void AddSpans(IEnumerable<SpanEntry> spans)
    {
        var materializedSpans = spans.ToList();
        if (materializedSpans.Count == 0)
        {
            return;
        }

        var changed = false;
        lock (sync)
        {
            using var connection = OpenConnection();
            using var transaction = connection.BeginTransaction();
            using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText =
                """
                INSERT INTO spans (
                    trace_id,
                    span_id,
                    parent_span_id,
                    service_name,
                    received_at_utc,
                    resource_json,
                    scope_name,
                    name,
                    kind,
                    start_time_utc,
                    end_time_utc,
                    status_code,
                    status_message,
                    attributes_json
                )
                VALUES (
                    $trace_id,
                    $span_id,
                    $parent_span_id,
                    $service_name,
                    $received_at_utc,
                    $resource_json,
                    $scope_name,
                    $name,
                    $kind,
                    $start_time_utc,
                    $end_time_utc,
                    $status_code,
                    $status_message,
                    $attributes_json
                );
                """;

            var traceIdParameter = insert.Parameters.Add("$trace_id", SqliteType.Text);
            var spanIdParameter = insert.Parameters.Add("$span_id", SqliteType.Text);
            var parentSpanIdParameter = insert.Parameters.Add("$parent_span_id", SqliteType.Text);
            var serviceNameParameter = insert.Parameters.Add("$service_name", SqliteType.Text);
            var receivedAtParameter = insert.Parameters.Add("$received_at_utc", SqliteType.Text);
            var resourceJsonParameter = insert.Parameters.Add("$resource_json", SqliteType.Text);
            var scopeNameParameter = insert.Parameters.Add("$scope_name", SqliteType.Text);
            var nameParameter = insert.Parameters.Add("$name", SqliteType.Text);
            var kindParameter = insert.Parameters.Add("$kind", SqliteType.Text);
            var startTimeParameter = insert.Parameters.Add("$start_time_utc", SqliteType.Text);
            var endTimeParameter = insert.Parameters.Add("$end_time_utc", SqliteType.Text);
            var statusCodeParameter = insert.Parameters.Add("$status_code", SqliteType.Text);
            var statusMessageParameter = insert.Parameters.Add("$status_message", SqliteType.Text);
            var attributesJsonParameter = insert.Parameters.Add("$attributes_json", SqliteType.Text);

            var touchedTraceIds = new HashSet<string>(StringComparer.Ordinal);
            foreach (var span in materializedSpans)
            {
                traceIdParameter.Value = span.TraceId;
                spanIdParameter.Value = span.SpanId;
                parentSpanIdParameter.Value = (object?)span.ParentSpanId ?? DBNull.Value;
                serviceNameParameter.Value = span.Resource.ServiceName;
                receivedAtParameter.Value = FormatDateTime(span.ReceivedAt);
                resourceJsonParameter.Value = SerializeAttributes(span.Resource.Attributes);
                scopeNameParameter.Value = span.ScopeName;
                nameParameter.Value = span.Name;
                kindParameter.Value = span.Kind;
                startTimeParameter.Value = FormatDateTime(span.StartTime);
                endTimeParameter.Value = FormatDateTime(span.EndTime);
                statusCodeParameter.Value = span.StatusCode;
                statusMessageParameter.Value = (object?)span.StatusMessage ?? DBNull.Value;
                attributesJsonParameter.Value = SerializeAttributes(span.Attributes);
                insert.ExecuteNonQuery();

                touchedTraceIds.Add(span.TraceId);
                changed = true;
            }

            foreach (var traceId in touchedTraceIds)
            {
                TrimTraceSpans(connection, transaction, traceId);
                RefreshTraceInfo(connection, transaction, traceId);
            }

            EnforceTraceLimit(connection, transaction);
            UpdateLastReceivedAt(connection, transaction, DateTimeOffset.UtcNow);
            transaction.Commit();
        }

        if (changed)
        {
            Changed?.Invoke(this, EventArgs.Empty);
        }
    }

    public IReadOnlyList<string> GetServiceNames()
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText =
                """
                SELECT service_name
                FROM (
                    SELECT service_name FROM metrics
                    UNION
                    SELECT service_name FROM logs
                    UNION
                    SELECT service_name FROM spans
                )
                ORDER BY service_name;
                """;

            var services = new List<string>();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                services.Add(reader.GetString(0));
            }

            return services;
        }
    }

    public IReadOnlyList<string> GetMetricNames(string serviceName)
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText =
                """
                SELECT DISTINCT metric_name
                FROM metrics
                WHERE service_name = $service_name
                ORDER BY metric_name;
                """;
            command.Parameters.AddWithValue("$service_name", serviceName);

            var metricNames = new List<string>();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                metricNames.Add(reader.GetString(0));
            }

            return metricNames;
        }
    }

    public MetricSeriesSnapshot? GetMetricSeries(string serviceName, string metricName, int maxPoints = 500)
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            MetricPointHeader? header = null;
            using (var headerCommand = connection.CreateCommand())
            {
                headerCommand.CommandText =
                    """
                    SELECT description, unit, kind
                    FROM metrics
                    WHERE service_name = $service_name
                      AND metric_name = $metric_name
                    ORDER BY id DESC
                    LIMIT 1;
                    """;
                headerCommand.Parameters.AddWithValue("$service_name", serviceName);
                headerCommand.Parameters.AddWithValue("$metric_name", metricName);

                using var reader = headerCommand.ExecuteReader();
                if (reader.Read())
                {
                    header = new MetricPointHeader(
                        reader.IsDBNull(0) ? null : reader.GetString(0),
                        reader.IsDBNull(1) ? null : reader.GetString(1),
                        (MetricKind)reader.GetInt32(2));
                }
            }

            if (header is null)
            {
                return null;
            }

            using var command = connection.CreateCommand();
            command.CommandText =
                """
                SELECT received_at_utc, resource_json, scope_name, timestamp_utc, value, attributes_json
                FROM (
                    SELECT id, received_at_utc, resource_json, scope_name, timestamp_utc, value, attributes_json
                    FROM metrics
                    WHERE service_name = $service_name
                      AND metric_name = $metric_name
                    ORDER BY id DESC
                    LIMIT $max_points
                )
                ORDER BY id;
                """;
            command.Parameters.AddWithValue("$service_name", serviceName);
            command.Parameters.AddWithValue("$metric_name", metricName);
            command.Parameters.AddWithValue("$max_points", maxPoints);

            var points = new List<MetricPoint>();
            using var pointReader = command.ExecuteReader();
            while (pointReader.Read())
            {
                points.Add(new MetricPoint(
                    ParseDateTime(pointReader.GetString(0)),
                    DeserializeResource(pointReader.GetString(1)),
                    pointReader.GetString(2),
                    metricName,
                    header.Description,
                    header.Unit,
                    header.Kind,
                    ParseDateTime(pointReader.GetString(3)),
                    pointReader.GetDouble(4),
                    DeserializeAttributes(pointReader.GetString(5))));
            }

            return new MetricSeriesSnapshot(serviceName, metricName, header.Unit, header.Description, header.Kind, points);
        }
    }

    public IReadOnlyList<LogEntry> GetLogs(string? serviceName = null, int maxCount = 500)
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText =
                """
                SELECT received_at_utc, resource_json, scope_name, timestamp_utc, severity_text, severity_number, body, trace_id, span_id, attributes_json
                FROM logs
                WHERE $service_name IS NULL OR service_name = $service_name
                ORDER BY timestamp_utc DESC, id DESC
                LIMIT $max_count;
                """;
            command.Parameters.AddWithValue("$service_name", (object?)serviceName ?? DBNull.Value);
            command.Parameters.AddWithValue("$max_count", maxCount);

            var logs = new List<LogEntry>();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                logs.Add(new LogEntry(
                    ParseDateTime(reader.GetString(0)),
                    DeserializeResource(reader.GetString(1)),
                    reader.GetString(2),
                    ParseDateTime(reader.GetString(3)),
                    reader.GetString(4),
                    reader.GetInt32(5),
                    reader.GetString(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    DeserializeAttributes(reader.GetString(9))));
            }

            return logs;
        }
    }

    public IReadOnlyList<TraceSummary> GetTraces(string? serviceName = null, int maxCount = 200)
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText =
                """
                SELECT trace_id, primary_service, services_json, root_span_name, start_time_utc, end_time_utc, span_count, error_count
                FROM trace_info
                WHERE $service_name IS NULL
                   OR EXISTS (
                       SELECT 1
                       FROM spans
                       WHERE spans.trace_id = trace_info.trace_id
                         AND spans.service_name = $service_name
                   )
                ORDER BY last_updated_utc DESC
                LIMIT $max_count;
                """;
            command.Parameters.AddWithValue("$service_name", (object?)serviceName ?? DBNull.Value);
            command.Parameters.AddWithValue("$max_count", maxCount);

            var traces = new List<TraceSummary>();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                traces.Add(new TraceSummary(
                    reader.GetString(0),
                    reader.GetString(1),
                    DeserializeServiceNames(reader.GetString(2)),
                    reader.GetString(3),
                    ParseDateTime(reader.GetString(4)),
                    ParseDateTime(reader.GetString(5)),
                    reader.GetInt32(6),
                    reader.GetInt32(7)));
            }

            return traces;
        }
    }

    public TraceDetail? GetTrace(string traceId)
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            var spans = ReadTraceSpans(connection, traceId);
            if (spans.Count == 0)
            {
                return null;
            }

            var details = ComputeTraceDetails(spans);
            return new TraceDetail(
                traceId,
                details.PrimaryService,
                details.Services,
                details.StartTime,
                details.EndTime,
                details.ErrorCount,
                spans);
        }
    }

    public TelemetrySummary GetSummary()
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText =
                """
                SELECT
                    (SELECT COUNT(*) FROM (
                        SELECT service_name FROM metrics
                        UNION
                        SELECT service_name FROM logs
                        UNION
                        SELECT service_name FROM spans
                    )) AS service_count,
                    (SELECT COUNT(*) FROM metrics) AS metric_count,
                    (SELECT COUNT(*) FROM logs) AS log_count,
                    (SELECT COUNT(*) FROM spans) AS span_count,
                    (SELECT COUNT(*) FROM trace_info) AS trace_count,
                    (SELECT value FROM metadata WHERE key = $last_received_at_key) AS last_received_at;
                """;
            command.Parameters.AddWithValue("$last_received_at_key", LastReceivedAtKey);

            using var reader = command.ExecuteReader();
            reader.Read();

            return new TelemetrySummary(
                reader.GetInt32(0),
                reader.GetInt32(1),
                reader.GetInt32(2),
                reader.GetInt32(3),
                reader.GetInt32(4),
                reader.IsDBNull(5) ? null : ParseDateTime(reader.GetString(5)));
        }
    }

    public void Clear()
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            using var transaction = connection.BeginTransaction();
            using (var deleteMetrics = connection.CreateCommand())
            {
                deleteMetrics.Transaction = transaction;
                deleteMetrics.CommandText = "DELETE FROM metrics;";
                deleteMetrics.ExecuteNonQuery();
            }

            using (var deleteLogs = connection.CreateCommand())
            {
                deleteLogs.Transaction = transaction;
                deleteLogs.CommandText = "DELETE FROM logs;";
                deleteLogs.ExecuteNonQuery();
            }

            using (var deleteSpans = connection.CreateCommand())
            {
                deleteSpans.Transaction = transaction;
                deleteSpans.CommandText = "DELETE FROM spans;";
                deleteSpans.ExecuteNonQuery();
            }

            using (var deleteTraceInfo = connection.CreateCommand())
            {
                deleteTraceInfo.Transaction = transaction;
                deleteTraceInfo.CommandText = "DELETE FROM trace_info;";
                deleteTraceInfo.ExecuteNonQuery();
            }

            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                """
                INSERT INTO metadata (key, value)
                VALUES ($key, NULL)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value;
                """;
            command.Parameters.AddWithValue("$key", LastReceivedAtKey);
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        Changed?.Invoke(this, EventArgs.Empty);
    }

    private void InitializeDatabase()
    {
        lock (sync)
        {
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText =
                """
                PRAGMA journal_mode = WAL;
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    service_name TEXT NOT NULL,
                    received_at_utc TEXT NOT NULL,
                    resource_json TEXT NOT NULL,
                    scope_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    description TEXT NULL,
                    unit TEXT NULL,
                    kind INTEGER NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    value REAL NOT NULL,
                    attributes_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ix_metrics_service_metric_id
                    ON metrics (service_name, metric_name, id DESC);

                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    service_name TEXT NOT NULL,
                    received_at_utc TEXT NOT NULL,
                    resource_json TEXT NOT NULL,
                    scope_name TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    severity_text TEXT NOT NULL,
                    severity_number INTEGER NOT NULL,
                    body TEXT NOT NULL,
                    trace_id TEXT NULL,
                    span_id TEXT NULL,
                    attributes_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ix_logs_service_id
                    ON logs (service_name, id DESC);

                CREATE INDEX IF NOT EXISTS ix_logs_timestamp_id
                    ON logs (timestamp_utc DESC, id DESC);

                CREATE TABLE IF NOT EXISTS spans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trace_id TEXT NOT NULL,
                    span_id TEXT NOT NULL,
                    parent_span_id TEXT NULL,
                    service_name TEXT NOT NULL,
                    received_at_utc TEXT NOT NULL,
                    resource_json TEXT NOT NULL,
                    scope_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    start_time_utc TEXT NOT NULL,
                    end_time_utc TEXT NOT NULL,
                    status_code TEXT NOT NULL,
                    status_message TEXT NULL,
                    attributes_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ix_spans_trace_start_id
                    ON spans (trace_id, start_time_utc, id);

                CREATE INDEX IF NOT EXISTS ix_spans_service_trace
                    ON spans (service_name, trace_id);

                CREATE TABLE IF NOT EXISTS trace_info (
                    trace_id TEXT PRIMARY KEY,
                    primary_service TEXT NOT NULL,
                    services_json TEXT NOT NULL,
                    root_span_name TEXT NOT NULL,
                    start_time_utc TEXT NOT NULL,
                    end_time_utc TEXT NOT NULL,
                    last_updated_utc TEXT NOT NULL,
                    span_count INTEGER NOT NULL,
                    error_count INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ix_trace_info_last_updated
                    ON trace_info (last_updated_utc DESC);

                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NULL
                );

                INSERT INTO metadata (key, value)
                VALUES ($last_received_at_key, NULL)
                ON CONFLICT(key) DO NOTHING;
                """;
            command.Parameters.AddWithValue("$last_received_at_key", LastReceivedAtKey);
            command.ExecuteNonQuery();
        }
    }

    private SqliteConnection OpenConnection()
    {
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText =
            """
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            """;
        command.ExecuteNonQuery();
        return connection;
    }

    private void TrimMetricSeries(SqliteConnection connection, SqliteTransaction transaction, string serviceName, string metricName)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            DELETE FROM metrics
            WHERE service_name = $service_name
              AND metric_name = $metric_name
              AND id NOT IN (
                  SELECT id
                  FROM metrics
                  WHERE service_name = $service_name
                    AND metric_name = $metric_name
                  ORDER BY id DESC
                  LIMIT $limit
              );
            """;
        command.Parameters.AddWithValue("$service_name", serviceName);
        command.Parameters.AddWithValue("$metric_name", metricName);
        command.Parameters.AddWithValue("$limit", options.MaxPointsPerMetricSeries);
        command.ExecuteNonQuery();
    }

    private void TrimLogs(SqliteConnection connection, SqliteTransaction transaction, string serviceName)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            DELETE FROM logs
            WHERE service_name = $service_name
              AND id NOT IN (
                  SELECT id
                  FROM logs
                  WHERE service_name = $service_name
                  ORDER BY id DESC
                  LIMIT $limit
              );
            """;
        command.Parameters.AddWithValue("$service_name", serviceName);
        command.Parameters.AddWithValue("$limit", options.MaxLogsPerService);
        command.ExecuteNonQuery();
    }

    private void TrimTraceSpans(SqliteConnection connection, SqliteTransaction transaction, string traceId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            DELETE FROM spans
            WHERE trace_id = $trace_id
              AND id NOT IN (
                  SELECT id
                  FROM spans
                  WHERE trace_id = $trace_id
                  ORDER BY start_time_utc DESC, id DESC
                  LIMIT $limit
              );
            """;
        command.Parameters.AddWithValue("$trace_id", traceId);
        command.Parameters.AddWithValue("$limit", options.MaxSpansPerTrace);
        command.ExecuteNonQuery();
    }

    private static void RefreshTraceInfo(SqliteConnection connection, SqliteTransaction transaction, string traceId)
    {
        var spans = ReadTraceSpans(connection, traceId, transaction);
        if (spans.Count == 0)
        {
            using var deleteCommand = connection.CreateCommand();
            deleteCommand.Transaction = transaction;
            deleteCommand.CommandText = "DELETE FROM trace_info WHERE trace_id = $trace_id;";
            deleteCommand.Parameters.AddWithValue("$trace_id", traceId);
            deleteCommand.ExecuteNonQuery();
            return;
        }

        var details = ComputeTraceDetails(spans);
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            INSERT INTO trace_info (
                trace_id,
                primary_service,
                services_json,
                root_span_name,
                start_time_utc,
                end_time_utc,
                last_updated_utc,
                span_count,
                error_count
            )
            VALUES (
                $trace_id,
                $primary_service,
                $services_json,
                $root_span_name,
                $start_time_utc,
                $end_time_utc,
                $last_updated_utc,
                $span_count,
                $error_count
            )
            ON CONFLICT(trace_id) DO UPDATE SET
                primary_service = excluded.primary_service,
                services_json = excluded.services_json,
                root_span_name = excluded.root_span_name,
                start_time_utc = excluded.start_time_utc,
                end_time_utc = excluded.end_time_utc,
                last_updated_utc = excluded.last_updated_utc,
                span_count = excluded.span_count,
                error_count = excluded.error_count;
            """;
        command.Parameters.AddWithValue("$trace_id", traceId);
        command.Parameters.AddWithValue("$primary_service", details.PrimaryService);
        command.Parameters.AddWithValue("$services_json", JsonSerializer.Serialize(details.Services, SerializerOptions));
        command.Parameters.AddWithValue("$root_span_name", details.RootSpanName);
        command.Parameters.AddWithValue("$start_time_utc", FormatDateTime(details.StartTime));
        command.Parameters.AddWithValue("$end_time_utc", FormatDateTime(details.EndTime));
        command.Parameters.AddWithValue("$last_updated_utc", FormatDateTime(DateTimeOffset.UtcNow));
        command.Parameters.AddWithValue("$span_count", spans.Count);
        command.Parameters.AddWithValue("$error_count", details.ErrorCount);
        command.ExecuteNonQuery();
    }

    private void EnforceTraceLimit(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var query = connection.CreateCommand();
        query.Transaction = transaction;
        query.CommandText =
            """
            SELECT trace_id
            FROM trace_info
            ORDER BY last_updated_utc ASC, trace_id ASC
            LIMIT (
                SELECT CASE
                    WHEN COUNT(*) > $limit THEN COUNT(*) - $limit
                    ELSE 0
                END
                FROM trace_info
            );
            """;
        query.Parameters.AddWithValue("$limit", options.MaxTraces);

        var traceIds = new List<string>();
        using (var reader = query.ExecuteReader())
        {
            while (reader.Read())
            {
                traceIds.Add(reader.GetString(0));
            }
        }

        foreach (var traceId in traceIds)
        {
            using var deleteSpans = connection.CreateCommand();
            deleteSpans.Transaction = transaction;
            deleteSpans.CommandText = "DELETE FROM spans WHERE trace_id = $trace_id;";
            deleteSpans.Parameters.AddWithValue("$trace_id", traceId);
            deleteSpans.ExecuteNonQuery();

            using var deleteTraceInfo = connection.CreateCommand();
            deleteTraceInfo.Transaction = transaction;
            deleteTraceInfo.CommandText = "DELETE FROM trace_info WHERE trace_id = $trace_id;";
            deleteTraceInfo.Parameters.AddWithValue("$trace_id", traceId);
            deleteTraceInfo.ExecuteNonQuery();
        }
    }

    private static void UpdateLastReceivedAt(SqliteConnection connection, SqliteTransaction transaction, DateTimeOffset timestamp)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            INSERT INTO metadata (key, value)
            VALUES ($key, $value)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value;
            """;
        command.Parameters.AddWithValue("$key", LastReceivedAtKey);
        command.Parameters.AddWithValue("$value", FormatDateTime(timestamp));
        command.ExecuteNonQuery();
    }

    private static List<SpanEntry> ReadTraceSpans(SqliteConnection connection, string traceId, SqliteTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT received_at_utc, resource_json, scope_name, trace_id, span_id, parent_span_id, name, kind, start_time_utc, end_time_utc, status_code, status_message, attributes_json
            FROM spans
            WHERE trace_id = $trace_id
            ORDER BY start_time_utc, id;
            """;
        command.Parameters.AddWithValue("$trace_id", traceId);

        var spans = new List<SpanEntry>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            spans.Add(new SpanEntry(
                ParseDateTime(reader.GetString(0)),
                DeserializeResource(reader.GetString(1)),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetString(6),
                reader.GetString(7),
                ParseDateTime(reader.GetString(8)),
                ParseDateTime(reader.GetString(9)),
                reader.GetString(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                DeserializeAttributes(reader.GetString(12))));
        }

        return spans;
    }

    private static TraceDetails ComputeTraceDetails(IReadOnlyList<SpanEntry> spans)
    {
        var orderedSpans = spans.OrderBy(span => span.StartTime).ToList();
        var spanIds = new HashSet<string>(orderedSpans.Select(span => span.SpanId), StringComparer.Ordinal);
        var rootSpan = orderedSpans.FirstOrDefault(span => string.IsNullOrEmpty(span.ParentSpanId) || !spanIds.Contains(span.ParentSpanId))
            ?? orderedSpans[0];

        var services = orderedSpans
            .Select(span => span.Resource.ServiceName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(serviceName => serviceName, StringComparer.Ordinal)
            .ToList();

        return new TraceDetails(
            rootSpan.Resource.ServiceName,
            services,
            rootSpan.Name,
            orderedSpans.Min(span => span.StartTime),
            orderedSpans.Max(span => span.EndTime),
            orderedSpans.Count(span => string.Equals(span.StatusCode, "ERROR", StringComparison.OrdinalIgnoreCase)));
    }

    private static string ResolveDatabasePath(string configuredPath, string contentRootPath)
    {
        if (Path.IsPathRooted(configuredPath))
        {
            return configuredPath;
        }

        return Path.GetFullPath(Path.Combine(contentRootPath, configuredPath));
    }

    private static string FormatDateTime(DateTimeOffset value) =>
        value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);

    private static DateTimeOffset ParseDateTime(string value) =>
        DateTimeOffset.ParseExact(value, "O", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);

    private static string SerializeAttributes(IReadOnlyList<KeyValueAttr> attributes) =>
        JsonSerializer.Serialize(attributes, SerializerOptions);

    private static List<KeyValueAttr> DeserializeAttributes(string json) =>
        JsonSerializer.Deserialize<List<KeyValueAttr>>(json, SerializerOptions) ?? [];

    private static ResourceInfo DeserializeResource(string json) =>
        new(DeserializeAttributes(json));

    private static List<string> DeserializeServiceNames(string json) =>
        JsonSerializer.Deserialize<List<string>>(json, SerializerOptions) ?? [];

    private sealed record MetricSeriesKey(string ServiceName, string MetricName);

    private sealed record MetricPointHeader(string? Description, string? Unit, MetricKind Kind);

    private sealed record TraceDetails(
        string PrimaryService,
        IReadOnlyList<string> Services,
        string RootSpanName,
        DateTimeOffset StartTime,
        DateTimeOffset EndTime,
        int ErrorCount);
}
