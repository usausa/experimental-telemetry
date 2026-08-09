# experimental-telemetry

OTLP/gRPC でテレメトリを受信して SQLite に蓄積し、Blazor で可視化するサーバーです。

## 設定 (`appsettings.json`)

```json
{
  "TelemetryStore": {
    "DatabasePath": "App_Data/telemetry.db",
    "RetentionDays": 7,
    "MaxPointsPerMetricSeries": 500,
    "MaxLogsPerService": 2000,
    "MaxTraces": 500,
    "MaxSpansPerTrace": 200
  }
}
```

| キー | 既定値 | 説明 |
|---|---|---|
| `DatabasePath` | `App_Data/telemetry.db` | SQLite ファイルのパス。相対パスはコンテンツルート基準 |
| `RetentionDays` | `7` | この日数より古い行を 1 時間ごとに削除します。`0` で無効 |
| `MaxPointsPerMetricSeries` | `500` | メトリクス 1 系列（サービス × メトリクス名）あたりの保持点数 |
| `MaxLogsPerService` | `2000` | サービスあたりのログ保持件数 |
| `MaxTraces` | `500` | 保持するトレース数 |
| `MaxSpansPerTrace` | `200` | トレースあたりのスパン保持件数 |

`Max*` は受信のたびに適用される件数上限です。ただし系列やサービスの**種類**の数には上限がないため、
これだけではディスク使用量は決まりません。`RetentionDays` による期間での削除を併用してください。
`RetentionDays` を `0` にすると期間での削除は行われず、容量は運用側の責任になります。

## エンドポイント

| 用途 | 既定 |
|---|---|
| OTLP/gRPC 受信 | `http://0.0.0.0:4317` |
| ダッシュボード | `http://0.0.0.0:5080/dashboard` |
