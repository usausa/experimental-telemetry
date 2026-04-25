namespace TelemetryServer.Telemetry.Models;

public sealed record ResourceInfo(IReadOnlyList<KeyValueAttr> Attributes)
{
    public string? GetAttribute(string key) =>
        Attributes.FirstOrDefault(a => a.Key == key)?.Value;

    public string ServiceName => GetAttribute("service.name") ?? "unknown";
}
