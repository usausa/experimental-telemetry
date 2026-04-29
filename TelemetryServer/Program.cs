using MudBlazor.Services;
using TelemetryServer.Components;
using TelemetryServer.Telemetry.DummyData;
using TelemetryServer.Telemetry.Services;
using TelemetryServer.Telemetry.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMudServices();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddGrpc();

var storeOptions = builder.Configuration.GetSection("TelemetryStore").Get<TelemetryStoreOptions>() ?? new TelemetryStoreOptions();
builder.Services.AddSingleton(storeOptions);
builder.Services.AddSingleton<ITelemetryStore, SqliteTelemetryStore>();
builder.Services.AddSingleton<DummyDataGenerator>();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
}
app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);

app.UseAntiforgery();

app.MapStaticAssets();

app.MapGrpcService<OtlpMetricsService>();
app.MapGrpcService<OtlpLogsService>();
app.MapGrpcService<OtlpTraceService>();

app.MapGet("/", context =>
{
    context.Response.Redirect("/dashboard");
    return Task.CompletedTask;
}).ExcludeFromDescription();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
