using ExampleServices;
using pubsub;

namespace EventProcessing;

public class SearchListLatencyMeasurementProcessor : SpPubSubEventHandler
{
    public Dictionary<string, (long, long)> results = new();
    public TaskCompletionSource workloadTerminationed = new TaskCompletionSource();

    public ValueTask HandleAsync(Event ev, CancellationToken token)
    {
        if (ev.Data.Equals("termination"))
        {
            workloadTerminationed.SetResult();
            return ValueTask.CompletedTask;
        }
        var split = ev.Data.Split(":");
        var timestamp = long.Parse(split[2]);
        var endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
        results[ev.Data] = (timestamp, endTime);
        Console.WriteLine($"Received {ev.Data}, {timestamp}, {endTime}");
        return ValueTask.CompletedTask;
    }

    public void OnRestart(PubsubCapabilities capabilities)
    {
    }
}