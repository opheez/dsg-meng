using FASTER.libdpr;
using pubsub;

namespace ExampleServices;


public interface SpPubSubEventHandler
{ 
    Task HandleAsync(Event ev, CancellationToken token);

    void OnRestart(IDarqProcessorClientCapabilities capabilities);
}

public class SpPubSubProcessorClient
{
    private int topicId;
    private SpPubSubServiceClient client;
    private ManualResetEventSlim terminationStart, terminationComplete;

    public SpPubSubProcessorClient(int topicId, SpPubSubServiceClient client)
    {
        this.topicId = topicId;
        this.client = client;
    }
    
    public async Task StartProcessingAsync(SpPubSubEventHandler handler, bool speculative = true, CancellationToken token = default)
    {
        terminationStart = new ManualResetEventSlim();
            while (!terminationStart.IsSet && !token.IsCancellationRequested)
            {
                var session = speculative ? new DprSession() : null;
                var stream = client.ReadEventsFromTopic(new ReadEventsRequest
                {
                    Speculative = speculative,
                    TopicId = topicId
                }, session, cancellationToken: token);
                try
                {
                    while (!terminationStart.IsSet && await stream.ResponseStream.MoveNext(token))
                        await handler.HandleAsync(stream.ResponseStream.Current, token);
                }
                catch (DprSessionRolledBackException e)
                {
                    // Just continue and restart the stream from where it's supposed to
                    continue;
                }
                catch (TaskCanceledException e)
                {
                    break;
                }
            }
            terminationComplete?.Set();
    }

    public async Task StopProcessingAsync()
    {
        terminationComplete = new ManualResetEventSlim();
        terminationStart.Set();
        while (!terminationComplete.IsSet)
            await Task.Delay(5);
    }
    
}