using System.Text;
using dse.services;
using Google.Protobuf;
using pubsub;
namespace EventProcessing;

public class AnomalyDetectionEventProcessor : SpPubSubEventHandler
{
    private int outputTopic;
    private PubsubCapabilities capabilities;
    private Dictionary<string, int> state;
    private StepRequest currentStep;
    private int batchedCount;
    private Random random;
    private double sampleRate;
    
    public AnomalyDetectionEventProcessor(int outputTopic, double sampleRate)
    {
        this.outputTopic = outputTopic;
        state = new Dictionary<string, int>();
        currentStep = new StepRequest
        {
            TopicId = outputTopic
        };
        random = new Random();
        this.sampleRate = sampleRate;
    }

    public async ValueTask HandleAsync(Event ev, CancellationToken token)
    {
        switch (ev.Type)
        {
            case DarqMessageType.In:
            {
                if (ev.Data.Equals("termination"))
                {
                    currentStep.ConsumedMessageOffsets.Add(ev.Offset);
                    CheckpointCurrentState();
                    currentStep.OutMessages.Add(new OutMessage
                    {
                        TopicId = outputTopic,
                        Event = ev.Data
                    });
                    await capabilities.Step(currentStep);
                    return;
                }
                // Console.WriteLine($"Received Message: {message}");
                var split = ev.Data.Split(":");
                var key = split[0];
                var count = long.Parse(split[1]);
                var prevHash = state.GetValueOrDefault(key, 0);
                state[key] = (31 * count + prevHash).GetHashCode();
                currentStep.ConsumedMessageOffsets.Add(ev.Offset);
                
                if (random.NextDouble() < sampleRate)
                {
                    CheckpointCurrentState();
                    currentStep.OutMessages.Add(new OutMessage
                    {
                        TopicId = outputTopic,
                        Event = ev.Data
                    });
                    await capabilities.Step(currentStep);
                    currentStep = new StepRequest();
                    batchedCount = 0;
                }
                else if (batchedCount == 100)
                {
                    CheckpointCurrentState();
                    await capabilities.Step(currentStep);
                    currentStep = new StepRequest();
                    batchedCount = 0;
                }
                return;
            }
            case DarqMessageType.Recovery:
            {
                var split = ev.Data.Split(":");
                state[split[0]] = int.Parse(split[1]);
                return;
            }
            default:
                throw new NotImplementedException();
        }
    }

    private void CheckpointCurrentState()
    {
        // TODO(Tianyu): Need some API to easily GC recovery messages
        foreach (var entry in state)
            currentStep.RecoveryMessages.Add(ByteString.CopyFrom($"{entry.Key}:{entry.Value}", Encoding.UTF8));
    }

    public void OnRestart(PubsubCapabilities capabilities)
    {
        this.capabilities = capabilities;
        state = new Dictionary<string, int>();
        currentStep = new StepRequest
        {
            TopicId = outputTopic
        };
    }
}