using System.Diagnostics;
using dse.services;
using Newtonsoft.Json;
using pubsub;

namespace EventProcessing;

public class FilterAndMapEventProcessor : SpPubSubEventHandler
{
    private int outputTopic;
    private PubsubCapabilities capabilities;
    private pubsub.StepRequest currentBatch;
    private int batchSize, numBatchedSteps = 0;

    public FilterAndMapEventProcessor(int outputTopic, int batchSize = 10)
    {
        this.outputTopic = outputTopic;
        this.batchSize = batchSize;
        currentBatch = new pubsub.StepRequest();
    }

    public async ValueTask HandleAsync(Event ev, CancellationToken token)
    {
        Debug.Assert(ev.Type == pubsub.DarqMessageType.In);
        if (ev.Data.Equals("termination"))
        {
            // Forward termination signal
            currentBatch.ConsumedMessageOffsets.Add(ev.Offset);
            currentBatch.OutMessages.Add(new OutMessage
            {
                TopicId = outputTopic,
                Event = ev.Data
            });
            await capabilities.Step(currentBatch);
            return;
        }
        
        var searchListItem =
            JsonConvert.DeserializeObject<SearchListJson>(ev.Data);
        Debug.Assert(searchListItem != null);
        currentBatch.ConsumedMessageOffsets.Add(ev.Offset);
        if (searchListItem.SearchTerm.Contains(SearchListStreamUtils.relevantSearchTerm))
        {
            var outputMessage =
                $"{SearchListStreamUtils.relevantSearchTerm} : {SearchListStreamUtils.GetRegionCode(searchListItem.IP)} : {searchListItem.Timestamp}";
            currentBatch.OutMessages.Add(new OutMessage
            {
                TopicId = outputTopic,
                Event = outputMessage
            });
        }
                
        // TODO(Tianyu): Need to flush final entries on clean shutdown?
        if (++numBatchedSteps == batchSize)
        {
            await capabilities.Step(currentBatch);
            currentBatch = new pubsub.StepRequest();
            numBatchedSteps = 0;
        }
    }

    public void OnRestart(PubsubCapabilities capabilities)
    {
        this.capabilities = capabilities;
        currentBatch = new pubsub.StepRequest();
        numBatchedSteps = 0;
    }
}