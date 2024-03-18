using Azure.Storage.Blobs;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using protobuf;

namespace ExampleServices;

// Writes nothing by itself but can persist arbitrary (small) things through metadata and IStateObjectAttachments
public class SpeculativeMetadataAzureBlob : DprWorker
{
    private BlobContainerClient client;
    private string baseName;
    private SimpleObjectPool<byte[]> serializationBufferPool;

    public SpeculativeMetadataAzureBlob(BlobContainerClient client, string baseName, IVersionScheme versionScheme,
        DprWorkerOptions options) : base(versionScheme, options)
    {
        this.client = client;
        this.baseName = baseName;
        serializationBufferPool = new SimpleObjectPool<byte[]>(() => new byte[1 << 24]);
    }

    public override void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
    {
        var buf = serializationBufferPool.Checkout();
        var head = 0;
        BitConverter.TryWriteBytes(buf, metadata.Length);
        head += sizeof(int);
        metadata.CopyTo(new Span<byte>(buf, head, buf.Length - head));
        head += metadata.Length;

        var blobClient = client.GetBlobClient($"{baseName}.{version}");
        Task.Run(async () =>
        {
            await blobClient.UploadAsync(new BinaryData(new ReadOnlyMemory<byte>(buf, 0, head)));
            serializationBufferPool.Return(buf);
            onPersist();
        });
    }

    public override void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        var blobClient = client.GetBlobClient($"{baseName}.{version}");
        using var ms = new MemoryStream();
        blobClient.DownloadTo(ms);
        var bytes = ms.ToArray();
        var head = 0;
        var metadataSize = BitConverter.ToInt32(new ReadOnlySpan<byte>(bytes, 0, bytes.Length));
        head += sizeof(int);
        metadata = new ReadOnlySpan<byte>(bytes, head, metadataSize);
    }

    public override void PruneVersion(long version)
    {
        var blobClient = client.GetBlobClient($"{baseName}.{version}");
        blobClient.DeleteIfExists();
    }

    public override IEnumerable<Memory<byte>> GetUnprunedVersions()
    {
        var unprunedVersions = new List<byte[]>();
        foreach (var item in client.GetBlobsByHierarchy())
        {
            var version = int.Parse(item.Blob.Name[(item.Blob.Name.LastIndexOf('.') + 1)..]);
            RestoreCheckpoint(version, out var span);
            unprunedVersions.Add(span.ToArray());
        }

        return unprunedVersions.Select(a => new Memory<byte>(a));
    }

    public override void Dispose()
    {
    }
}


public class SpPubSubProcessorClient
{
    private string topicId;
    private SpPubSubServiceClient client;
    private SpeculativeMetadataAzureBlob storage;
    private LongValueAttachment processingOffset;
    private ManualResetEventSlim terminationStart;
    private CountdownEvent terminationComplete;

    public SpPubSubProcessorClient(string topicId, SpPubSubServiceClient client, SpeculativeMetadataAzureBlob storage)
    {
        this.topicId = topicId;
        this.client = client;
        this.storage = storage;
        processingOffset = new LongValueAttachment();
        storage.AddAttachment(processingOffset);
    }
    
    public struct EventProcessingArgs
    {
        public readonly Event data;
        private SpPubSubProcessorClient parent;
        private bool speculative;

        internal EventProcessingArgs(Event data, SpPubSubProcessorClient parent, bool speculative)
        {
            this.data = data;
            this.parent = parent;
        }

        public ValueTask UpdateCheckpoint()
        {
            parent.processingOffset.value = data.NextOffset;
            return speculative ? ValueTask.CompletedTask : new ValueTask(parent.storage.NextCommit());
        }

        public DprSession Detach() => parent.storage.DetachFromWorker();

        public bool Merge(DprSession session) => parent.storage.TryMergeAndStartAction(session);
    }
    
    public delegate Task EventHandler(EventProcessingArgs ev, CancellationToken token);
    
    public async Task StartProcessingAsync(EventHandler handler, Action<long> onRollback = null, bool speculative = true, CancellationToken token = default)
    {
        terminationStart = new ManualResetEventSlim();
        storage.ConnectToCluster();
        
        Task.Run(async () =>
        {
            while (!terminationStart.IsSet && !token.IsCancellationRequested)
            {
                storage.Refresh();
                try
                {
                    await Task.Delay(
                        (int)Math.Min(storage.options.CheckpointPeriodMilli, storage.options.RefreshPeriodMilli),
                        token);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }

            terminationComplete?.Signal();
        }); 
        
        if (speculative)
        {
            while (!terminationStart.IsSet && !token.IsCancellationRequested)
            {
                var session = new DprSession();
                var stream = client.ReadEventsFromTopic(new ReadEventsRequest
                {
                    Speculative = true,
                    TopicId = topicId,
                    StartLsn = processingOffset.value,
                }, session, cancellationToken: token);
                try
                {
                    while (!terminationStart.IsSet && await stream.ResponseStream.MoveNext(token))
                    {
                        var ev = stream.ResponseStream.Current;
                        if (!storage.TrySynchronizeAndStartAction(session))
                            // This means the session is behind the storage, which can only mean that it needs to
                            // be rolled back
                            throw new DprSessionRolledBackException(storage.WorldLine());
                        await handler(new EventProcessingArgs(ev, this, true), token);
                    }
                }
                catch (DprSessionRolledBackException e)
                {
                    storage.ForceRefresh();
                    while (storage.WorldLine() < e.NewWorldLine)
                        await Task.Delay(5);
                    onRollback?.Invoke(processingOffset.value);
                }
                catch (TaskCanceledException e)
                {
                    break;
                }
            }
            terminationComplete?.Signal();

        }
        else
        {
            var stream = client.ReadEventsFromTopic(new ReadEventsRequest
            {
                Speculative = false,
                TopicId = topicId,
                StartLsn = processingOffset.value,
            }, cancellationToken: token);
            try
            {
                while (!terminationStart.IsSet && !token.IsCancellationRequested &&
                       await stream.ResponseStream.MoveNext(token))
                {
                    var ev = stream.ResponseStream.Current;
                    await handler(new EventProcessingArgs(ev, this, false), token);
                }
            }
            catch (TaskCanceledException) {}
            terminationComplete?.Signal();
        }
    }

    public async Task StopProcessingAsync()
    {
        terminationComplete = new CountdownEvent(2);
        terminationStart.Set();
        while (!terminationComplete.IsSet)
            await Task.Delay(5);
    }
    
}