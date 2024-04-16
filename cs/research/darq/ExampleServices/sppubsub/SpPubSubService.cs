using System.Collections.Concurrent;
using System.Text;
using Consul;
using darq;
using darq.client;
using FASTER.client;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json.Linq;
using pubsub;
using DarqMessageType = FASTER.libdpr.DarqMessageType;
using DarqStepStatus = pubsub.DarqStepStatus;
using Event = pubsub.Event;
using RegisterProcessorRequest = pubsub.RegisterProcessorRequest;
using RegisterProcessorResult = pubsub.RegisterProcessorResult;
using Status = Grpc.Core.Status;
using StepRequest = pubsub.StepRequest;

namespace dse.services;

internal struct EventDataAdapter : ILogEnqueueEntry
{
    internal string data;
    public int SerializedLength => sizeof(DarqMessageType) + sizeof(long) + sizeof(int) + data.Length;

    public unsafe void SerializeTo(Span<byte> dest)
    {
        fixed (byte* h = dest)
        {
            var head = h;
            *(DarqMessageType*)head = DarqMessageType.IN;
            head += sizeof(DarqMessageType);
            *(long*)head = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            head += sizeof(long);
            *(int*)head = data.Length;
            head += sizeof(int);
            Encoding.UTF8.GetBytes(data, new Span<byte>(head, data.Length));
        }
    }
}

public class PubsubDarqProducer : IDarqProducer
{
    private SpPubSubServiceClient client;
    private DprSession session;

    public PubsubDarqProducer(DprSession session, ConsulClient consul)
    {
        client = new SpPubSubServiceClient(consul);
        this.session = session;
    }

    public void Dispose()
    {
    }

    public void EnqueueMessageWithCallback(DarqId darqId, ReadOnlySpan<byte> message, Action<bool> callback,
        long producerId, long lsn)
    {
        var enqueueRequest = new EnqueueRequest
        {
            ProducerId = producerId,
            SequenceNum = lsn,
            TopicId = (int)darqId.guid
        };
        enqueueRequest.Events.Add(Encoding.UTF8.GetString(message));
        Task.Run(async () =>
        {
            await client.EnqueueEventsAsync(enqueueRequest, session);
            callback(true);
        });
    }

    public void ForceFlush()
    {
        throw new NotImplementedException();
    }
}

public class SpPubSubServiceSettings
{
    public ConsulClientConfiguration consulConfig;
    public Func<int, DprWorkerId, Darq> factory;
    public string hostId;
}

public class SpPubSubService : SpPubSub.SpPubSubBase
{
    private ConcurrentDictionary<int, Darq> topics;
    private string hostId;
    private SpPubSubServiceSettings settings;
    private ConsulClient consul;
    private Func<int, DprWorkerId, Darq> factory;
    private ThreadLocalObjectPool<FASTER.libdpr.StepRequest> stepRequestPool;
    private int requestId;

    private DarqMaintenanceBackgroundService maintenanceService;
    private StateObjectRefreshBackgroundService refreshService;


    public SpPubSubService(SpPubSubServiceSettings settings, DarqMaintenanceBackgroundService maintenanceService,
        StateObjectRefreshBackgroundService refreshService)
    {
        this.settings = settings;
        topics = new ConcurrentDictionary<int, Darq>();
        stepRequestPool = new ThreadLocalObjectPool<FASTER.libdpr.StepRequest>(() => new FASTER.libdpr.StepRequest());
        consul = new ConsulClient(settings.consulConfig);
        factory = settings.factory;
        hostId = settings.hostId;
        this.maintenanceService = maintenanceService;
        this.refreshService = refreshService;
    }

    private async Task<Darq> GetOrCreateTopic(int topicId)
    {
        if (topics.TryGetValue(topicId, out var entry)) return entry;
        // Otherwise, check if topic has been created in manager
        var queryResult = await consul.KV.Get("topic-" + topicId);
        if (queryResult.Response == null)
            throw new RpcException(new Status(StatusCode.NotFound, "requested topic does not exist"));

        var metadataEntry = JObject.Parse(Encoding.UTF8.GetString(queryResult.Response.Value));

        if (((string)metadataEntry["hostId"])!.Equals(hostId))
            throw new RpcException(new Status(StatusCode.NotFound, "requested topic is not assigned to this host"));

        lock (this)
        {
            if (topics.TryGetValue(topicId, out entry)) return entry;
            var dprWorkerId = new DprWorkerId((long)metadataEntry["dprWorkerId"]!);
            var result = factory(topicId, dprWorkerId);
            result.ConnectToCluster(out _);
            maintenanceService.RegisterMaintenanceTask(result, new DarqMaintenanceBackgroundServiceSettings
            {
                morselSize = 512,
                batchSize = 16,
                producerFactory = session => new PubsubDarqProducer(session, new ConsulClient(settings.consulConfig))
            });
            refreshService.RegisterRefreshTask(result);
            topics[topicId] = result;
            return result;
        }
    }

    public override async Task<EnqueueResult> EnqueueEvents(EnqueueRequest request, ServerCallContext context)
    {
        // TODO(Tianyu): Create Epoch Context
        var topic = await GetOrCreateTopic(request.TopicId);
        if (request.DprHeader != null)
        {
            // Speculative code path
            if (!topic.TryReceiveAndStartAction(request.DprHeader.Span))
                // Use an error to signal to caller that this call cannot proceed
                // TODO(Tianyu): add more descriptive exception information
                throw new RpcException(Status.DefaultCancelled);
            var result = new EnqueueResult
            {
                Ok = topic.Enqueue(request.Events.Select(e => new EventDataAdapter { data = e }),
                    request.ProducerId, request.SequenceNum)
            };
            unsafe
            {
                var dprHeaderBytes = stackalloc byte[DprMessageHeader.FixedLenSize];
                topic.ProduceTagAndEndAction(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize));
                result.DprHeader = ByteString.CopyFrom(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize));
            }

            return result;
        }
        else
        {
            topic.StartLocalAction();
            var result = new EnqueueResult
            {
                Ok = topic.Enqueue(request.Events.Select(e => new EventDataAdapter { data = e }),
                    request.ProducerId, request.SequenceNum)
            };
            topic.EndAction();
            // TODO(Tianyu): Allow custom version headers to avoid waiting on, say, a read into a committed value
            await topic.NextCommit();
            return result;
        }
    }

    private unsafe bool TryReadOneEntry(Darq topic, long worldLine, DarqScanIterator scanner,
        LightEpoch.EpochContext context, out Event ev)
    {
        ev = default;
        var dprHeaderBytes = stackalloc byte[DprMessageHeader.FixedLenSize];
        try
        {
            topic.StartLocalAction(context);
            if (topic.WorldLine() != worldLine)
                throw new DprSessionRolledBackException(topic.WorldLine());

            if (!scanner.UnsafeGetNext(out var b, out var length, out var offset, out var nextOffset, out var type))
                return false;

            if (type is not (DarqMessageType.IN or DarqMessageType.RECOVERY))
            {
                scanner.UnsafeRelease();
                return false;
            }

            ev = new Event
            {
                Type = type switch
                {
                    DarqMessageType.IN => pubsub.DarqMessageType.In,
                    DarqMessageType.RECOVERY => pubsub.DarqMessageType.Recovery,
                    _ => throw new ArgumentOutOfRangeException()
                },
                Data = Encoding.UTF8.GetString(b + sizeof(long) + sizeof(int), *(int*)(b + sizeof(long))),
                Timestamp = *(long*)b,
                Offset = offset,
                NextOffset = nextOffset
            };
            scanner.UnsafeRelease();
            return true;
        }
        finally
        {
            topic.ProduceTagAndEndAction(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize), context);
            if (ev != default)
                ev.DprHeader = ByteString.CopyFrom(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize));
        }
    }

    public override async Task ReadEventsFromTopic(ReadEventsRequest request, IServerStreamWriter<Event> responseStream,
        ServerCallContext context)
    {
        var topic = await GetOrCreateTopic(request.TopicId);
        var worldLine = topic.WorldLine();
        var scanner = topic.StartScan(request.Speculative);

        // TODO(Tianyu): We could burn through ints if these are long-running and wrap around, which can be a problem
        // Get a id that is almost guaranteed to be unique for the runtime of the RPC for use in epoch as context
        var epochContext = new LightEpoch.EpochContext
        {
            customId = Interlocked.Increment(ref requestId)
        };

        while (!context.CancellationToken.IsCancellationRequested)
        {
            if (TryReadOneEntry(topic, worldLine, scanner, epochContext, out var ev))
            {
                // TODO(Tianyu): avoid awaiting multiple times
                if (!request.Speculative)
                    await topic.NextCommit();
                // TODO(Tianyu): Should not await?
                await responseStream.WriteAsync(ev);
            }
            else
            {
                await scanner.WaitAsync(context.CancellationToken);
            }
        }
    }

    public override async Task<RegisterProcessorResult> RegisterProcessor(RegisterProcessorRequest request,
        ServerCallContext context)
    {
        var topic = await GetOrCreateTopic(request.TopicId);
        var result = await topic.RegisterNewProcessorAsync();
        return new RegisterProcessorResult
        {
            IncarnationId = result
        };
    }

    public override async Task<StepResult> Step(StepRequest request, ServerCallContext context)
    {
        var topic = await GetOrCreateTopic(request.TopicId);
        // Get a id that is almost guaranteed to be unique for the runtime of the RPC for use in epoch as context
        var epochContext = new LightEpoch.EpochContext
        {
            customId = Interlocked.Increment(ref requestId)
        };
        var requestObject = stepRequestPool.Checkout();
        var requestBuilder = new StepRequestBuilder(requestObject);
        foreach (var consumed in request.ConsumedMessageOffsets)
            requestBuilder.MarkMessageConsumed(consumed);
        foreach (var self in request.RecoveryMessages)
            requestBuilder.AddRecoveryMessage(self.Span);
        foreach (var outBatch in request.OutMessages)
        {
            if (outBatch.TopicId == request.TopicId)
                requestBuilder.AddSelfMessage(outBatch.Event);
            else
                requestBuilder.AddOutMessage(new DarqId(outBatch.TopicId), outBatch.Event);
        }

        if (request.DprHeader != null)
        {
            // Speculative code path
            if (!topic.TryReceiveAndStartAction(request.DprHeader.Span, epochContext))
                // Use an error to signal to caller that this call cannot proceed
                // TODO(Tianyu): add more descriptive exception information
                throw new RpcException(Status.DefaultCancelled);
            var status = topic.Step(request.IncarnationId, requestBuilder.FinishStep());
            var result = new StepResult
            {
                Status = status switch
                {
                    // Should never happen
                    StepStatus.INCOMPLETE => throw new NotImplementedException(),
                    StepStatus.SUCCESS => DarqStepStatus.Success,
                    StepStatus.INVALID => DarqStepStatus.Invalid,
                    StepStatus.REINCARNATED => DarqStepStatus.Reincarnated,
                    _ => throw new ArgumentOutOfRangeException()
                }
            };
            unsafe
            {
                var dprHeaderBytes = stackalloc byte[DprMessageHeader.FixedLenSize];
                topic.ProduceTagAndEndAction(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize),
                    epochContext);
                result.DprHeader = ByteString.CopyFrom(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize));
            }

            return result;
        }
        else
        {
            topic.StartLocalAction(epochContext);
            var status = topic.Step(request.IncarnationId, requestBuilder.FinishStep());
            var result = new StepResult
            {
                Status = status switch
                {
                    // Should never happen
                    StepStatus.INCOMPLETE => throw new NotImplementedException(),
                    StepStatus.SUCCESS => DarqStepStatus.Success,
                    StepStatus.INVALID => DarqStepStatus.Invalid,
                    StepStatus.REINCARNATED => DarqStepStatus.Reincarnated,
                    _ => throw new ArgumentOutOfRangeException()
                }
            };
            topic.EndAction(epochContext);
            await topic.NextCommit();
            return result;
        }
    }
}