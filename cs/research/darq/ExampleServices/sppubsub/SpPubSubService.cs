using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Consul;
using ExampleServices.splog;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Newtonsoft.Json.Linq;
using protobuf;
using Status = Grpc.Core.Status;

namespace ExampleServices;

public struct SpPubSubTopic
{
    public SpeculativeLog worker;
    public LongValueAttachment seqNumCounter;
}

public class SpPubSubService : SpPubSub.SpPubSubBase
{
    private ConcurrentDictionary<string, SpPubSubTopic> topics;
    private ThreadLocalObjectPool<byte[]> serializationBufferPool;
    private string hostId;
    private ConsulClient consul;
    private Func<string, DprWorkerId, SpeculativeLog> factory;

    public SpPubSubService(ConsulClient consul, Func<string, DprWorkerId, SpeculativeLog> factory, string hostId)
    {
        this.consul = consul;
        this.factory = factory;
        serializationBufferPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 20]);
        topics = new ConcurrentDictionary<string, SpPubSubTopic>();
        this.hostId = hostId;
    }

    private async Task<SpPubSubTopic> GetOrCreateTopic(string topicId)
    {
        if (topics.TryGetValue(topicId, out var topic)) return topic;
        // Otherwise, check if topic has been created in manager
        var queryResult = await consul.KV.Get("topic-" + topicId);
        if (queryResult.Response == null)
            throw new RpcException(new Status(StatusCode.NotFound, "requested topic does not exist"));

        var metadataEntry = JObject.Parse(Encoding.UTF8.GetString(queryResult.Response.Value));

        if (((string)metadataEntry["hostId"])!.Equals(hostId))
            throw new RpcException(new Status(StatusCode.NotFound, "requested topic is not assigned to this host"));

        lock (this)
        {
            if (topics.TryGetValue(topicId, out topic)) return topic;
            var dprWorkerId = new DprWorkerId((long)metadataEntry["dprWorkerId"]!);
            var result = new SpPubSubTopic
            {
                worker = factory(topicId, dprWorkerId),
                seqNumCounter = new LongValueAttachment()
            };
            topics[topicId] = result;
            return result;
        }
    }

    public override async Task<EnqueueEventsResponse> EnqueueEvents(EventDataBatch request, ServerCallContext context)
    {
        var topic = await GetOrCreateTopic(request.TopicId);
        if (request.DprHeader != null)
        {
            // Speculative code path
            if (!topic.worker.TryReceiveAndStartAction(request.DprHeader.Span))
                // Use an error to signal to caller that this call cannot proceed
                // TODO(Tianyu): add more descriptive exception information
                throw new RpcException(Status.DefaultCancelled);
            var result = EnqueueEvents(topic, request);
            var buf = serializationBufferPool.Checkout();
            topic.worker.ProduceTagAndEndAction(buf);
            context.ResponseTrailers.Add(DprMessageHeader.GprcMetadataKeyName, buf);
            serializationBufferPool.Return(buf);
            return result;
        }
        else
        {
            topic.worker.StartLocalAction();
            var result = EnqueueEvents(topic, request);
            topic.worker.EndAction();
            // TODO(Tianyu): Allow custom version headers to avoid waiting on, say, a read into a committed value
            await topic.worker.NextCommit();
            return result;
        }
    }

    private unsafe long SerializeEvent(long seqNum, EventData e, byte[] buffer)
    {
        fixed (byte* h = buffer)
        {
            var head = h;
            *(long*)head = seqNum;
            head += sizeof(long);
            *(long*)head = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            head += sizeof(long);
            *(int*)head = e.MessageId.Length;
            head += sizeof(int);
            Encoding.UTF8.GetBytes(e.MessageId, new Span<byte>(head, (int)(buffer.Length - (head - h))));
            head += e.MessageId.Length;
            e.Body.Span.CopyTo(new Span<byte>(head, (int)(buffer.Length - (head - h))));
            head += e.Body.Span.Length;
            return head - h;
        }
    }

    private EnqueueEventsResponse EnqueueEvents(SpPubSubTopic topic, EventDataBatch request)
    {
        var serializationBuffer = serializationBufferPool.Checkout();
        topic.seqNumCounter.value++;

        foreach (var e in request.Events)
        {
            var length = SerializeEvent(topic.seqNumCounter.value++, e, serializationBuffer);
            topic.worker.log.Enqueue(new Span<byte>(serializationBuffer, 0, (int)length));
        }

        serializationBufferPool.Return(serializationBuffer);
        return new EnqueueEventsResponse
        {
            Ok = true
        };
    }

    public override async Task ReadEventsFromTopic(ReadEventsRequest request,
        IServerStreamWriter<protobuf.Event> responseStream, ServerCallContext context)
    {
        var topic = await GetOrCreateTopic(request.TopicId);
        if (request.Speculative)
        {
            var scanner = topic.worker.log.Scan(request.StartLsn, long.MaxValue, recover: false, scanUncommitted: true);
            while (!context.CancellationToken.IsCancellationRequested)
            {
                topic.worker.StartLocalAction();
                var result = await ReadEventFromTopic(topic, scanner, context.CancellationToken);
                var buf = serializationBufferPool.Checkout();
                var size = topic.worker.ProduceTagAndEndAction(buf);
                result.DprHeader =  ByteString.CopyFrom(new Span<byte>(buf, 0, size));
                serializationBufferPool.Return(buf);
                // TODO(Tianyu): unnecessary await?
                await responseStream.WriteAsync(result);
            }
        }
        else
        {
            var scanner = topic.worker.log.Scan(request.StartLsn, long.MaxValue, recover: false, scanUncommitted: false);
            while (!context.CancellationToken.IsCancellationRequested)
            {
                topic.worker.StartLocalAction();
                var result = await ReadEventFromTopic(topic, scanner, context.CancellationToken);
                // TODO(Tianyu): Still need to wait for DPR commit before exposing
                topic.worker.EndAction();
                // TODO(Tianyu): unnecessary await?
                await responseStream.WriteAsync(result);
            }
        }
    }
    
    private unsafe bool TryReadOneEntry(FasterLogScanIterator iterator, protobuf.Event response)
    {
        // TODO(Tianyu): Cache in memory to save serialization for hot entries?
        if (!iterator.UnsafeGetNext(out var entry, out var entryLength, out var offset, out var nextAddress))
            return false;
        response.Offset = offset;
        response.NextOffset = nextAddress;

        var head = entry;

        response.SequenceNum = *(long*)head;
        head += sizeof(long);

        response.Timestamp = *(long*)head;
        head += sizeof(long);


        var messageIdLength = *(int*)head;
        head += sizeof(int);
        response.MessageId = Encoding.UTF8.GetString(new Span<byte>(head, messageIdLength));
        head += messageIdLength;

        response.Body = ByteString.CopyFrom(new ReadOnlySpan<byte>(head, (int)(entryLength - (head - entry))));
        iterator.UnsafeRelease();
        return true;
    }

    private async Task<protobuf.Event> ReadEventFromTopic(SpPubSubTopic topic, FasterLogScanIterator scanner, CancellationToken token)
    {
        var responseObject = new protobuf.Event();
        if (TryReadOneEntry(scanner, responseObject)) return responseObject;
        var nextEntry = scanner.WaitAsync(token).AsTask();
        var session = topic.worker.DetachFromWorker();
        await nextEntry;
        if (!topic.worker.TryMergeAndStartAction(session))
            throw new RpcException(Status.DefaultCancelled);
        var read = TryReadOneEntry(scanner, responseObject);
        Debug.Assert(read);
        return responseObject;
    }
}