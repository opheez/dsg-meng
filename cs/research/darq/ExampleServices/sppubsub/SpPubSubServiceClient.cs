using System.Diagnostics;
using System.Text;
using Consul;
using FASTER.common;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using protobuf;

namespace ExampleServices;

public class SpPubSubServiceClient
{
    private ConsulClient consul;
    private Dictionary<string, GrpcChannel> openConnections;
    private ThreadLocalObjectPool<byte[]> serializationBufferPool;

    public SpPubSubServiceClient(ConsulClient consul)
    {
        this.consul = consul;
        openConnections = new Dictionary<string, GrpcChannel>();
        serializationBufferPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 20]);
    }

    private async Task<GrpcChannel> GetOrCreateConnection(string topicId)
    {
        if (openConnections.TryGetValue(topicId, out var result)) return result;
        var queryResult = await consul.KV.Get("topic-" + topicId);
        if (queryResult.Response == null)
            throw new NotImplementedException("Topic does not exist");

        var metadataEntry = JObject.Parse(Encoding.UTF8.GetString(queryResult.Response.Value));
        return openConnections[topicId] = GrpcChannel.ForAddress((string)metadataEntry["hostAddress"]);
    }

    public async Task<bool> CreateTopic(string topicId, string hostId, string hostAddress, DprWorkerId id)
    {
        var jsonEntry = JsonConvert.SerializeObject(new
            { hostId = hostId, hostAddress = hostAddress, dprWorkerId = id.guid });
        return (await consul.KV.CAS(new KVPair("topic-" + topicId)
        {
            Value = Encoding.UTF8.GetBytes(jsonEntry)
        })).Response;
    }

    public async Task<EnqueueEventsResponse> EnqueueEventsAsync(EventDataBatch batch, DprSession session = null)
    {
        var channel = await GetOrCreateConnection(batch.TopicId);
        if (session != null)
        {
            var buf = serializationBufferPool.Checkout();
            var size = session.TagMessage(buf);
            batch.DprHeader = ByteString.CopyFrom(new Span<byte>(buf, 0, size));
        }

        var client = new SpPubSub.SpPubSubClient(channel);
        var result = await client.EnqueueEventsAsync(batch);
        if (session == null || session.Receive(result.DprHeader.Span))
            return result;
        throw new TaskCanceledException();
    }

    private class StreamingCallInterceptor : Interceptor
    {
        private DprSession session;

        public StreamingCallInterceptor(DprSession session)
        {
            this.session = session;
        }

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        {
            var originalCall = continuation(request, context);

            var responseStream = new DprHandlingStream<TResponse>(originalCall.ResponseStream, session);

            // Return a new AsyncServerStreamingCall with our custom response stream
            return new AsyncServerStreamingCall<TResponse>(
                responseStream,
                originalCall.ResponseHeadersAsync,
                originalCall.GetStatus,
                originalCall.GetTrailers,
                originalCall.Dispose);
        }
    }

    public class DprHandlingStream<T> : IAsyncStreamReader<T>
    {
        private readonly IAsyncStreamReader<T> inner;
        private readonly DprSession session;

        public DprHandlingStream(IAsyncStreamReader<T> inner, DprSession session)
        {
            this.inner = inner;
            this.session = session;
        }

        public T Current => inner.Current;

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            var hasNext = await inner.MoveNext(cancellationToken);
            if (!hasNext) return false;
            
            var batch = inner.Current as EventDataBatch;
            Debug.Assert(batch != null);
            if (!session.Receive(batch.DprHeader.Span))
                throw new TaskCanceledException();
            return true;
        }
    }

    public AsyncServerStreamingCall<protobuf.Event> ReadEventsFromTopic(ReadEventsRequest request,
        DprSession session = null, DateTime? deadline = null, CancellationToken cancellationToken = default)
    {
        var channel = GetOrCreateConnection(request.TopicId).GetAwaiter().GetResult();
        if (session == null)
        {
            var client = new SpPubSub.SpPubSubClient(channel);
            request.Speculative = false;
            return client.ReadEventsFromTopic(request, null, deadline, cancellationToken);
        }
        else
        {
            var client = new SpPubSub.SpPubSubClient(channel.Intercept(new StreamingCallInterceptor(session)));
            request.Speculative = true;
            return client.ReadEventsFromTopic(request, null, deadline, cancellationToken);
        }
    }
}