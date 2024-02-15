using System.Collections.Concurrent;
using System.Diagnostics;
using darq;
using FASTER.client;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;

namespace FASTER.server
{
    public class DarqProvider<TVersionScheme> : ISessionProvider where TVersionScheme : IVersionScheme
    {
        private Darq backend;
        private ConcurrentQueue<ProducerResponseBuffer> responseQueue;

        internal DarqProvider(Darq backend, ConcurrentQueue<ProducerResponseBuffer> responseQueue)
        {
            this.backend = backend;
            GetMaxSizeSettings = new MaxSizeSettings();
            this.responseQueue = responseQueue;
        }

        public IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender)
        {
            switch ((DarqProtocolType) wireFormat)
            {
                case DarqProtocolType.DarqSubscribe:
                    return new DarqSubscriptionSession<TVersionScheme>(networkSender, backend);
                case DarqProtocolType.DarqProducer:
                    return new DarqProducerSession<TVersionScheme>(networkSender, backend, responseQueue);
                case DarqProtocolType.DarqProcessor:
                    return new DarqProcessorSession<TVersionScheme>(networkSender, backend);
                default:
                    throw new NotSupportedException();
            }
        }

        public MaxSizeSettings GetMaxSizeSettings { get; }
    }

    public class DarqServer<TVersionScheme> : IDisposable where TVersionScheme : IVersionScheme
    {
        private readonly IFasterServer server;
        private readonly Darq darq;
        private readonly DarqBackgroundWorkerPool workerPool;
        private readonly DarqProvider<TVersionScheme> provider;
        private readonly DarqBackgroundTask _backgroundTask;
        private readonly ManualResetEventSlim terminationStart;
        private readonly CountdownEvent terminationComplete;
        private Thread refreshThread, responseThread;
        private ConcurrentQueue<ProducerResponseBuffer> responseQueue;

        public DarqServer(DarqServerOptions options, TVersionScheme versionScheme)
        {
            darq = new Darq(options.DarqSettings, versionScheme);
            _backgroundTask = new DarqBackgroundTask(darq, options.WorkerPool, options.ClusterInfo);
            terminationStart = new ManualResetEventSlim();
            terminationComplete = new CountdownEvent(2);
            darq.ConnectToCluster();
            responseQueue = new();
            provider = new DarqProvider<TVersionScheme>(darq, responseQueue);
            server = new FasterServerTcp(options.Address, options.Port);
            // Check that our custom defined wire format is not clashing with anything implemented by FASTER
            Debug.Assert(!Enum.IsDefined(typeof(WireFormat), (WireFormat) (int) DarqProtocolType.DarqSubscribe));
            Debug.Assert(!Enum.IsDefined(typeof(WireFormat), (WireFormat) (int)DarqProtocolType.DarqProcessor));
            Debug.Assert(!Enum.IsDefined(typeof(WireFormat), (WireFormat) (int)DarqProtocolType.DarqProducer));

            server.Register((WireFormat) DarqProtocolType.DarqSubscribe, provider);
            server.Register((WireFormat) DarqProtocolType.DarqProcessor, provider);
            server.Register((WireFormat) DarqProtocolType.DarqProducer, provider);
        }

        public Darq GetDarq() => darq;

        public long BackgroundProcessingLag => _backgroundTask.ProcessingLag;

        public void Start()
        {
            server.Start();
            _backgroundTask.BeginProcessing();

            refreshThread = new Thread(() =>
            {
                while (!terminationStart.IsSet)
                    darq.Refresh();
                terminationComplete.Signal();
            });
            refreshThread.Start();

            responseThread = new Thread(async () =>
            {
                while (!terminationStart.IsSet && responseQueue != null && !responseQueue.IsEmpty)
                {
                    // TODO(Tianyu): current implementation may have response buffers in the queue with versions
                    // out-of-order, resulting in some responses getting sent later than necessary
                    while (responseQueue.TryPeek(out var response))
                    {
                        if (response.version <= darq.CommittedVersion())
                            // TODO(Tianyu): Figure out how to handle errors
                            response.networkSender.SendResponse(response.buf, 0, response.size, response.Dispose);
                        responseQueue.TryDequeue(out _);
                    }

                    await darq.NextCommit();
                }

                terminationComplete.Signal();
            });
            responseThread.Start();
        }

        public void Dispose()
        {
            terminationStart.Set();
            // TODO(Tianyu): this shutdown process is unsafe and may leave things unsent/unprocessed in the queue
            darq.ForceCheckpoint();
            Thread.Sleep(2000);
            _backgroundTask?.StopProcessing();
            _backgroundTask?.Dispose();
            server.Dispose();
            terminationComplete.Wait();
            darq.StateObject().Dispose();
            refreshThread.Join();
        }
    }
}