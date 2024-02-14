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
        private Darq<TVersionScheme> backend;
        private ConcurrentQueue<ProducerResponseBuffer> responseQueue;

        internal DarqProvider(Darq<TVersionScheme> backend, ConcurrentQueue<ProducerResponseBuffer> responseQueue)
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
        private readonly Darq<TVersionScheme> darq;
        private readonly DarqBackgroundWorkerPool workerPool;
        private readonly DarqProvider<TVersionScheme> provider;
        private readonly DarqBackgroundWorker<TVersionScheme> backgroundWorker;
        private readonly ManualResetEventSlim terminationStart;
        private readonly CountdownEvent terminationComplete;
        private Thread refreshThread, responseThread;
        private ConcurrentQueue<ProducerResponseBuffer> responseQueue;

        public DarqServer(DarqServerOptions options, TVersionScheme versionScheme)
        {
            darq = new Darq<TVersionScheme>(options.DarqSettings, versionScheme);
            backgroundWorker = new DarqBackgroundWorker<TVersionScheme>(darq, options.WorkerPool, options.ClusterInfo);
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

        public Darq<TVersionScheme> GetDarq() => darq;

        public long BackgroundProcessingLag => backgroundWorker.ProcessingLag;

        public void Start()
        {
            server.Start();
            backgroundWorker.BeginProcessing();

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
            backgroundWorker?.StopProcessing();
            backgroundWorker?.Dispose();
            server.Dispose();
            terminationComplete.Wait();
            darq.StateObject().Dispose();
            refreshThread.Join();
        }
    }
}