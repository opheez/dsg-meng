using System.Diagnostics;
using darq;
using darq.client;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;

namespace FASTER.client
{
    public class DarqBackgroundTask : IDisposable
    {
        private Darq darq;
        private DarqBackgroundWorkerPool workerPool;
        private ManualResetEventSlim terminationStart, terminationComplete;
        private const int morselSize = 512;

        private long worldLine;
        private DarqScanIterator iterator;
        private DarqCompletionTracker completionTracker;
        private long processedUpTo;

        private SimpleObjectPool<DarqMessage> messagePool;

        private Func<DprSession, IDarqProducer> producerFactory;
        private IDarqProducer currentProducerClient;
        private int batchSize, numBatched = 0;
        
        /// <summary>
        /// Constructs a new ColocatedDarqProcessorClient
        /// </summary>
        /// <param name="darq">DARQ DprServer that this consumer attaches to </param>
        /// <param name="clusterInfo"> information about the DARQ cluster </param>
        public DarqBackgroundTask(Darq darq,
            DarqBackgroundWorkerPool workerPool, Func<DprSession, IDarqProducer> producerFactory, int batchSize = 16)
        {
            this.darq = darq;
            this.workerPool = workerPool;
            this.producerFactory = producerFactory;
            messagePool = new SimpleObjectPool<DarqMessage>(() => new DarqMessage(messagePool), 1 << 15);
            this.batchSize = batchSize;
            Reset();
        }

        private void Reset()
        {
            worldLine = darq.WorldLine();
            currentProducerClient = producerFactory?.Invoke(new DprSession());
            completionTracker = new DarqCompletionTracker();
            iterator = darq.StartScan();
        }

        public long ProcessingLag => darq.StateObject().log.TailAddress - processedUpTo;

        public void Dispose()
        {
            iterator?.Dispose();
            currentProducerClient?.Dispose();
        }

        private unsafe bool TryReadEntry(out DarqMessage message)
        {
            message = null;
            long nextAddress = 0;

            if (!iterator.UnsafeGetNext(out var entry, out var entryLength,
                    out var lsn, out processedUpTo, out var type))
                return false;

            completionTracker.AddEntry(lsn, processedUpTo);
            // Short circuit without looking at the entry -- no need to process in background
            if (type != DarqMessageType.OUT && type != DarqMessageType.COMPLETION)
            {
                iterator.UnsafeRelease();
                return true;
            }

            // Copy out the entry before dropping protection
            message = messagePool.Checkout();
            message.Reset(type, lsn, processedUpTo,
                new ReadOnlySpan<byte>(entry, entryLength));
            iterator.UnsafeRelease();

            return true;
        }

        // TODO(Tianyu): Create variants that allow DARQ instances to talk with each other through more than just the FASTER wire protocol
        private unsafe void SendMessage(DarqMessage m)
        {
            Debug.Assert(m.GetMessageType() == DarqMessageType.OUT);
            var body = m.GetMessageBody();
            fixed (byte* h = body)
            {
                var dest = *(DarqId*)h;
                var toSend = new ReadOnlySpan<byte>(h + sizeof(DarqId),
                    body.Length - sizeof(DarqId));
                var completionTrackerLocal = completionTracker;
                var lsn = m.GetLsn();
                // TODO(Tianyu): Make ack more efficient through batching
                currentProducerClient.EnqueueMessageWithCallback(dest, toSend,
                    _ => { completionTrackerLocal.RemoveEntry(lsn); }, darq.Me().guid, lsn);
                if (++numBatched == batchSize)
                {
                    numBatched = 0;
                    currentProducerClient.ForceFlush();
                }
            }

            m.Dispose();
        }

        private bool TryConsumeNext()
        {
            var hasNext = TryReadEntry(out var m);
            // Don't go through the normal receive code path for performance
            if (worldLine != darq.WorldLine())
            {
                Console.WriteLine("Processor detected rollback, restarting");
                Reset();
                // Reset to next iteration without doing anything
                return true;
            }

            if (!hasNext) return false;
            // Not a message we care about
            if (m == null) return true;

            switch (m.GetMessageType())
            {
                case DarqMessageType.OUT:
                {
                    SendMessage(m);
                    break;
                }
                case DarqMessageType.COMPLETION:
                {
                    var body = m.GetMessageBody();
                    unsafe
                    {
                        fixed (byte* h = body)
                        {
                            for (var completed = (long*)h; completed < h + body.Length; completed++)
                                completionTracker.RemoveEntry(*completed);
                        }
                    }

                    completionTracker.RemoveEntry(m.GetLsn());
                    m.Dispose();
                    break;
                }
                default:
                    throw new NotImplementedException();
            }

            if (completionTracker.GetTruncateHead() > darq.StateObject().log.BeginAddress)
            {
                Console.WriteLine($"Truncating log until {completionTracker.GetTruncateHead()}");
                darq.StartLocalAction();
                darq.TruncateUntil(completionTracker.GetTruncateHead());
                darq.EndAction();
            }

            return true;
        }

        private async Task ProcessBatch()
        {
            try
            {
                for (var i = 0; i < morselSize; i++)
                {
                    if (TryConsumeNext()) continue;
                    currentProducerClient.ForceFlush();
                    var iteratorWait = iterator.WaitAsync().AsTask();
                    if (await Task.WhenAny(iteratorWait, Task.Delay(5)) != iteratorWait)
                        break;
                }

                if (!terminationStart.IsSet)
                    workerPool.AddWork(ProcessBatch);
                else
                    terminationComplete.Set();
            }
            catch (Exception e)
            {
                // Just restart the failed background thread
                Console.WriteLine($"Exception {e.Message} was thrown, restarting background worker");
                Reset();
                if (!terminationStart.IsSet)
                    workerPool.AddWork(ProcessBatch);
            }
        }

        public void BeginProcessing()
        {
            var terminationToken = new ManualResetEventSlim();
            if (Interlocked.CompareExchange(ref terminationStart, terminationToken, null) != null)
                // already started
                return;
            terminationComplete = new ManualResetEventSlim();

            Reset();
            Console.WriteLine($"Starting background send from address {darq.StateObject().log.BeginAddress}");
            workerPool.AddWork(ProcessBatch);
        }

        public void StopProcessing()
        {
            var t = terminationStart;
            var c = terminationComplete;
            if (t == null) return;

            t.Set();
            while (!c.IsSet) Thread.Yield();
            terminationStart = null;
        }
    }
}