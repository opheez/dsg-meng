using System.Diagnostics;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;

namespace FASTER.client
{
    public class DarqBackgroundWorker : IDisposable
    {
        private Darq darq;
        private SimpleObjectPool<DarqMessage> messagePool;
        private ManualResetEventSlim terminationStart, terminationComplete;
        private IDarqClusterInfo clusterInfo;
        private DprSession session;
        private DarqScanIterator iterator;
        private DarqProducerClient producerClient;
        private DarqCompletionTracker completionTracker;
        private long processedUpTo;
        private int batchSize, numBatched = 0;


        /// <summary>
        /// Constructs a new ColocatedDarqProcessorClient
        /// </summary>
        /// <param name="darq">DARQ DprServer that this consumer attaches to </param>
        /// <param name="clusterInfo"> information about the DARQ cluster </param>
        public DarqBackgroundWorker(Darq darq, IDarqClusterInfo clusterInfo, int batchSize = 16)
        {
            this.darq = darq;
            this.clusterInfo = clusterInfo;
            messagePool = new SimpleObjectPool<DarqMessage>(() => new DarqMessage(messagePool), 1 << 15);
            this.batchSize = batchSize;
            session = new DprSession(darq.WorldLine());
        }

        public long ProcessingLag => darq.StateObject().log.TailAddress - processedUpTo;

        public void Dispose()
        {
            iterator?.Dispose();
            producerClient?.Dispose();
        }

        private unsafe bool TryReadEntry(out DarqMessage message)
        {
            message = null;
            long nextAddress = 0;
            try
            {
                darq.StartStep();

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
            }
            finally
            {
                darq.EndStep();
            }

            return true;
        }

        private unsafe void SendMessage(DarqMessage m)
        {
            Debug.Assert(m.GetMessageType() == DarqMessageType.OUT);
            var body = m.GetMessageBody();
            fixed (byte* h = body)
            {
                var dest = *(WorkerId*)h;
                var toSend = new ReadOnlySpan<byte>(h + sizeof(WorkerId),
                    body.Length - sizeof(WorkerId));
                var completionTrackerLocal = completionTracker;
                var lsn = m.GetLsn();
                if (++numBatched < batchSize)
                {
                    producerClient.EnqueueMessageWithCallback(dest, toSend,
                        _ => { completionTrackerLocal.RemoveEntry(lsn); }, darq.Me().guid, lsn, forceFlush: false);
                }
                else
                {
                    numBatched = 0;
                    producerClient.EnqueueMessageWithCallback(dest, toSend,
                        _ => { completionTrackerLocal.RemoveEntry(lsn); }, darq.Me().guid, lsn,
                        forceFlush: true);
                }
            }

            m.Dispose();
        }

        private bool TryConsumeNext()
        {
            var hasNext = TryReadEntry(out var m);
            // Don't go through the normal receive code path for performance
            if (session.RolledBack || darq.WorldLine() > session.WorldLine)
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
                darq.StartStep();
                darq.TruncateUntil(completionTracker.GetTruncateHead());
                darq.EndStep();
            }

            return true;
        }

        private void Reset()
        {
            session = new DprSession();
            producerClient = new DarqProducerClient(clusterInfo, session);
            completionTracker = new DarqCompletionTracker();
            iterator = darq.StartScan();
        }

        public async Task StartProcessing()
        {
            try
            {
                var terminationToken = new ManualResetEventSlim();
                if (Interlocked.CompareExchange(ref terminationStart, terminationToken, null) != null)
                    // already started
                    return;
                terminationComplete = new ManualResetEventSlim();

                Reset();
                Console.WriteLine($"Starting background send from address {darq.StateObject().log.BeginAddress}");
                while (!terminationStart.IsSet)
                {
                    while (TryConsumeNext())
                    {
                    }

                    producerClient.ForceFlush();
                    var iteratorWait = iterator.WaitAsync().AsTask();
                    if (await Task.WhenAny(iteratorWait, Task.Delay(5)) == iteratorWait)
                    {
                        // No more entries, can signal finished and return 
                        if (!iteratorWait.Result) break;
                    }
                    // Otherwise, just continue looping
                }

                producerClient.ForceFlush();
                terminationComplete.Set();
            }
            catch (Exception e)
            {
                // Just restart the failed background thread
                Console.WriteLine($"Exception {e.Message} was thrown, restarting background worker");
                terminationComplete.Set();
                terminationStart = null;
                await StartProcessing();
            }
        }

        public async Task StopProcessingAsync()
        {
            var t = terminationStart;
            var c = terminationComplete;
            if (t == null) return;

            t.Set();
            while (!c.IsSet)
                await Task.Delay(10);
            terminationStart = null;
        }
    }
}