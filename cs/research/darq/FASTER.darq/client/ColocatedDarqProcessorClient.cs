using System.Diagnostics;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.darq
{
    /// <summary>
    /// A DarqConsumer that runs in the same process as a DARQ instance
    /// </summary>
    public class ColocatedDarqProcessorClient<TVersionScheme> : IDarqProcessorClient
        where TVersionScheme : IVersionScheme
    {
        private Darq darq;
        private SimpleObjectPool<DarqMessage> messagePool;
        private ManualResetEventSlim terminationStart, terminationComplete;

        private bool shouldSnapshotDpr;

        // TODO(Tianyu): For benchmark purposes only
        public Stopwatch sw = new();

        // TODO(Tianyu): Reason about behavior in the case of rollback
        private long incarnation;
        private DarqScanIterator iterator;
        private Capabilities capabilities;
        private CancellationTokenSource loopAwaitCancellationTokenSource;

        private enum ProcessResult
        {
            CONTINUE,
            NO_ENTRY,
            TERMINATED
        }

        private class Capabilities : IDarqProcessorClientCapabilities
        {
            private readonly ColocatedDarqProcessorClient<TVersionScheme> parent;
            internal readonly long worldLine;

            public Capabilities(ColocatedDarqProcessorClient<TVersionScheme> parent, long worldLine)
            {
                this.parent = parent;
                this.worldLine = worldLine;
            }

            public unsafe ValueTask<StepStatus> Step(StepRequest request)
            {
                Span<byte> header = default;
                // If step results in a version mismatch, rely on the scan to trigger a rollback for simplicity
                if (parent.darq.WorldLine() != worldLine)
                    return new ValueTask<StepStatus>(StepStatus.REINCARNATED);

                var status = parent.darq.Step(parent.incarnation, request);
                return new ValueTask<StepStatus>(status);
            }
        }

        /// <summary>
        /// Constructs a new ColocatedDarqProcessorClient
        /// </summary>
        /// <param name="darq">DARQ DprServer that this consumer attaches to </param>
        /// <param name="clusterInfo"> information about the DARQ cluster </param>
        public ColocatedDarqProcessorClient(Darq darq)
        {
            this.darq = darq;
            messagePool = new SimpleObjectPool<DarqMessage>(() => new DarqMessage(messagePool));
            loopAwaitCancellationTokenSource = new CancellationTokenSource();
        }

        public void Dispose()
        {
            messagePool.Dispose();
            iterator?.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool TryReadEntry(out DarqMessage message)
        {
            message = null;
            var headerBytes = stackalloc byte[DprMessageHeader.FixedLenSize];

            if (!iterator.UnsafeGetNext(out var entry, out var entryLength,
                    out var lsn, out var nextLsn, out var type))
                return false;

            // Short circuit without looking at the entry -- no need to process in background
            if (type != DarqMessageType.IN && type != DarqMessageType.RECOVERY)
            {
                iterator.UnsafeRelease();
                return true;
            }

            // Copy out the entry before dropping protection
            message = messagePool.Checkout();
            message.Reset(type, lsn, nextLsn, new ReadOnlySpan<byte>(entry, entryLength));
            iterator.UnsafeRelease();

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ProcessResult TryConsumeNext<T>(T processor) where T : IDarqProcessor
        {
            try
            {
                var hasNext = TryReadEntry(out var m);
                // Manually check if worldLine matches without going through the heavyweight DPR path
                if (capabilities.worldLine != darq.WorldLine())
                {
                    Console.WriteLine("Processor detected rollback, restarting");
                    OnProcessorClientRestart(processor);
                    // Reset to next iteration without doing anything
                    return ProcessResult.CONTINUE;
                }

                if (!hasNext)
                    return ProcessResult.NO_ENTRY;

                // Not a message we need to worry about
                if (m == null) return ProcessResult.CONTINUE;

                if (!sw.IsRunning) sw.Start();
                switch (m.GetMessageType())
                {
                    case DarqMessageType.IN:
                    case DarqMessageType.RECOVERY:
                        if (processor.ProcessMessage(m))
                            return ProcessResult.CONTINUE;
                        return ProcessResult.TERMINATED;
                    default:
                        throw new NotImplementedException();
                }
            }
            catch (DprSessionRolledBackException)
            {
                Console.WriteLine("Processor detected rollback, restarting");
                OnProcessorClientRestart(processor);
                // Reset to next iteration without doing anything
                return ProcessResult.CONTINUE;
            }
        }

        private void OnProcessorClientRestart<T>(T processor) where T : IDarqProcessor
        {
            capabilities = new Capabilities(this, darq.WorldLine());
            processor.OnRestart(capabilities);
            iterator = darq.StartScan();
        }

        /// <inheritdoc/>
        public void StartProcessing<T>(T processor) where T : IDarqProcessor
        {
            StartProcessingAsync(processor).GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public async Task StartProcessingAsync<T>(T processor)
            where T : IDarqProcessor
        {
            try
            {
                terminationStart = new ManualResetEventSlim();
                terminationComplete = new ManualResetEventSlim();

                incarnation = darq.RegisterNewProcessor();
                OnProcessorClientRestart(processor);
                Console.WriteLine("Starting Processor...");
                while (!terminationStart.IsSet)
                {
                    ProcessResult result;
                    do
                    {
                        result = TryConsumeNext(processor);
                    } while (result == ProcessResult.CONTINUE);

                    if (result == ProcessResult.TERMINATED)
                        break;

                    // FASTER.darq.StateObject().RefreshSafeReadTail();
                    try
                    {
                        await iterator.WaitAsync(loopAwaitCancellationTokenSource.Token).AsTask();
                    }
                    catch (OperationCanceledException) {}
                }

                Console.WriteLine($"Colocated processor has exited on worker {darq.Me().guid}");
                terminationComplete.Set();
            }
            catch (Exception e)
            {
                Console.WriteLine("C# why you eat exceptions");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }

        /// <inheritdoc/>
        public void StopProcessing()
        {
            StopProcessingAsync().GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public async Task StopProcessingAsync()
        {
            terminationStart.Set();
            loopAwaitCancellationTokenSource.Cancel();
            while (!terminationComplete.IsSet)
                await Task.Delay(10);
            iterator.Dispose();
            if (sw.IsRunning) sw.Stop();
        }
    }
}