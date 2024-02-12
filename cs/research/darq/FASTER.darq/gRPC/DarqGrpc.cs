using System.Diagnostics;
using FASTER.client;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using protobuf;
using DarqMessageType = FASTER.libdpr.DarqMessageType;

namespace darq.gRPC;

public class DarqGrpcServiceImpl : DarqGrpcService.DarqGrpcServiceBase, IDisposable
{
    private Darq backend;
    private readonly DarqBackgroundWorker backgroundWorker;
    private readonly ManualResetEventSlim terminationStart;
    private readonly CountdownEvent terminationComplete;
    private Thread backgroundThread, refreshThread;

    private ThreadLocalObjectPool<StepRequest> stepRequestPool;
    private ThreadLocalObjectPool<byte[]> enqueueRequestPool;

    private long currentIncarnationId;
    private DarqScanIterator currentIterator;

    public DarqGrpcServiceImpl(DarqSettings darqSettings, IDarqClusterInfo clusterInfo)
    {
        backend = new Darq(darqSettings);
        backgroundWorker = new DarqBackgroundWorker(backend, clusterInfo);
        terminationStart = new ManualResetEventSlim();
        terminationComplete = new CountdownEvent(2);
        stepRequestPool = new ThreadLocalObjectPool<StepRequest>(() => new StepRequest());
        enqueueRequestPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 15]);
        backend.ConnectToCluster();

        if (backgroundWorker != default)
        {
            backgroundThread = new Thread(() => backgroundWorker.StartProcessingAsync());
            backgroundThread.Start();
        }

        refreshThread = new Thread(() =>
        {
            while (!terminationStart.IsSet)
                backend.Refresh();
            terminationComplete.Signal();
        });
        refreshThread.Start();
    }

    public void Dispose()
    {
        terminationStart.Set();
        // TODO(Tianyu): this shutdown process is unsafe and may leave things unsent/unprocessed in the queue
        backend.ForceCheckpoint();
        Thread.Sleep(2000);
        backgroundWorker?.StopProcessingAsync().GetAwaiter().GetResult();
        backgroundWorker?.Dispose();
        terminationComplete.Wait();
        backend.StateObject().Dispose();
        backgroundThread?.Join();
        refreshThread.Join();
    }

    public Darq GetDarq() => backend;

    public override async Task<RegisterProcessorResult> RegisterProcessor(RegisterProcessorRequest request,
        ServerCallContext context)
    {
        var result = await backend.RegisterNewProcessorAsync();
        // Not a serial bottleneck as we don't expect this to be invoked under high concurrency
        lock (this)
        {
            currentIncarnationId = result;
            currentIterator = backend.StartScan();
        }
        return new RegisterProcessorResult
        {
            IncarnationId = result
        };
    }

    public override Task<DarqStepResult> Step(DarqStepRequest request, ServerCallContext context)
    {
        var requestObject = stepRequestPool.Checkout();
        var requestBuilder = new StepRequestBuilder(requestObject);
        foreach (var consumed in request.ConsumedMessages)
            requestBuilder.MarkMessageConsumed(consumed);
        foreach (var self in request.SelfMessages)
            requestBuilder.AddRecoveryMessage(self.Span);
        foreach (var outMessage in request.OutMessages)
            requestBuilder.AddOutMessage(new DarqId(outMessage.Recipient), outMessage.Message.Span);
        var result = backend.Step(request.IncarnationId, requestBuilder.FinishStep());
        stepRequestPool.Return(requestObject);

        return Task.FromResult(new DarqStepResult
        {
            Status = result switch
            {
                // Should never happen
                StepStatus.INCOMPLETE => throw new NotImplementedException(),
                StepStatus.SUCCESS => DarqStepStatus.Success,
                StepStatus.INVALID => DarqStepStatus.Invalid,
                StepStatus.REINCARNATED => DarqStepStatus.Reincarnated,
                _ => throw new ArgumentOutOfRangeException()
            }
        });
    }

    public override Task<DarqEnqueueResult> Enqueue(DarqEnqueueRequest request, ServerCallContext context)
    {
        var enqueueBuffer = enqueueRequestPool.Checkout();
        SerializedDarqEntryBatch enqueueRequest;
        unsafe
        {
            fixed (byte* b = enqueueBuffer)
            {
                enqueueRequest = new SerializedDarqEntryBatch(b);
                enqueueRequest.SetContent(request.Message.Span);
            }
        }

        var ok = backend.Enqueue(enqueueRequest, request.ProducerId, request.Lsn);
        enqueueRequestPool.Return(enqueueBuffer);
        return Task.FromResult(new DarqEnqueueResult
        {
            Ok = ok
        });
    }

    public override Task<DarqPollResult> Poll(DarqPollRequest request, ServerCallContext context)
    {
        // Not a serial bottleneck as we don't expect this to be invoked under high concurrency
        lock (this)
        {
            // Not able to poll if you are not the recognized consumer
            if (currentIncarnationId != request.IncarnationId)
                return Task.FromResult(new DarqPollResult
                {
                    Ok = false
                });

            var result = new DarqPollResult { Ok = true };
            unsafe
            {
                for (var i = 0; i < request.MaxBatchSize; i++)
                {
                    if (!currentIterator.UnsafeGetNext(out var b, out var length, out _, out _, out var type))
                        break;
                    if (type is DarqMessageType.IN or DarqMessageType.RECOVERY)
                    {
                        var darqMessage = new protobuf.DarqMessage
                        {
                            Type = type switch
                            {
                                DarqMessageType.IN => protobuf.DarqMessageType.In,
                                DarqMessageType.RECOVERY => protobuf.DarqMessageType.Self,
                                _ => throw new ArgumentOutOfRangeException()
                            },
                            MesssageBody = ByteString.CopyFrom(new ReadOnlySpan<byte>(b, length))
                        };
                        result.Messages.Add(darqMessage);
                    }

                    currentIterator.UnsafeRelease();
                }
            }
            return Task.FromResult(result);

        }
    }
}