using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Google.Protobuf;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using protobuf;
using DarqMessage = FASTER.libdpr.DarqMessage;
using DarqMessageType = FASTER.libdpr.DarqMessageType;

namespace SimpleWorkflowBench;

internal enum ReservationWorkflowMessageTypes : byte
{
    WORKFLOW_DEFN, RESERVATION_START,  RESERVATION_ROLLBACK
}

internal struct WorkflowDefinitionDarqEntry : ILogEnqueueEntry
{
    internal long workflowId;
    internal ByteString input;

    public int SerializedLength => sizeof(long) + sizeof(ReservationWorkflowMessageTypes) + sizeof(int) + input?.Length ?? 0;

    public void SerializeTo(Span<byte> dest)
    {
        unsafe
        {
            fixed (byte* d = dest)
            {
                var head = d;
                *(long*)head = workflowId;
                head += sizeof(long);
                *(ReservationWorkflowMessageTypes*)head = ReservationWorkflowMessageTypes.WORKFLOW_DEFN;
                head += sizeof(ReservationWorkflowMessageTypes);
                *(int*)head = input?.Length ?? 0;
                head += sizeof(int);
                input?.Span.CopyTo(new Span<byte>(head, dest.Length  - (int)(head - d)));
            }
        }
    }
}

internal struct ActivityDarqEntry : ILogEnqueueEntry
{
    internal long workflowId;
    internal ReservationWorkflowMessageTypes type;
    internal int index;

    public int SerializedLength => sizeof(long) + sizeof(ReservationWorkflowMessageTypes) + sizeof(int);

    public void SerializeTo(Span<byte> dest)
    {
        unsafe
        {
            fixed (byte* d = dest)
            {
                var head = d;
                *(long*)head = workflowId;
                head += sizeof(long);
                *(ReservationWorkflowMessageTypes *)head = type;
                head += sizeof(ReservationWorkflowMessageTypes);
                *(int*)head = index;
            }
        }
    }
}

public class ReservationWorkflowStateMachine : IWorkflowStateMachine
{
    private long workflowId = -1;
    private List<(int, ReservationRequest)> toExecute;
    private TaskCompletionSource<bool> tcs = new();
    private IDarqProcessorClientCapabilities capabilities;
    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private StateObject backend;
    
    private ConcurrentDictionary<int, GrpcChannel> connections = new();
    
    public async Task<ExecuteWorkflowResult> GetResult(CancellationToken token)
    {
        var result = await tcs.Task.WaitAsync(token);
        return new ExecuteWorkflowResult
        {
            Ok = result,
            Result = ByteString.Empty,
        };
    }
    
    public void ProcessMessage(DarqMessage m)
    {
        backend.StartLocalAction();
        if (workflowId == -1 && m.GetMessageType() == DarqMessageType.IN)
        {
            // This is the first message and it's the input that parameterizes the workflow
            workflowId = BitConverter.ToInt64(m.GetMessageBody());
            // TODO(Tianyu): read and create the workflow from the rest
            
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            requestBuilder.AddRecoveryMessage(new WorkflowDefinitionDarqEntry
            {
                workflowId = workflowId,
                input = ByteString.CopyFrom(m.GetMessageBody()[sizeof(long)..])
            });

            requestBuilder.AddSelfMessage(new ActivityDarqEntry
            {
                workflowId = workflowId,
                type = ReservationWorkflowMessageTypes.RESERVATION_START,
                index = 0
            });
            requestBuilder.MarkMessageConsumed(m.GetLsn());
            
            // Will always be completed synchronously
            capabilities.Step(requestBuilder.FinishStep()).GetAwaiter().GetResult();
            m.Dispose();
            stepRequestPool.Return(stepRequest);
        }
        if (m.GetMessageType() == DarqMessageType.RECOVERY)
        {
            // We only send ourselves one type of recovery message that encodes a workflow definition
            workflowId = BitConverter.ToInt64(m.GetMessageBody());
            // TODO(Tianyu): read and create the workflow from the rest
            // This handle was created by this thread, which gives us the ability to go ahead and start the workflow
            m.Dispose();
        }
        Debug.Assert(m.GetMessageType() == DarqMessageType.IN);
        var lsn = m.GetLsn();
        var type = (ReservationWorkflowMessageTypes) m.GetMessageBody()[sizeof(long)];
        var index = BitConverter.ToInt32(
            m.GetMessageBody()[(sizeof(long) + sizeof(ReservationWorkflowMessageTypes))..]);
        if (type == ReservationWorkflowMessageTypes.RESERVATION_START)
            MakeReservation(lsn, index);
        else
            CancelReservation(lsn, index);
        m.Dispose();
    }

    private void MakeReservation(long lsn, int index)
    {
        if (index == toExecute.Count)
        {
            // We are done and there are no more reservations to make
            tcs.SetResult(true);
            backend.EndAction();
            // Clean up
            foreach(var val in connections.Values)
                val.Dispose();
            return;
        }

        var session = backend.DetachFromWorker();
        Task.Run(async () =>
        {
            var serviceId = toExecute[index].Item1;
            var channel = connections.GetOrAdd(serviceId,
                k => GrpcChannel.ForAddress($"service{serviceId}:15721"));
            var client =
                new FasterKVReservationService.FasterKVReservationServiceClient(
                    channel.Intercept(new DprClientInterceptor(session)));
            var result = await client.MakeReservationAsync(toExecute[index].Item2);
            if (!backend.TryMergeAndStartAction(session)) return;                
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
                requestBuilder.MarkMessageConsumed(lsn);
                requestBuilder.AddSelfMessage(new ActivityDarqEntry
                {
                    workflowId = workflowId,
                    type = result.Ok ? ReservationWorkflowMessageTypes.RESERVATION_START : ReservationWorkflowMessageTypes.RESERVATION_ROLLBACK,
                    index = result.Ok ? index + 1 : index - 1
                });
                requestBuilder.FinishStep();
                // Will always be completed synchronously
                capabilities.Step(requestBuilder.FinishStep()).GetAwaiter().GetResult();
                stepRequestPool.Return(stepRequest);

        });
    }
    
    
    private void CancelReservation(long lsn, int index)
    {
        if (index == -1)
        {
            // We are done and there are no more reservations to make
            tcs.SetResult(false);
            backend.EndAction();
            // Clean up
            foreach(var val in connections.Values)
                val.Dispose();
            return;
        }
        
        var session = backend.DetachFromWorker();
        Task.Run(async () =>
        {
            var serviceId = toExecute[index].Item1;
            var channel = connections.GetOrAdd(serviceId,
                k => GrpcChannel.ForAddress($"service{serviceId}:15721"));
            var client =
                new FasterKVReservationService.FasterKVReservationServiceClient(
                    channel.Intercept(new DprClientInterceptor(session)));
            var result = await client.CancelReservationAsync(toExecute[index].Item2);
            if (!backend.TryMergeAndStartAction(session)) return;                
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            requestBuilder.MarkMessageConsumed(lsn);
            requestBuilder.AddSelfMessage(new ActivityDarqEntry
            {
                workflowId = workflowId,
                type = ReservationWorkflowMessageTypes.RESERVATION_ROLLBACK,
                index = index - 1
            });
            requestBuilder.FinishStep();
            // Will always be completed synchronously
            capabilities.Step(requestBuilder.FinishStep()).GetAwaiter().GetResult();
            stepRequestPool.Return(stepRequest);
        });
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities, StateObject backend)
    {
        this.capabilities = capabilities;
        this.backend = backend;
    }
}