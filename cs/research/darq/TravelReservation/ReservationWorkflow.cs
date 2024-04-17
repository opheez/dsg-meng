using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using dse.services;
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

namespace TravelReservation;

internal enum ReservationWorkflowMessageTypes : byte
{
    RESERVATION_START,
    RESERVATION_ROLLBACK
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
                *(ReservationWorkflowMessageTypes*)head = type;
                head += sizeof(ReservationWorkflowMessageTypes);
                *(int*)head = index;
            }
        }
    }
}

public class ReservationWorkflowStateMachine : IWorkflowStateMachine
{
    private long workflowId;
    private List<ReservationRequest> toExecute = new();
    private TaskCompletionSource<bool> tcs = new();
    private IDarqProcessorClientCapabilities capabilities;
    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private ConcurrentDictionary<int, GrpcChannel> connectionPool;
    private IEnvironment environment;
    private bool speculative;
    private StateObject backend;

    public ReservationWorkflowStateMachine(ReadOnlySpan<byte> input,
        ConcurrentDictionary<int, GrpcChannel> connectionPool, IEnvironment environment, bool speculative)
    {
        var messageString = Encoding.UTF8.GetString(input);
        var split = messageString.Split(',');
        workflowId = long.Parse(split[1]);
        for (var i = 2; i < split.Length; i += 4)
        {
            toExecute.Add(new ReservationRequest
            {
                ReservationId = long.Parse(split[i]),
                OfferingId = long.Parse(split[i + 1]),
                CustomerId = long.Parse(split[i + 2]),
                Count = int.Parse(split[i + 3])
            });
        }

        this.connectionPool = connectionPool;
        this.environment = environment;
        this.speculative = speculative;
    }

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
        if (m.GetMessageBody().Length == sizeof(long))
        {
            // Then this is the initial message, bootstrap the state machine and begin execution
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);

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
            return;
        }

        Debug.Assert(m.GetMessageType() == DarqMessageType.IN);
        var lsn = m.GetLsn();
        var type = (ReservationWorkflowMessageTypes)m.GetMessageBody()[sizeof(long)];
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
            return;
        }

        var c = capabilities;
        var s = c.Detach();
        Task.Run(async () =>
        {
            var channel = connectionPool.GetOrAdd(index,
                i => GrpcChannel.ForAddress(environment.GetServiceConnString(i)));
            FasterKVReservationService.FasterKVReservationServiceClient client;
            if (speculative)
            {
                client = new FasterKVReservationService.FasterKVReservationServiceClient(
                        channel.Intercept(new DprClientInterceptor(s)));
            }
            else
            {
                client = new FasterKVReservationService.FasterKVReservationServiceClient(channel);
                await s.SpeculationBarrier(backend.GetDprFinder());
            }

            var result = await client.MakeReservationAsync(toExecute[index]);
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            requestBuilder.MarkMessageConsumed(lsn);
            requestBuilder.AddSelfMessage(new ActivityDarqEntry
            {
                workflowId = workflowId,
                type = result.Ok
                    ? ReservationWorkflowMessageTypes.RESERVATION_START
                    : ReservationWorkflowMessageTypes.RESERVATION_ROLLBACK,
                index = result.Ok ? index + 1 : index - 1
            });
            requestBuilder.FinishStep();
            // Will always be completed synchronously
            c.Step(requestBuilder.FinishStep(), s).GetAwaiter().GetResult();
            c.Return(s);
            stepRequestPool.Return(stepRequest);
        });
    }

    private void CancelReservation(long lsn, int index)
    {
        if (index == -1)
        {
            // We are done and there are no more reservations to make
            tcs.SetResult(false);
            return;
        }

        var c = capabilities;
        var s = c.Detach();
        Task.Run(async () =>
        {
            var channel = connectionPool.GetOrAdd(index,
                k => GrpcChannel.ForAddress(environment.GetServiceConnString(index)));
            FasterKVReservationService.FasterKVReservationServiceClient client;
            if (speculative)
            {
                client = new FasterKVReservationService.FasterKVReservationServiceClient(
                    channel.Intercept(new DprClientInterceptor(s)));
            }
            else
            {
                client = new FasterKVReservationService.FasterKVReservationServiceClient(channel);
                await s.SpeculationBarrier(backend.GetDprFinder());
            }
            
            await client.CancelReservationAsync(toExecute[index]);
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
            c.Step(requestBuilder.FinishStep(), s).GetAwaiter().GetResult();
            stepRequestPool.Return(stepRequest);
            c.Return(s);
        });
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities, StateObject backend)
    {
        this.capabilities = capabilities;
        this.backend = backend;
    }
}