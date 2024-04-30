using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Grpc.Net.Client;
using System.Collections.Concurrent;

namespace DB {
public class DarqTransactionProcessorService : TransactionProcessor.TransactionProcessorBase
{
    private DarqTransactionProcessorBackgroundService backend;
    public DarqTransactionProcessorService(DarqTransactionProcessorBackgroundService backend)
    {
        this.backend = backend;
    }

    public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context)
    {
        return backend.Read(request);
    }

    public override Task<ReadSecondaryReply> ReadSecondary(ReadSecondaryRequest request, ServerCallContext context)
    {
        return backend.ReadSecondary(request);
    }

    public override Task<PopulateTablesReply> PopulateTables(PopulateTablesRequest request, ServerCallContext context)
    {
        return backend.PopulateTables(request);
    }

    public override Task<EnqueueWorkloadReply> EnqueueWorkload(EnqueueWorkloadRequest request, ServerCallContext context)
    {
        return backend.EnqueueWorkload(request);
    }

    public override Task<WalReply> WriteWalEntry(WalRequest request, ServerCallContext context)
    {
        return backend.WriteWalEntry(request);
    }

}

public class DarqTransactionProcessorBackgroundService : BackgroundService, IDarqProcessor  {
    private ShardedTransactionManager txnManager;
    private ConcurrentDictionary<(long, long), long> externalToInternalTxnId = new ConcurrentDictionary<(long, long), long>();
    private ConcurrentDictionary<long, TransactionContext> txnIdToTxnCtx = new ConcurrentDictionary<long, TransactionContext>();
    private DarqWal wal;
    private long partitionId;
    Dictionary<int, ShardedTable> tables;
    // from darqProcessor
    Darq backend;
    private ColocatedDarqProcessorClient processorClient;
    private IDarqProcessorClientCapabilities capabilities;
    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private StepRequest reusableRequest = new();
    protected ILogger logger;
    public DarqTransactionProcessorBackgroundService(
        long partitionId,
        Dictionary<int, ShardedTable> tables,
        ShardedTransactionManager txnManager,
        DarqWal wal,
        Darq darq,
        ILogger logger = null
    ) {
        this.tables = tables;
        this.logger = logger;
        this.txnManager = txnManager;
        this.partitionId = partitionId;
        this.wal = wal;

        backend = darq;
        processorClient = new ColocatedDarqProcessorClient(backend, false);
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Starting DARQ background service");
        backend.ConnectToCluster(out var _);
        
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        logger.LogInformation("Faster service is stopping...");
    }
    public Task<ReadReply> Read(ReadRequest request)
    {
        long internalTid = GetOrRegisterTid(request.PartitionId, request.Tid);
        Table table = tables[request.Key.Table];
        TransactionContext ctx = txnIdToTxnCtx[internalTid];
        PrimaryKey tupleId = new PrimaryKey(request.Key.Table, request.Key.Keys.ToArray());
        TupleDesc[] tupleDescs = table.GetSchema();
        ReadReply reply = new ReadReply{ Value = ByteString.CopyFrom(table.Read(tupleId, tupleDescs, ctx))};
        return Task.FromResult(reply);
    }

    public Task<ReadSecondaryReply> ReadSecondary(ReadSecondaryRequest request)
    {
        PrintDebug($"Reading secondary from rpc service");
        long internalTid = GetOrRegisterTid(request.PartitionId, request.Tid);
        Table table = tables[request.Table];
        TransactionContext ctx = txnIdToTxnCtx[internalTid];
        var (value, pk) = table.ReadSecondary(request.Key.ToArray(), table.GetSchema(), ctx);
        ReadSecondaryReply reply = new ReadSecondaryReply{ Value = ByteString.CopyFrom(value), Key = new PbPrimaryKey{ Keys = {pk.Keys}, Table = pk.Table}};
        return Task.FromResult(reply);
    }

    public Task<PopulateTablesReply> PopulateTables(PopulateTablesRequest request)
    {
        PrintDebug($"Populating tables from rpc service");
        BenchmarkConfig cfg = new BenchmarkConfig(
            seed: request.Seed,
            ratio: request.Ratio,
            threadCount: request.ThreadCount,
            attrCount: request.AttrCount,
            perThreadDataCount: request.PerThreadDataCount,
            iterationCount: request.IterationCount,
            perTransactionCount: request.PerTransactionCount,
            nCommitterThreads: request.NCommitterThreads
        );

        TpccConfig tpccCfg = new TpccConfig(
            numWh: request.NumWh,
            numDistrict: request.NumDistrict,
            numCustomer: request.NumCustomer,
            numItem: request.NumItem,
            numOrder: request.NumOrder,
            numStock: request.NumStock,
            newOrderCrossPartitionProbability: request.NewOrderCrossPartitionProbability,
            paymentCrossPartitionProbability: request.PaymentCrossPartitionProbability,
            partitionsPerThread: request.PartitionsPerThread
        );
        TpccBenchmark tpccBenchmark = new TpccBenchmark((int)partitionId, tpccCfg, cfg, tables, txnManager);
        txnManager.Run();
        tpccBenchmark.PopulateTables();
        txnManager.Terminate();
        PopulateTablesReply reply = new PopulateTablesReply{ Success = true};
        return Task.FromResult(reply);
    }

    public Task<EnqueueWorkloadReply> EnqueueWorkload(EnqueueWorkloadRequest request)
    {
        BenchmarkConfig ycsbCfg = new BenchmarkConfig(
            ratio: 0.2,
            attrCount: 10,
            threadCount: 3,
            insertThreadCount: 12,
            iterationCount: 1,
            nCommitterThreads: 4
        );
        switch (request.Workload) {
            case "tpcc":
                TpccConfig tpccConfig = new TpccConfig(
                    numWh: 12,
                    partitionsPerThread: 4,
                    newOrderCrossPartitionProbability: 0,
                    paymentCrossPartitionProbability: 0
                );
                
                TpccBenchmark tpccBenchmark = new TpccBenchmark((int)partitionId, tpccConfig, ycsbCfg, tables, txnManager);
                tpccBenchmark.RunTransactions();
                // tpccBenchmark.GenerateTables();
                break;
            default:
                throw new NotImplementedException();
        }

        EnqueueWorkloadReply enqueueWorkloadReply = new EnqueueWorkloadReply{Success = true};
        return Task.FromResult(enqueueWorkloadReply);
    }

    // typically used for Prepare() and Commit() 
    public async Task<WalReply> WriteWalEntry(WalRequest request)
    {
        // PrintDebug($"Writing to WAL from {request.PartitionId}");
        LogEntry entry = LogEntry.FromBytes(request.Message.ToArray());

        if (entry.type == LogType.Prepare || entry.type == LogType.Commit)
        {
            long internalTid = GetOrRegisterTid(request.PartitionId, request.Tid);
            entry.lsn = internalTid; // TODO: HACKY reuse, we keep tid to be original tid
            entry.prevLsn = request.PartitionId; // TODO: hacky place to put sender id
            PrintDebug($"Stepping prepare/commit {entry.lsn}");
        } else {
            PrintDebug($"Stepping ok/ack {entry.tid}");
        }
        
        var stepRequest = stepRequestPool.Checkout();
        var requestBuilder = new StepRequestBuilder(stepRequest);
        // TODO: do we need to step messages consumed, self, and out messages 
        requestBuilder.AddSelfMessage(entry.ToBytes());
        await capabilities.Step(requestBuilder.FinishStep());
        stepRequestPool.Return(stepRequest);
        backend.EndAction();
        return new WalReply{Success = true};
    }

    private long GetOrRegisterTid(long partitionId, long tid) {
        PrintDebug($"Getting or registering tid: ({partitionId}, {tid})");
        if (externalToInternalTxnId.ContainsKey((partitionId, tid))) 
            return externalToInternalTxnId[(partitionId, tid)];

        var ctx = txnManager.Begin();
        long internalTid = ctx.tid;
        PrintDebug("Registering new tid: " + internalTid);
        externalToInternalTxnId[(partitionId, tid)] = internalTid;
        txnIdToTxnCtx[internalTid] = ctx;
        return internalTid;
    }

    override public void Dispose(){
        foreach (var table in tables.Values) {
            table.Dispose();
        }
        txnManager.Terminate();
        base.Dispose();
    }

    public bool ProcessMessage(DarqMessage m){
        PrintDebug($"Processing message");
        bool recoveryMode = false;
        switch (m.GetMessageType()){
            case DarqMessageType.IN:
            {
                unsafe
                {
                    fixed (byte* b = m.GetMessageBody())
                    {
                        int signal = *(int*)b;
                        // This is a special termination signal
                        if (signal == -1)
                        {
                            m.Dispose();
                            // Return false to signal that there are no more messages to process and the processing
                            // loop can exit
                            return false;
                        }
                    }
                }

                LogEntry entry = LogEntry.FromBytes(m.GetMessageBody().ToArray());
                var requestBuilder = new StepRequestBuilder(reusableRequest);
                // requestBuilder.AddRecoveryMessage(m.GetMessageBody());
                switch (entry.type)
                {
                    // Coordinator side
                    case LogType.Ok:
                    {
                        PrintDebug($"Got OK log entry: {entry}");
                        txnManager.MarkAcked(entry.tid, TransactionStatus.Validated, m.GetLsn(), entry.prevLsn);
                        m.Dispose();
                        return true;
                    }
                    case LogType.Ack:
                    {
                        PrintDebug($"Got ACK log entry: {entry}");
                        // can ignore in DARQ since we know out commit message is sent
                        m.Dispose();
                        return true;
                    }
                    // Worker side
                    case LogType.Prepare:
                    {
                        PrintDebug($"Got prepare log entry: {entry}");
                        requestBuilder.MarkMessageConsumed(m.GetLsn());
                        long sender = entry.prevLsn; // hacky
                        long internalTid = entry.lsn; // ""

                        // add each write to context before validating
                        TransactionContext ctx = txnIdToTxnCtx[internalTid];
                        for (int i = 0; i < entry.keyAttrs.Length; i++)
                        {
                            KeyAttr keyAttr = entry.keyAttrs[i];
                            Table table = tables[keyAttr.Key.Table];
                            (int, int) metadata = table.GetAttrMetadata(keyAttr.Attr);
                            ctx.AddWriteSet(new PrimaryKey(table.GetId(), keyAttr.Key.Keys), new TupleDesc[]{new TupleDesc(keyAttr.Attr, metadata.Item1, 0)}, entry.vals[i]);
                        }
                        bool success = txnManager.Validate(ctx);
                        PrintDebug($"Validated at node {partitionId}: {success}; now sending OK to {sender}");
                        if (success) {
                            LogEntry okEntry = new LogEntry(partitionId, entry.tid, LogType.Ok);
                            requestBuilder.AddOutMessage(new DarqId(sender), okEntry.ToBytes());
                        }
                        break;
                    }
                    case LogType.Commit:
                    {
                        PrintDebug($"Got commit log entry: {entry}");
                        requestBuilder.MarkMessageConsumed(m.GetLsn());
                        long sender = entry.prevLsn; // hacky
                        long internalTid = entry.lsn; // ""
                        
                        txnManager.Write(txnIdToTxnCtx[internalTid], (tid, type) => wal.Finish(tid, type));

                        PrintDebug($"Committed at node {partitionId}; now sending ACK to {sender}");
                        LogEntry ackEntry = new LogEntry(partitionId, entry.tid, LogType.Ack);
                        requestBuilder.AddOutMessage(new DarqId(sender), ackEntry.ToBytes());
                        break;
                    }
                    default:
                        throw new NotImplementedException();
                }

                
                m.Dispose();
                var v = capabilities.Step(requestBuilder.FinishStep());
                Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
                return true;
            }
            case DarqMessageType.RECOVERY: // this is on recovery; TODO: do we need to double pass?
                PrintDebug($"Recovering?, got log");
                if (recoveryMode) {
                    LogEntry entry = LogEntry.FromBytes(m.GetMessageBody().ToArray());
                    
                    PrintDebug($"Recovering, got log entry: {entry}");

                }
                m.Dispose();
                return true;
            default:
                throw new NotImplementedException();
        }
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
        this.wal.SetCapabilities(capabilities);
    }

    void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[TPS {partitionId} TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }
}

}