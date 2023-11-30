using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Google.Protobuf.Reflection;

namespace FASTER.libdpr;

public class TestMessage
{
    internal WorkerId originator;
    internal long originatorStateSerialNum;
}

public class TestStateStore : IStateObject<TestMessage>
{
    private struct TestCheckpointInfo
    {
        internal long stateSerialNum;
        internal int numReceivedMessages;
        internal byte[] dprMetadata;
    }
    
    private WorkerId me;
    private List<TestMessage> receivedMessages = new();
    private long stateSerialNum;
    private long persistedSerialNum;
    private Dictionary<long, TestCheckpointInfo> checkpoints = new();

    public TestStateStore(WorkerId me)
    {
        this.me = me;
    }

    public void Receive(TestMessage m)
    {
        Interlocked.Increment(ref stateSerialNum);
        lock (receivedMessages)
            receivedMessages.Add(m);
    }

    public void DoLocalWork()
    {
        Interlocked.Increment(ref stateSerialNum);
    }

    public TestMessage GenerateMessageToSend()
    {
        var incremented = Interlocked.Increment(ref stateSerialNum);
        return new TestMessage
        {
            originator = me,
            originatorStateSerialNum = incremented
        };
    }

    public void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
    {
        persistedSerialNum = stateSerialNum;
        checkpoints[version] = new TestCheckpointInfo
        {
            stateSerialNum = persistedSerialNum,
            numReceivedMessages = receivedMessages.Count,
            dprMetadata = metadata.ToArray()
        };
    }

    public void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        var restored = checkpoints[version];
        persistedSerialNum = stateSerialNum = restored.stateSerialNum;
        receivedMessages.RemoveRange(restored.numReceivedMessages,
            receivedMessages.Count - restored.numReceivedMessages);
        metadata = restored.dprMetadata;
    }

    public void PruneVersion(long version)
    {
        checkpoints.Remove(version);
    }

    public IEnumerable<(byte[], int)> GetUnprunedVersions()
    {
        return checkpoints.Values.Select(info => ValueTuple.Create(info.dprMetadata, 0));
    }
}