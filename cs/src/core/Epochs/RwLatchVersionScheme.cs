using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core;

public class NonThreadBasedReaderWriterLatch
{
    private long readerCount = 0;
    private int writerPending = 0;

    public void EnterReadLock()
    {
        while (true)
        {
            while (writerPending != 0) {}
            Interlocked.Increment(ref readerCount);
            if (writerPending == 0) return;
            Interlocked.Decrement(ref readerCount);
        }
    }

    public void ExitReadLock()
    {
        Interlocked.Decrement(ref readerCount);
    }

    public void EnterWriteLock()
    {
        while (Interlocked.CompareExchange(ref writerPending, 1, 0) != 0) {}
        while (readerCount != 0) {}
    }

    public void ExitWriteLock()
    {
        writerPending = 0;
    }
}

public class RwLatchVersionScheme : VersionSchemeBase
{
    private NonThreadBasedReaderWriterLatch rwLatch = new();
    
    // Atomic transition from expectedState -> nextState
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool MakeTransition(VersionSchemeState expectedState, VersionSchemeState nextState)
    {
        if (Interlocked.CompareExchange(ref state.Word, nextState.Word, expectedState.Word) != expectedState.Word) 
            return false;
        Debug.WriteLine("Moved to {0}, {1}", nextState.Phase, nextState.Version);
        return true;
    }
    
    protected override void TryStepStateMachine(VersionSchemeStateMachine expectedMachine = null)
    {
        var machineLocal = currentMachine;
        var oldState = state;

        // Nothing to step
        if (machineLocal == null) return;

        // Should exit to avoid stepping infinitely (until stack overflow)
        if (expectedMachine != null && machineLocal != expectedMachine) return;

        // Still computing actual to version
        if (machineLocal.actualToVersion == -1) return;

        // Machine finished, but not reset yet. Should reset and avoid starting another cycle
        if (oldState.Phase == VersionSchemeState.REST && oldState.Version == machineLocal.actualToVersion)
        {
            Interlocked.CompareExchange(ref currentMachine, null, machineLocal);
            return;
        }

        // Step is in progress or no step is available
        if (oldState.IsIntermediate() || !machineLocal.GetNextStep(oldState, out var nextState)) return;

        var intermediate = VersionSchemeState.MakeIntermediate(oldState);
        if (!MakeTransition(oldState, intermediate)) return;
        
        rwLatch.EnterWriteLock();
        machineLocal.OnEnteringState(oldState, nextState);
        var success = MakeTransition(VersionSchemeState.MakeIntermediate(oldState), nextState);
        machineLocal.AfterEnteringState(nextState);
        Debug.Assert(success);
        TryStepStateMachine(machineLocal);
        rwLatch.ExitWriteLock();
        
    }

    public override VersionSchemeState Enter()
    {
        TryStepStateMachine();
        rwLatch.EnterReadLock();

        VersionSchemeState result;
        while (true)
        {
            result = state;
            if (!result.IsIntermediate()) break;
            rwLatch.ExitReadLock();
            Thread.Yield();
            rwLatch.EnterReadLock();
        }

        return result;
    }

    public override VersionSchemeState Refresh()
    {
        Leave();
        return Enter();
    }

    public override void Leave()
    {
        rwLatch.ExitReadLock();
    }
}