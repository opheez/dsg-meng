using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// The current state of a state-machine operation such as a checkpoint.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct VersionSchemeState
    {
        public const byte REST = 0;
        const int kTotalSizeInBytes = 8;
        const int kTotalBits = kTotalSizeInBytes * 8;

        // Phase
        const int kPhaseBits = 8;
        const int kPhaseShiftInWord = kTotalBits - kPhaseBits;
        const long kPhaseMaskInWord = ((1L << kPhaseBits) - 1) << kPhaseShiftInWord;
        const long kPhaseMaskInInteger = (1L << kPhaseBits) - 1;

        // Version
        const int kVersionBits = kPhaseShiftInWord;
        const long kVersionMaskInWord = (1L << kVersionBits) - 1;

        /// <summary>Internal intermediate state of state machine</summary>
        private const byte kIntermediateMask = 128;

        [FieldOffset(0)] internal long Word;

        public byte Phase
        {
            get { return (byte) ((Word >> kPhaseShiftInWord) & kPhaseMaskInInteger); }
            set
            {
                Word &= ~kPhaseMaskInWord;
                Word |= (((long) value) & kPhaseMaskInInteger) << kPhaseShiftInWord;
            }
        }

        public bool IsIntermediate() => (Phase & kIntermediateMask) != 0;

        public long Version
        {
            get { return Word & kVersionMaskInWord; }
            set
            {
                Word &= ~kVersionMaskInWord;
                Word |= value & kVersionMaskInWord;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VersionSchemeState Copy(ref VersionSchemeState other)
        {
            var info = default(VersionSchemeState);
            info.Word = other.Word;
            return info;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VersionSchemeState Make(byte phase, long version)
        {
            var info = default(VersionSchemeState);
            info.Phase = phase;
            info.Version = version;
            return info;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VersionSchemeState MakeIntermediate(VersionSchemeState state)
            => Make((byte) (state.Phase | kIntermediateMask), state.Version);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void RemoveIntermediate(ref VersionSchemeState state)
        {
            state.Phase = (byte) (state.Phase & ~kIntermediateMask);
        }

        internal static bool Equal(VersionSchemeState s1, VersionSchemeState s2)
        {
            return s1.Word == s2.Word;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"[{Phase},{Version}]";
        }

        /// <summary>
        /// Compare the current <see cref="SystemState"/> to <paramref name="obj"/> for equality if obj is also a <see cref="SystemState"/>
        /// </summary>
        public override bool Equals(object obj)
        {
            return obj is VersionSchemeState other && Equals(other);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return Word.GetHashCode();
        }

        /// <summary>
        /// Compare the current <see cref="SystemState"/> to <paramref name="other"/> for equality
        /// </summary>
        private bool Equals(VersionSchemeState other)
        {
            return Word == other.Word;
        }

        /// <summary>
        /// Equals
        /// </summary>
        public static bool operator ==(VersionSchemeState left, VersionSchemeState right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Not Equals
        /// </summary>
        public static bool operator !=(VersionSchemeState left, VersionSchemeState right)
        {
            return !(left == right);
        }
    }

    public abstract class VersionSchemeStateMachine
    {
        private long toVersion;
        protected internal long actualToVersion = -1;
        protected EpochProtectedVersionScheme epvs;

        protected VersionSchemeStateMachine(EpochProtectedVersionScheme epvs, long toVersion = -1)
        {
            this.epvs = epvs;
            this.toVersion = toVersion;
            actualToVersion = toVersion;
        }
        
        public long ToVersion() => toVersion;
        
        public abstract bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState);
        
        public abstract void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState);

        public abstract void AfterEnteringState(VersionSchemeState state);
    }

    public enum StateMachineExecutionStatus
    {
        OK, RETRY, FAIL
    }

    public class EpochProtectedVersionScheme
    {
        private LightEpoch epoch;
        private VersionSchemeState state;
        private VersionSchemeStateMachine currentMachine;
        public EpochProtectedVersionScheme(LightEpoch epoch)
        {
            this.epoch = epoch;
            state = VersionSchemeState.Make(VersionSchemeState.REST, 1);
            currentMachine = null;
        }

        public VersionSchemeState CurrentState() => state;
        
        // Atomic transition from expectedState -> nextState
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MakeTransition(VersionSchemeState expectedState, VersionSchemeState nextState)
        {
            if (Interlocked.CompareExchange(ref state.Word, nextState.Word, expectedState.Word) !=
                expectedState.Word) return false;
            Debug.WriteLine("Moved to {0}, {1}", nextState.Phase, nextState.Version);
            return true;
        }

        public VersionSchemeState Enter()
        {
            epoch.Resume();
            TryStepStateMachine(null);

            VersionSchemeState result;
            while (true)
            {
                result = state;
                if (!result.IsIntermediate()) break;
                epoch.Suspend();
                Thread.Yield();
                epoch.Resume();
            }
            
            return result;
        }

        public VersionSchemeState Refresh()
        {
            epoch.ProtectAndDrain();
            VersionSchemeState result = default;
            TryStepStateMachine(null);

            while (true)
            {
                result = state;
                if (!result.IsIntermediate()) break;
                epoch.Suspend();
                Thread.Yield();
                epoch.Resume();
            }
            return result;
        }

        public void Leave()
        {
            epoch.Suspend();
        }

        internal void TryStepStateMachine(VersionSchemeStateMachine expectedMachine)
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
            // Avoid upfront memory allocation by going to a function
            StepMachineHeavy(machineLocal, oldState, nextState);

            // Ensure that state machine is able to make progress if this thread is the only active thread
            if (!epoch.ThisInstanceProtected())
            {
                epoch.Resume();
                epoch.Suspend();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void StepMachineHeavy(VersionSchemeStateMachine machineLocal, VersionSchemeState old, VersionSchemeState next)
        {
            epoch.BumpCurrentEpoch(() =>
            {
                machineLocal.OnEnteringState(old, next);
                var success = MakeTransition(VersionSchemeState.MakeIntermediate(old), next);
                machineLocal.AfterEnteringState(next);
                Debug.Assert(success);
                TryStepStateMachine(machineLocal);
            });
        }

        public StateMachineExecutionStatus TryExecuteStateMachine(VersionSchemeStateMachine stateMachine)
        {
            if (stateMachine.ToVersion() != -1 && stateMachine.ToVersion() <= state.Version) return StateMachineExecutionStatus.FAIL;
            var actualStateMachine = Interlocked.CompareExchange(ref currentMachine, stateMachine, null);
            if (actualStateMachine == null)
            {
                // Compute the actual ToVersion of state machine
                stateMachine.actualToVersion =
                    stateMachine.ToVersion() == -1 ? state.Version + 1 : stateMachine.ToVersion();
                // Trigger one initial step to begin the process
                TryStepStateMachine(stateMachine);
                return StateMachineExecutionStatus.OK;
            }
            
            // Otherwise, need to check that we are not a duplicate attempt to increment version
            if (stateMachine.ToVersion() != -1 && currentMachine.actualToVersion >= stateMachine.ToVersion()) 
                return StateMachineExecutionStatus.FAIL;

            return StateMachineExecutionStatus.RETRY;
        }


        public bool ExecuteStateMachine(VersionSchemeStateMachine stateMachine)
        {
            if (epoch.ThisInstanceProtected())
                throw new FasterException("unsafe to execute a state machine blockingly when under protection"); 
            StateMachineExecutionStatus status;
            do
            {
                status = TryExecuteStateMachine(stateMachine);
            } while (status == StateMachineExecutionStatus.RETRY);

            return status == StateMachineExecutionStatus.OK;
        }

        public bool ExecuteStateMachineToFinish(VersionSchemeStateMachine stateMachine)
        {
            if (!ExecuteStateMachine(stateMachine)) return false;
            while (state.Version != stateMachine.actualToVersion || state.Phase != VersionSchemeState.REST)
            {
                TryStepStateMachine(stateMachine);
                Thread.Yield();
            }

            return true;
        }
    }
}