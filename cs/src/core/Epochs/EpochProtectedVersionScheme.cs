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

    public abstract class VersionSchemeStateMachineBase
    {
        private long toVersion;
        private EpochProtectedVersionScheme epvs;

        protected VersionSchemeStateMachineBase(EpochProtectedVersionScheme epvs, long toVersion = -1)
        {
            this.epvs = epvs;
            this.toVersion = toVersion;
        }

        protected void NotifyAvailableState() => epvs.TryStepStateMachine();

        public long ToVersion() => toVersion;
        
        public abstract bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState);
        
        public abstract void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState);
    }

    public class EpochProtectedVersionScheme
    {
        private LightEpoch epoch;
        private VersionSchemeState state;
        private VersionSchemeStateMachineBase currentMachine;

        public EpochProtectedVersionScheme()
        {
            epoch = new LightEpoch();
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
            while (state.IsIntermediate())
                Thread.Yield();
            TryStepStateMachine();
            return state;
        }

        public VersionSchemeState Refresh()
        {
            epoch.ProtectAndDrain();
            while (state.IsIntermediate())
                Thread.Yield();
            TryStepStateMachine();
            return state;
        }

        public void Leave()
        {
            epoch.Suspend();
        }


        internal bool TryStepStateMachine()
        {
            var oldState = state;
            if (oldState.IsIntermediate() || !currentMachine.GetNextStep(oldState, out var nextState)) return false;
            
            var intermediate = VersionSchemeState.MakeIntermediate(oldState);
            if (!MakeTransition(oldState, intermediate)) return false;
            epoch.BumpCurrentEpoch(() =>
            {
                currentMachine.OnEnteringState(oldState, nextState);
                var success = MakeTransition(intermediate, nextState);
                Debug.Assert(success);
                if (nextState.Phase == VersionSchemeState.REST)
                    currentMachine = null;
            });

            // Ensure that state machine is able to make progress if this thread is the only active thread
            if (!epoch.ThisInstanceProtected())
            {
                epoch.Resume();
                epoch.Suspend();
            }
            return true;
        }

        public bool ExecuteStateMachine(VersionSchemeStateMachineBase stateMachine)
        {
            while (Interlocked.CompareExchange(ref currentMachine, stateMachine, null) != null)
                Thread.Yield();

            if (currentMachine.ToVersion() != -1 && currentMachine.ToVersion() > state.Version)
            {
                currentMachine = null;
                return false;
            }

            Debug.Assert(state.Phase == VersionSchemeState.REST);
            Debug.Assert(stateMachine.GetNextStep(state, out _));
            // Trigger one initial step to begin the process
            TryStepStateMachine();
            return true;
        }
    }
}