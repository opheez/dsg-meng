using System;
using System.Diagnostics;

namespace FASTER.core
{
    internal class SimpleVersionSchemeStateMachine : VersionSchemeStateMachine
    {
        private Action<long, long> criticalSection;

        public SimpleVersionSchemeStateMachine(Action<long, long> criticalSection, EpochProtectedVersionScheme epvs, long toVersion = -1) : base(epvs, toVersion)
        {
            this.criticalSection = criticalSection;
        }
        
        public override bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState)
        {
            Debug.Assert(currentState.Phase == VersionSchemeState.REST);
            nextState = VersionSchemeState.Make(VersionSchemeState.REST, ToVersion() == -1 ? currentState.Version + 1 : ToVersion());
            return true;
        }

        public override void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState)
        {
            Debug.Assert(fromState.Phase == VersionSchemeState.REST && toState.Phase == VersionSchemeState.REST);
            criticalSection(fromState.Version, toState.Version);
        }

        public override void AfterEnteringState(VersionSchemeState state) {}
    }
    
    public class SimpleVersionScheme
    {
        private EpochProtectedVersionScheme versionScheme;

        public SimpleVersionScheme(LightEpoch epoch)
        {
            versionScheme = new EpochProtectedVersionScheme(epoch);
        }

        public long Enter() => versionScheme.Enter().Version;

        public void Leave() => versionScheme.Leave();

        public long Refresh() => versionScheme.Refresh().Version;

        
        public bool AdvanceVersion(Action<long, long> criticalSection, long toVersion = -1)
        {
            return versionScheme.ExecuteStateMachine(new SimpleVersionSchemeStateMachine(criticalSection, versionScheme, toVersion));
        }

        public StateMachineExecutionStatus TryAdvanceVersion(Action<long, long> criticalSection, long toVersion = -1)
        {
            return versionScheme.TryExecuteStateMachine(
                new SimpleVersionSchemeStateMachine(criticalSection, versionScheme, toVersion));
        }
    }
}