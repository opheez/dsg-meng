using System;
using System.Diagnostics;
using System.Threading;
using FASTER.core;

namespace FASTER.core
{
    internal class SimpleVersionSchemeStateMachine : IVersionSchemeStateMachine
    {
        private Action<long, long> criticalSection;
        private long toVersion = -1;

        public SimpleVersionSchemeStateMachine(Action<long, long> criticalSection, long toVersion = -1)
        {
            this.criticalSection = criticalSection;
            this.toVersion = toVersion;
        }

        public long ToVersion() => toVersion;
        
        public bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState)
        {
            Debug.Assert(currentState.Phase == VersionSchemeState.REST);
            nextState = VersionSchemeState.Make(VersionSchemeState.REST, toVersion == -1 ? currentState.Version + 1 : toVersion);
            return true;
        }

        public void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState)
        {
            Debug.Assert(fromState.Phase == VersionSchemeState.REST && toState.Phase == VersionSchemeState.REST);
            criticalSection(fromState.Version, toState.Version);
        }
    }
    
    public class SimpleVersionScheme
    {
        private EpochProtectedVersionScheme versionScheme;

        public SimpleVersionScheme()
        {
            versionScheme = new EpochProtectedVersionScheme();
        }

        public long Enter() => versionScheme.Enter().Version;

        public void Leave() => versionScheme.Leave();

        public long Refresh() => versionScheme.Refresh().Version;

        public bool AdvanceVersion(Action<long, long> criticalSection, long toVersion = -1)
        {
            return versionScheme.ExecuteStateMachine(new SimpleVersionSchemeStateMachine(criticalSection, toVersion));
        }
    }
}