namespace FASTER.core
{
    public enum FasterEpvsPhase : byte
    {
        REST = VersionSchemeState.REST, LOG_FLUSH, META_FLUSH 
    }
    
    
    public class FasterEpvsStateMachine<Key, Value> : VersionSchemeStateMachine
    {
        private FasterKV<Key, Value> faster;
        
        public FasterEpvsStateMachine(FasterKV<Key, Value> faster, EpochProtectedVersionScheme epvs, long toVersion = -1) : base(epvs, toVersion)
        {
            this.faster = faster;
        }

        public override bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState)
        {
            switch ((FasterEpvsPhase) currentState.Phase)
            {
                case FasterEpvsPhase.REST:
                    nextState = VersionSchemeState.Make((byte) FasterEpvsPhase.LOG_FLUSH, currentState.Version);
                    return true;
                case FasterEpvsPhase.LOG_FLUSH:
                    nextState = VersionSchemeState.Make((byte) FasterEpvsPhase.META_FLUSH, currentState.Version);
                    return faster.hlog.FlushedUntilAddress >= faster._hybridLogCheckpoint.info.finalLogicalAddress;
                case FasterEpvsPhase.META_FLUSH:
                    nextState = VersionSchemeState.Make((byte) FasterEpvsPhase.META_FLUSH, currentState.Version);

                    r
                    
            }
        }

        public override void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState)
        {
            throw new System.NotImplementedException();
        }

        public override void AfterEnteringState(VersionSchemeState state)
        {
            throw new System.NotImplementedException();
        }
    }
}