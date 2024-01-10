namespace FASTER.libdpr
{
    public class DprWorkerOptions
    {
        public WorkerId Me;
        public IDprFinder DprFinder = null;
        public long CheckpointPeriodMilli = 5;
        public long RefreshPeriodMilli = 5;
    }
}