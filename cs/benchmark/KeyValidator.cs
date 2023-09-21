using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using FASTER.core;

namespace FASTER.benchmark
{
    public class BaselineKeyValidator
    {
        private ConcurrentDictionary<int, byte> validationMap;
        private ReaderWriterLockSlim latch;

        public BaselineKeyValidator()
        {
            validationMap = new ConcurrentDictionary<int, byte>();
            latch = new ReaderWriterLockSlim();
            for (var i = 0; i < 16384; i++)
                validationMap.TryAdd(i, 0);
        }
        
        public bool ValidateKey(long key)
        {
            try
            {
                latch.EnterReadLock();
                var hashBucket = key.GetHashCode() & ((1 << 14) - 1);
                return validationMap.ContainsKey(hashBucket);
            }
            finally
            {
                latch.ExitReadLock();
            }
        }
    }

    public class EpvsKeyValidator
    {
        private Dictionary<int, byte> validationMap;
        private EpochProtectedVersionScheme svs;

        public EpvsKeyValidator()
        {
            validationMap = new Dictionary<int, byte>();
            svs = new EpochProtectedVersionScheme(new LightEpoch());
            for (var i = 0; i < 16384; i++)
                validationMap.Add(i, 0);
            
        }

        public bool ValidateKey(long key)
        {
            try
            {
                svs.Enter();
                var hashBucket = key.GetHashCode() & ((1 << 14) - 1);
                return validationMap.ContainsKey(hashBucket);
            }
            finally
            {
                svs.Leave();
            }
        }
    }
}