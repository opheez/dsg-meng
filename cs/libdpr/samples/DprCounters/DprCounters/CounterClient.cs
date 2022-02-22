using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using FASTER.libdpr;

namespace DprCounters
{
    /// <summary>
    /// Client to a cluster of CounterServers. DPR-capable. 
    /// </summary>
    public class CounterClient
    {
        private DprClient client;
        private Dictionary<Worker, IPEndPoint> cluster;

        public CounterClient(IDprFinder dprFinder, IDprFinder dprFinderApi)
        {
            client = new DprClient(dprFinder, dprFinderApi);
        }
        
        public CounterClientSession GetSession()
        {
            return new(client.GetSession(Guid.NewGuid()), client.FetchCluster());
        }

        public void RefreshDpr()
        {
            client.RefreshDprView();
        }
    }
}