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
        private IPAddress clusterIp;
        Dictionary<Worker, EndPoint> cluster = new Dictionary<Worker, EndPoint>();

        public CounterClient(IDprFinder dprFinder, string ip)
        {
            client = new DprClient(dprFinder);
            this.clusterIp = IPAddress.Parse(ip);
        }

        public Dictionary<Worker, EndPoint> GetCluster()
        {
            return cluster;
        }
        
        private void FetchCluster()
        {
            cluster = new Dictionary<Worker, EndPoint>();
            var fetchedClusterInfo = client.FetchCluster();
            foreach (var (key, value) in fetchedClusterInfo)
            {
                cluster[key] = new IPEndPoint(clusterIp, value.Item1);
            }
        }

        public CounterClientSession GetSession()
        {   
            FetchCluster();
            return new(client.GetSession(Guid.NewGuid()), cluster);
        }

        public void RefreshDpr()
        {
            client.RefreshDprView();
            // FetchCluster();
        }
    }
}