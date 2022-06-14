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
        private IPAddress ip;
        Dictionary<Worker, EndPoint> cluster = new Dictionary<Worker, EndPoint>();

        public CounterClient(string ip, int port)
        {
            client = new DprClient(new EnhancedDprFinder(ip, port));
            this.ip = IPAddress.Parse(ip);
        }

        public CounterClient(IDprFinder dprFinder)
        {
            // used when the Dpr Finder uses DNS for when the client is inside the cluster
            // usually for testing purposes
            client = new DprClient(dprFinder);
            this.ip = null;
        }

        public Dictionary<Worker, EndPoint> getCluster()
        {
            return cluster;
        }
        
        public CounterClientSession GetSession()
        {
            var fetchedClusterInfo = client.FetchCluster();
            foreach (var (key, value) in fetchedClusterInfo)
            {
                cluster[key] = new IPEndPoint(ip, value.Item1);
            }
            return new(client.GetSession(Guid.NewGuid()), cluster);
        }

        public CounterClientSession GetSession(Dictionary<Worker, EndPoint> specifiedCluster)
        {
            return new(client.GetSession(Guid.NewGuid()), specifiedCluster);
        }

        public void RefreshDpr()
        {
            client.RefreshDprView();
        }
    }
}