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
        
        public CounterClientSession GetSession()
        {
            var fetchedClusterInfo = client.FetchCluster();
            Dictionary<Worker, EndPoint> formattedCluster = new Dictionary<Worker, EndPoint>();
            foreach (var (key, value) in fetchedClusterInfo)
            {
                formattedCluster[key] = new IPEndPoint(ip, value.Item1);
            }
            return new(client.GetSession(Guid.NewGuid()), formattedCluster);
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