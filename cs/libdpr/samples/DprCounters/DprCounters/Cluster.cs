using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.libdpr;

namespace DprCounters
{
    public class Cluster 
    {
        
        private string ipFinder, ipApi;
        private int portFinder, portApi;
        private EnhancedDprFinderServer backendServerFinder, backendServerApi;
        private ClusterBackend apiBackend;
        private Dictionary<Worker, EndPoint> workers = new Dictionary<Worker, EndPoint>();
        private Dictionary<Worker, Tuple<CounterServer, Thread>> servers = new Dictionary<Worker, Tuple<CounterServer, Thread>>();

        public Cluster() 
        {
            // Use a simple pair of in-memory storage to back our DprFinder server for now. Start a local DPRFinder
            // server for the cluster
            this.ipFinder = "127.0.0.1";
            this.ipApi = "127.0.0.1";
            this.portFinder = 15721;
            this.portApi = 15722;
            startBackend();
        }

        public Cluster(string ipFinder, int portFinder, string ipApi, int portApi) 
        {
            this.ipFinder = ipFinder;
            this.portFinder = portFinder;
            this.ipApi = ipApi;
            this.portApi = portApi;
            startBackend();
        }

        private void startBackend()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var device = new PingPongDevice(localDevice1, localDevice2);
            apiBackend = new ClusterBackend(workers);
            backendServerFinder = new EnhancedDprFinderServer(ipFinder, portFinder, new EnhancedDprFinderBackend(device));
            backendServerFinder.StartServer();
            backendServerApi = new EnhancedDprFinderServer(ipApi, portApi, null, apiBackend);
            backendServerApi.StartServer();
        }

        public void AddWorker(long guid)
        {
            string workerIp = this.ipFinder;
            int workerPort = this.portApi + 1 + (int)guid;

            AddWorker(guid, workerIp, workerPort);
        }

        public void AddWorker(long guid, string workerIp, int workerPort)
        {
            Worker worker = new Worker(guid);

            workers.Add(worker, new IPEndPoint(IPAddress.Parse(workerIp), workerPort));
            var wServer = new CounterServer(workerIp, workerPort, worker, "worker" + guid.ToString() + "/",
                new EnhancedDprFinder(this.ipFinder, this.portFinder));
            var wThread = new Thread(wServer.RunServer);
            servers.Add(worker, new Tuple<CounterServer, Thread>(wServer, wThread));
            wThread.Start();
            apiBackend.Refresh();
        }

        public void DeleteWorker(long guid)
        {
            Worker worker = new Worker(guid);
            DeleteWorker(worker);
        }

        public void DeleteWorker(Worker worker)
        {
            servers[worker].Item1.StopServer();
            servers[worker].Item2.Join();
            servers.Remove(worker);
            workers.Remove(worker);
            apiBackend.Refresh();
        }

        public void Stop() 
        {
            foreach(KeyValuePair<Worker, Tuple<CounterServer, Thread>> entry in servers) {
                entry.Value.Item1.StopServer();
                entry.Value.Item2.Join();
            }
            backendServerFinder.Dispose();
            backendServerApi.Dispose();
        }
    }
}