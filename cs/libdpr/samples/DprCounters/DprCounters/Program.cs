using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;
using FASTER.libdpr;

namespace DprCounters
{
    class Program
    {
        static void runWithoutKubernetes() 
        {
            var cluster = new Cluster("127.0.0.1", 15721, "127.0.0.1", 15722);
            Dictionary<Worker, IPEndPoint> temp = new Dictionary<Worker, IPEndPoint>();

            // Start two counter servers
            cluster.AddWorker(0, "127.0.0.1", 15723);
            cluster.AddWorker(1, "127.0.0.1", 15724);

            // Start a client that performs some operations
            var client = new CounterClient(new EnhancedDprFinder("127.0.0.1", 15721), new EnhancedDprFinder("127.0.0.1", 15722)); // passing the api ip/port to the client

            var session = client.GetSession();            
            var op0 = session.Increment(new Worker(0), 42, out _);
            var op1 = session.Increment(new Worker(1), 2, out _);
            var op2 = session.Increment(new Worker(1), 7, out _);
            var op3 = session.Increment(new Worker(0), 10, out _);
            while (!session.Committed(op3))
                client.RefreshDpr();

            // Shutdown
            cluster.Stop();
        }

        static void runCounterServer(string backendIp, int backendPort, int guid) 
        {   
            Worker worker = new Worker(guid);
            var wServer = new CounterServer("0.0.0.0", 80, worker, "/DprCounters/data/worker" + guid.ToString() + "/",
                new EnhancedDprFinder(backendIp, backendPort));
            wServer.RunServer();
        }

        static void runBackendServer()
        {
            var localDevice1 = new ManagedLocalStorageDevice("/DprCounters/data/dpr1.dat", deleteOnClose: true);
            var localDevice2 = new ManagedLocalStorageDevice("/DprCounters/data/dpr2.dat", deleteOnClose: true);
            var device = new PingPongDevice(localDevice1, localDevice2);
            EnhancedDprFinderServer backendServerFinder = new EnhancedDprFinderServer("0.0.0.0", 3000, new EnhancedDprFinderBackend(device));
            backendServerFinder.StartServer();
        }

        static void runClient()
        {
            var client = new CounterClient(new EnhancedDprFinder("dpr-finder-0.dpr-finder-svc", 3000));
            Dictionary<Worker, EndPoint> cluster = new Dictionary<Worker, EndPoint>();
            Worker w0 = new Worker(0);
            Worker w1 = new Worker(1);
            cluster[w0] = new DnsEndPoint("counter-0.counter-server-svc", 80);
            cluster[w1] = new DnsEndPoint("counter-1.counter-server-svc", 80);
            client.RefreshDpr();
            var session = client.GetSession(cluster);        
            var op0 = session.Increment(new Worker(0), 42, out _);
            var op1 = session.Increment(new Worker(1), 2, out _);
            var op2 = session.Increment(new Worker(1), 7, out _);
            var op3 = session.Increment(new Worker(0), 10, out _);
            while (!session.Committed(op3))
                client.RefreshDpr();
        }

        static void Main(string[] args)
        {
            Console.Out.WriteLine("TESTTTTT");
            if(args.Length == 0 || args[0] == "single")
            {
                runWithoutKubernetes();
                return;
            }
            if(args[0] == "counter")
            {
                string DPR_FINDER_SERVICE = "dpr-finder-0.dpr-finder-svc";
                int DPR_FINDER_PORT = 3000;
                string name = Environment.GetEnvironmentVariable("POD_NAME");
                int guid = Int32.Parse(name.Split("-")[1]);
                runCounterServer(DPR_FINDER_SERVICE, DPR_FINDER_PORT, guid);
                return;
            }
            if(args[0] == "backend")
            {   
                runBackendServer();
                return;
            }
            if(args[0] == "client")
            {
                runClient();
                return;
            }
        }
    }
}