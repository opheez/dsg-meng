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
        static string DPR_FINDER_IP = "192.168.49.2"; // equal to $(minikube ip), which is the persistent IP of the DPR Finder

        static void RunWithoutKubernetes() 
        {
            // Use a simple pair of in-memory storage to back our DprFinder server for now. Start a local DPRFinder
            // server for the cluster
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var device = new PingPongDevice(localDevice1, localDevice2);
            using var dprFinderServer = new EnhancedDprFinderServer("127.0.0.1", 15721, new EnhancedDprFinderBackend(device));
            dprFinderServer.StartServer();

            var w0 = new Worker(0);
            var w0Server = new CounterServer("127.0.0.1", 15722, new WorkerInformation(w0, 15722, 0), "worker0/",
                new EnhancedDprFinder("127.0.0.1", 15721));
            var w0Thread = new Thread(w0Server.RunServer);
            w0Thread.Start();


            var w1 = new Worker(1);
            var w1Server = new CounterServer("127.0.0.1", 15723, new WorkerInformation(w1, 15723, 0), "worker1/",
                new EnhancedDprFinder("127.0.0.1", 15721));
            var w1Thread = new Thread(w1Server.RunServer);
            w1Thread.Start();

            Thread.Sleep(5000); // needs to sleep here until the cluster gets established
            // TODO(Nikola): Handle things in the cluster being down from the client side as well so it doesn't fail

            // Start a client that performs some operations
            var client = new CounterClient("127.0.0.1", 15721);
            var session = client.GetSession();            
            var op0 = session.Increment(w0, 42, out _);
            var op1 = session.Increment(w1, 2, out _);
            var op2 = session.Increment(w1, 7, out _);
            var op3 = session.Increment(w0, 10, out _);
            while (!session.Committed(op3))
                client.RefreshDpr();

            // Shutdown
            w0Server.StopServer();
            w0Thread.Join();
            
            w1Server.StopServer();
            w1Thread.Join();
            Console.WriteLine("SUCCESS");
        }

        static void RunCounterServer(string backendIp, int backendPort, int guid, int frontendPort) 
        {   
            // Worker worker = new Worker(guid);
            WorkerInformation workerInfo = new WorkerInformation(new Worker(guid), frontendPort, 0);
            var wServer = new CounterServer("0.0.0.0", 80, workerInfo, "/DprCounters/data/worker" + guid.ToString() + "/",
                new EnhancedDprFinder(backendIp, backendPort));
            wServer.RunServer();
        }

        static void RunBackendServer()
        {
            var localDevice1 = new ManagedLocalStorageDevice("/DprCounters/data/dpr1.dat", deleteOnClose: true);
            var localDevice2 = new ManagedLocalStorageDevice("/DprCounters/data/dpr2.dat", deleteOnClose: true);
            var device = new PingPongDevice(localDevice1, localDevice2);
            EnhancedDprFinderServer backendServerFinder = new EnhancedDprFinderServer("0.0.0.0", 3000, new EnhancedDprFinderBackend(device));
            backendServerFinder.StartServer();
        }

        static void RunClient()
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

        static void RunClientLeft()
        {
            var client = new CounterClient(DPR_FINDER_IP, 6379);
            Worker w0 = new Worker(0);
            Worker w1 = new Worker(1);
            client.RefreshDpr();
            var session = client.GetSession();
            var cluster = client.getCluster();
            var op0 = session.Increment(new Worker(0), 42, out _);
            var op1 = session.Increment(new Worker(1), 2, out _);
            var op2 = session.Increment(new Worker(1), 7, out _);
            var op3 = session.Increment(new Worker(0), 10, out _);
            while (!session.Committed(op3))
                client.RefreshDpr();
        }

        static void Main(string[] args)
        {
            Console.Out.WriteLine("TEST");
            if(args.Length == 0 || args[0] == "single")
            {
                RunWithoutKubernetes();
                return;
            }
            if(args[0] == "counter")
            {
                string DPR_FINDER_SERVICE = "dpr-finder-0.dpr-finder-svc";
                int DPR_FINDER_PORT = 3000;
                string name = Environment.GetEnvironmentVariable("POD_NAME");
                int guid = Int32.Parse(name.Split("-")[1]);
                string frontPort = Environment.GetEnvironmentVariable("FRONTEND_PORT");
                RunCounterServer(DPR_FINDER_SERVICE, DPR_FINDER_PORT, guid, Int32.Parse(frontPort));
                return;
            }
            if(args[0] == "backend")
            {   
                RunBackendServer();
                return;
            }
            if(args[0] == "client")
            {
                RunClient();
                return;
            }
            if(args[0] == "clientLeft")
            {
                Console.WriteLine("Starting client from the outside");
                RunClientLeft();
                Console.WriteLine("SUCCESS!!!");
            }
        }
    }
}