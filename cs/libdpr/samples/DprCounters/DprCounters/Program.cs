using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;
using FASTER.libdpr;

namespace DprCounters
{
    class Program
    {
        static void Main(string[] args)
        {
            var cluster = new Cluster("127.0.0.1", 15721, "127.0.0.1", 15722);
            Dictionary<Worker, IPEndPoint> temp = new Dictionary<Worker, IPEndPoint>();

            // Start two counter servers
            // TODO(Nikola): add a enum that specifies which type of worker should be started
            cluster.AddWorker(0, "127.0.0.1", 15723);
            cluster.AddWorker(1, "127.0.0.1", 15724);

            // Start a client that performs some operations
            var client = new CounterClient(new EnhancedDprFinder("127.0.0.1", 15721), new EnhancedDprFinder("127.0.0.1", 15722)); // passing the api ip/port to the client
            // TODO(Nikola): actually get a list of workers from the session and use that
            var session = client.GetSession();            
            var op0 = session.Increment(new Worker(0), 42, out _);
            var op1 = session.Increment(new Worker(1), 2, out _);
            var op2 = session.Increment(new Worker(1), 7, out _);
            var op3 = session.Increment(new Worker(0), 10, out _);
            while (!session.Committed(op3))
                client.RefreshDpr();

            // Shutdown
            // the below two operations could be used for individual worker, but no need since we're stopping the entire cluster
            // cluster.DeleteWorker(0);
            // cluster.DeleteWorker(1);
            cluster.Stop();
        }
    }
}