﻿﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using FASTER.serverless;

namespace DprMicrobench
{
    public class SimulatedDprWorker
    {
        private IDprManager dprManager;
        private IWorkloadGenerator generator;
        private IList<Worker> workers;
        private Worker me;
        private double delayProb;
        private Random random = new Random();

        private SortedList<long, long> versionPersistent;
        private SortedList<long, long> versionRecoverable;
        private Stopwatch stopwatch;
        
        public SimulatedDprWorker(IDprManager dprManager, IWorkloadGenerator generator, 
            IList<Worker> workers, Worker me,
            double delayProb)
        {
            this.dprManager = dprManager;
            this.generator = generator;
            this.workers = workers;
            this.me = me;
            this.delayProb = delayProb;
        }

        public List<long> ComputeVersionCommitLatencies()
        {
            var result = new List<long>(versionRecoverable.Count);

            for (var i = 0; i < versionRecoverable.Count; i++)
            {
                Debug.Assert(versionPersistent.Keys[i] == versionRecoverable.Keys[i]);
                result.Add(versionRecoverable.Values[i] - versionPersistent.Values[i]);
            }

            return result;
        }

        public void RunContinuously(int runSeconds, int averageMilli, int delayMilli)
        {
            versionPersistent = new SortedList<long, long>(runSeconds * 1000 / averageMilli);
            versionRecoverable = new SortedList<long, long>(runSeconds * 1000 / averageMilli);
            stopwatch = new Stopwatch();
            stopwatch.Start();
            var currentVersion = 1L;
            var safeVersion = 0L;
            while (stopwatch.ElapsedMilliseconds < runSeconds * 1000)
            {
                var elapsed = stopwatch.ElapsedMilliseconds;
                if (elapsed > runSeconds * 1000) break;
                dprManager.Refresh();
                var currentSafeVersion = dprManager.SafeVersion(me);
                for (var i = safeVersion + 1; i <= currentSafeVersion; i++)
                {
                    if (versionPersistent.Keys[versionRecoverable.Count] == i)
                        versionRecoverable.Add(i, elapsed);
                }
                safeVersion = currentSafeVersion;
                var expectedVersion = 1 + elapsed / averageMilli;
                if (expectedVersion > currentVersion)
                {
                    var deps = generator.GenerateDependenciesOneRun(workers, me, currentVersion);
                    if (random.NextDouble() < delayProb)
                        Thread.Sleep(delayMilli);
                    elapsed = stopwatch.ElapsedMilliseconds;
                    versionPersistent.Add(currentVersion, elapsed);
                    dprManager.ReportNewPersistentVersion(new WorkerVersion(me, currentVersion), deps);
                    currentVersion = expectedVersion;
                }
            }

            stopwatch.Stop();
            dprManager.Clear();
        }
    }
}