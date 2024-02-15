using System.Collections.Concurrent;

namespace darq;

public class DarqBackgroundWorkerPoolSettings
{
    public int numWorkers = 2;
}

public class DarqBackgroundWorkerPool : IDisposable
{
    private ConcurrentQueue<Func<Task>> workQueue;
    private ManualResetEventSlim terminationStart;
    private CountdownEvent terminationComplete;

    public DarqBackgroundWorkerPool(DarqBackgroundWorkerPoolSettings settings)
    {
        workQueue = new ConcurrentQueue<Func<Task>>();
        terminationStart = new ManualResetEventSlim();
        terminationComplete = new CountdownEvent(settings.numWorkers);
        for (var i = 0; i < settings.numWorkers; i++)
        {
            Task.Run(async () =>
            {
                while (!terminationStart.IsSet)
                {
                    while (workQueue.TryDequeue(out var task))
                        await task();
                    await Task.Yield();
                }

                terminationComplete.Signal();
            });
        }
    }

    public void Dispose()
    {
        terminationStart.Set();
        terminationComplete.Wait();
    }

    public void AddWork(Func<Task> work)
    {
        if (!terminationStart.IsSet)
            workQueue.Enqueue(work);
    }
}