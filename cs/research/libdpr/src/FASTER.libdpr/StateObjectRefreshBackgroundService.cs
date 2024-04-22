using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FASTER.libdpr
{
    public class StateObjectRefreshBackgroundService : BackgroundService
    {
        private ILogger<StateObjectRefreshBackgroundService> logger;
        private CancellationToken stoppingToken;
        private int dispatchedTasks;
        private StateObject defaultStateObject;

        public StateObjectRefreshBackgroundService(ILogger<StateObjectRefreshBackgroundService> logger,
            StateObject defaultObject = null)
        {
            this.logger = logger;
            this.defaultStateObject = defaultObject;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Refresh background service is starting");
            this.stoppingToken = stoppingToken;
            if (defaultStateObject != null)
                RegisterRefreshTask(defaultStateObject);
            await Task.Delay(Timeout.Infinite, this.stoppingToken);
            while (dispatchedTasks != 0)
                await Task.Yield();
            logger.LogInformation("Refresh background service is winding down");
        }

        public void RegisterRefreshTask(StateObject toRegister)
        {
            if (defaultStateObject != null && toRegister != defaultStateObject)
                throw new InvalidOperationException(
                    "Runtime creation if refresh task is only allowed if no singleton default state object is configured");
            if (stoppingToken.IsCancellationRequested) throw new TaskCanceledException();
            Interlocked.Increment(ref dispatchedTasks);
            Task.Run(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    // Must not begin refreshing before the state object is connected
                    if (toRegister.ConnectedToCluster())
                        toRegister.Refresh();
                    await Task.Yield();
                }

                Interlocked.Decrement(ref dispatchedTasks);
            });
        }
    }
}