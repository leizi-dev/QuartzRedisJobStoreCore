using Quartz;
using Quartz.Spi;

namespace QuartzRedisJobStoreCore.JobStore
{
    public class SampleSignaler : ISchedulerSignaler
    {
        internal int fMisfireCount;

        public Task NotifyTriggerListenersMisfired(
            ITrigger trigger,
            CancellationToken cancellationToken = default)
        {
            fMisfireCount++;
            return TaskUtil.CompletedTask;
        }

        public Task NotifySchedulerListenersFinalized(
            ITrigger trigger,
            CancellationToken cancellationToken = default)
        {
            return TaskUtil.CompletedTask;
        }

        public Task NotifySchedulerListenersError(
            string message,
            SchedulerException jpe,
            CancellationToken cancellationToken = default)
        {
            return TaskUtil.CompletedTask;
        }

        public Task NotifySchedulerListenersJobDeleted(
            JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            return TaskUtil.CompletedTask;
        }

        void ISchedulerSignaler.SignalSchedulingChange(DateTimeOffset? candidateNewNextFireTimeUtc, CancellationToken cancellationToken)
        {
            Task.FromResult(true);
        }
    }
}
