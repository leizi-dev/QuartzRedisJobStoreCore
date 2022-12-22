using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QuartzRedisJobStoreCore.UnitTest
{
    public class RedisDbJobStoreTests: BaseStoreTests, IDisposable
    {
        private IScheduler _scheduler;

        public RedisDbJobStoreTests()
        {
            _scheduler = CreateScheduler().Result;
            _scheduler.Clear().Wait();
        }

        public void Dispose()
        {
            _scheduler.Shutdown().Wait();
        }


        /// <summary>
        /// Start Assembly AllJob
        /// </summary>
        /// <returns></returns>
        [Fact]
        public Task StartAssemblyAllJob()
        {
            if (_scheduler != null)
            {
                var types = System.Reflection.Assembly.GetExecutingAssembly().GetTypes().Where(t => t.GetCustomAttributes(typeof(JobCronAttribute),true) != null && t.GetInterfaces().Contains(typeof(IJob))).ToList();
                types.ForEach(t => {
                    var attribute = t.GetCustomAttributes(typeof(JobCronAttribute),true)[0] as JobCronAttribute;
                    if (attribute != null)
                    {
                        string group = string.IsNullOrWhiteSpace(attribute.Group) ? t.Name + "Group" : attribute.Group;
                        IJobDetail job = JobBuilder.Create(t)
                                       .WithIdentity(t.Name, group)
                                       .StoreDurably(true)
                                       .Build();

                        var trigger = TriggerBuilder.Create()
                            .ForJob(job)
                            .WithSchedule(CronScheduleBuilder.CronSchedule(attribute.Cron))//0 0/1 * * * ?
                            .StartNow()
                            .Build();
                        _scheduler.AddJob(job, true);
                        _scheduler.ScheduleJob(trigger);
                    }
                });

                _scheduler.Start();
            }
            return Task.CompletedTask;
        }


    }
}
