using Quartz;
using Quartz.Impl;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuartzRedisJobStoreCore.UnitTest
{
    public abstract class BaseStoreTests
    {
        protected async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
        {
            var properties = new NameValueCollection
            {
                ["quartz.jobStore.type"] = "QuartzRedisJobStoreCore.JobStore.RedisJobStore, QuartzRedisJobStoreCore.JobStore",
                ["quartz.jobStore.keyPrefix"] = "UnitJob",
                ["quartz.serializer.type"] = "json",
                ["quartz.scheduler.instanceId"] = "AUTO",
                ["quartz.jobStore.dbNum"] = "8",
                ["quartz.jobStore.redisConfiguration"] = "127.0.0.1:6379,allowAdmin=true,syncTimeout=5000,password=Hsyq@123",
                //Start Redis Sentinel Model
                //["quartz.jobStore.isSentinel"] = "true",
                //["quartz.jobStore.sentinelServiceName"] = "cfmaster",
                //["quartz.jobStore.password"] = "123456",
                //["quartz.jobStore.allowAdmin"] = "true",
                //End Redis Sentinel Model
                ["quartz.jobStore.clustered"] = "true",
                ["quartz.threadPool.threadCount"] = "1",
                [StdSchedulerFactory.PropertySchedulerInstanceName] = "QUARTZ_JOB",
            };

            var scheduler = new StdSchedulerFactory(properties);
            return await scheduler.GetScheduler();
        }
    }
}
