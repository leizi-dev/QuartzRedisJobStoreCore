Redis Job Store for Quartz.NET
================================
Thanks to @icyice80 for providing QuartzRedisJobStore project.

## Basic Usage

```cs
var properties = new NameValueCollection
{
	["quartz.jobStore.type"] = "QuartzRedisJobStoreCore.JobStore.RedisJobStore, QuartzRedisJobStoreCore.JobStore",
	["quartz.jobStore.keyPrefix"] = "UnitJob",
	["quartz.serializer.type"] = "json",
	["quartz.scheduler.instanceId"] = "AUTO",
	["quartz.jobStore.dbNum"] = "8",
	["quartz.jobStore.redisConfiguration"] = "127.0.0.1:6379,allowAdmin=true,syncTimeout=5000,password=******",
	//Start Redis Sentinel Model
	//["quartz.jobStore.isSentinel"] = "true",
	//["quartz.jobStore.sentinelServiceName"] = "",
	//["quartz.jobStore.password"] = "******",
	//["quartz.jobStore.allowAdmin"] = "true",
	//End Redis Sentinel Model
	["quartz.jobStore.clustered"] = "true",
	["quartz.threadPool.threadCount"] = "1",
	[StdSchedulerFactory.PropertySchedulerInstanceName] = "QUARTZ_JOB",
};
```

## Nuget

```
Install-Package QuartzRedisJobStoreCore
```