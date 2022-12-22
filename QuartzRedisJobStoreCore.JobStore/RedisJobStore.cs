using Common.Logging;
using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using StackExchange.Redis;

namespace QuartzRedisJobStoreCore.JobStore
{
    /// <summary>
    /// Redis Job Store 
    /// </summary>
    public class RedisJobStore : IJobStore
    {

        #region private fields
        /// <summary>
        /// logger
        /// </summary>
        private readonly ILog _logger = LogManager.GetLogger(typeof(RedisJobStore));
        /// <summary>
        /// redis job store schema
        /// </summary>
        private RedisJobStoreSchema _storeSchema;
        /// <summary>
        /// redis db.
        /// </summary>
        private IDatabase _db;
        /// <summary>
        /// master/slave redis store.
        /// </summary>
        private RedisStorage _storage;

        #endregion

        #region public properties

        /// <summary>
        /// Indicates whether job store supports persistence.
        /// </summary>
        /// <returns/>
        public bool SupportsPersistence
        {
            get { return true; }
        }
        /// <summary>
        /// How long (in milliseconds) the <see cref="T:Quartz.Spi.IJobStore"/> implementation 
        ///             estimates that it will take to release a trigger and acquire a new one. 
        /// </summary>
        public long EstimatedTimeToReleaseAndAcquireTrigger
        {
            get { return 200; }
        }
        /// <summary>
        /// Whether or not the <see cref="T:Quartz.Spi.IJobStore"/> implementation is clustered.
        /// </summary>
        /// <returns/>
        public bool Clustered { get; set; }
        //{
        //    get { return true; }
        //}
        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> of the Scheduler instance's Id, 
        ///             prior to initialize being invoked.
        /// </summary>
        public string InstanceId { get; set; }
        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> of the Scheduler instance's name, 
        ///             prior to initialize being invoked.
        /// </summary>
        public string InstanceName { get; set; }
        /// <summary>
        /// Tells the JobStore the pool size used to execute jobs.
        /// </summary>
        public int ThreadPoolSize { get; set; }
        /// <summary>
        /// Redis configuration
        /// </summary>
        public string RedisConfiguration { set; get; }

        /// <summary>
        /// gets / sets the delimiter for concatinate redis keys.
        /// </summary>
        public string KeyDelimiter { get; set; }

        /// <summary>
        /// gets /sets the prefix for redis keys.
        /// </summary>
        public string KeyPrefix { get; set; }

        /// <summary>
        /// trigger lock time out, used to release the orphan triggers in case when a scheduler crashes and still has locks on some triggers. 
        /// make sure the lock time out is bigger than the time for running the longest job.
        /// </summary>
        public int? TriggerLockTimeout { get; set; }

        /// <summary>
        /// redis lock time out in milliseconds.
        /// </summary>
        public int? RedisLockTimeout { get; set; }

        /// <summary>
        /// Redis Sentinel model  
        /// </summary>
        public bool IsSentinel { get; set; }

        /// <summary>
        /// Redis Sentinel Service Name
        /// </summary>
        public string SentinelServiceName { get; set; }

        /// <summary>
        /// Redis Password
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Redis DB
        /// </summary>
        public int DbNum { get; set; } = 0;

        /// <summary>
        /// Redis AllowAdmin
        /// </summary>
        public bool AllowAdmin { get; set; }
        #endregion

        #region Implementation of IJobStore

        public Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler, CancellationToken cancellationToken = default)
        {
            _storeSchema = new RedisJobStoreSchema(KeyPrefix ?? string.Empty, KeyDelimiter ?? ":");
            if (IsSentinel)
            {
                ConfigurationOptions sentinelOptions = new ConfigurationOptions();
                sentinelOptions.ClientName = Guid.NewGuid().ToString();
                var endPoints = RedisConfiguration.Split(',');
                for (int i = 0; i < endPoints.Length; i++)
                {
                    sentinelOptions.EndPoints.Add(endPoints[i]);
                }
                sentinelOptions.CommandMap = CommandMap.Sentinel;
                sentinelOptions.AllowAdmin = AllowAdmin;
                sentinelOptions.AbortOnConnectFail = false;
                var sentinelConnect = ConnectionMultiplexer.Connect(sentinelOptions);

                ConfigurationOptions redisServiceOptions = new ConfigurationOptions();
                redisServiceOptions.ServiceName = SentinelServiceName;
                redisServiceOptions.Password = Password;
                redisServiceOptions.AbortOnConnectFail = true;
                redisServiceOptions.ClientName = Guid.NewGuid().ToString();
                redisServiceOptions.AllowAdmin = AllowAdmin;
                _db = sentinelConnect.GetSentinelMasterConnection(redisServiceOptions).GetDatabase(DbNum);
            }
            else
            {
                _db = ConnectionMultiplexer.Connect(RedisConfiguration).GetDatabase(DbNum);
            }

            _storage = new RedisStorage(_storeSchema, _db, signaler, InstanceId, TriggerLockTimeout ?? 300000, RedisLockTimeout ?? 6000);
            return TaskUtil.CompletedTask;
        }

        public Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has started");
            return TaskUtil.CompletedTask;
        }

        public Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has paused");
            return TaskUtil.CompletedTask;
        }

        public Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has resumed");
            return TaskUtil.CompletedTask;
        }

        public Task Shutdown(CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has shutdown");
            _db.Multiplexer.Dispose();
            return TaskUtil.CompletedTask;
        }

        public Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreJobAndTrigger");
            DoWithLock(() =>
            {
                _storage.StoreJob(newJob, false);
                _storage.StoreTrigger(newTrigger, false);
            }, "Could store job/trigger");
            return TaskUtil.CompletedTask;
        }

        public Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has paused");
            return Task.FromResult(true);
        }

        public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("IsJobGroupPaused");
            return Task.FromResult(DoWithLock(() => _storage.IsJobGroupPaused(groupName),
                              string.Format("Error on IsJobGroupPaused - Group {0}", groupName)));
        }

        public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreJob");
            DoWithLock(() => _storage.StoreJob(newJob, replaceExisting), "Could not store job");
            return TaskUtil.CompletedTask;
        }

        public Task StoreJobsAndTriggers(IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreJobsAndTriggers");
            foreach (var job in triggersAndJobs)
            {
                DoWithLock(() =>
                {
                    _storage.StoreJob(job.Key, replace);
                    foreach (var trigger in job.Value)
                    {
                        _storage.StoreTrigger(trigger, replace);
                    }

                }, "Could store job/trigger");

            }
            return TaskUtil.CompletedTask;
        }

        public Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveJob");
            return Task.FromResult(DoWithLock(() => _storage.RemoveJob(jobKey),
                              "Could not remove a job"));
        }

        public Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveJobs");
            bool removed = jobKeys.Count > 0;

            foreach (var jobKey in jobKeys)
            {
                DoWithLock(() =>
                {
                    removed = _storage.RemoveJob(jobKey);
                }, "Error on removing job");

            }
            return Task.FromResult(removed);
        } 

        public Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RetrieveJob");
            return Task.FromResult(DoWithLock(() => _storage.RetrieveJob(jobKey),
                              "Could not retriev job"));
        }

        public Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreTrigger");
            DoWithLock(() => _storage.StoreTrigger(newTrigger, replaceExisting),
                             "Could not store trigger");
            return TaskUtil.CompletedTask;
        }

        public Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveTrigger");
            return Task.FromResult(DoWithLock(() => _storage.RemoveTrigger(triggerKey),
                              "Could not remove trigger"));
        }

        public Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveTriggers");

            bool removed = triggerKeys.Count > 0;

            foreach (var triggerKey in triggerKeys)
            {
                DoWithLock(() =>
                {
                    removed = _storage.RemoveTrigger(triggerKey);
                }, "Error on removing trigger");

            }
            return Task.FromResult(removed);
        }

        public Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ReplaceTrigger");

            return Task.FromResult(DoWithLock(() => _storage.ReplaceTrigger(triggerKey, newTrigger),
                              "Error on replacing trigger"));
        }

        public Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RetrieveTrigger");

            return Task.FromResult(DoWithLock(() => _storage.RetrieveTrigger(triggerKey),
                              "could not retrieve trigger"));
        }

        public Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("CalendarExists");

            return Task.FromResult(DoWithLock(() => _storage.CheckExists(calName),
                             string.Format("could not check if the calendar {0} exists", calName)));
        }

        public Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("CheckExists - Job");
            return Task.FromResult(DoWithLock(() => _storage.CheckExists(jobKey),
                              string.Format("could not check if the job {0} exists", jobKey)));
        }

        public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("CheckExists - Trigger");
            return Task.FromResult(DoWithLock(() => _storage.CheckExists(triggerKey),
                            string.Format("could not check if the trigger {0} exists", triggerKey)));
        }

        public Task ClearAllSchedulingData(CancellationToken cancellationToken = default)
        {
            _logger.Debug("ClearAllSchedulingData");
            DoWithLock(() => _storage.ClearAllSchedulingData(), "Could not clear all the scheduling data");
            return TaskUtil.CompletedTask;
        }

        public Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreCalendar");
            DoWithLock(() => _storage.StoreCalendar(name, calendar, replaceExisting, updateTriggers),
                       string.Format("Error on store calendar - {0}", name));
            return TaskUtil.CompletedTask;
        }

        public Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveCalendar");
            return Task.FromResult(DoWithLock(() => _storage.RemoveCalendar(calName),
                       string.Format("Error on remvoing calendar - {0}", calName)));
        }

        public Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RetrieveCalendar");
            return Task.FromResult(DoWithLock(() => _storage.RetrieveCalendar(calName),
                              string.Format("Error on retrieving calendar - {0}", calName)));
        }

        public Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetNumberOfJobs");
            return Task.FromResult(DoWithLock(() => _storage.NumberOfJobs(), "Error on getting Number of jobs"));
        }

        public Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetNumberOfTriggers");
            return Task.FromResult(DoWithLock(() => _storage.NumberOfTriggers(), "Error on getting number of triggers"));
        }

        public Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetNumberOfCalendars");
            return Task.FromResult(DoWithLock(() => _storage.NumberOfCalendars(), "Error on getting number of calendars"));
        }

        public Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetJobKeys");
            return Task.FromResult(DoWithLock(() => _storage.JobKeys(matcher), "Error on getting job keys"));
        }

        public Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetTriggerKeys");
            return Task.FromResult(DoWithLock(() => _storage.TriggerKeys(matcher), "Error on getting trigger keys"));
        }

        public Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetJobGroupNames");
            return Task.FromResult(DoWithLock(() => _storage.JobGroupNames(), "Error on getting job group names"));
        }

        public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetTriggerGroupNames");
            return Task.FromResult(DoWithLock(() => _storage.TriggerGroupNames(), "Error on getting trigger group names"));
        }

        public Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetCalendarNames");
            return Task.FromResult(DoWithLock(() => _storage.CalendarNames(), "Error on getting calendar names"));
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetTriggersForJob");
            return Task.FromResult(DoWithLock(() => _storage.GetTriggersForJob(jobKey), string.Format("Error on getting triggers for job - {0}", jobKey)));
        }

        public Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetTriggerState");
            return Task.FromResult(DoWithLock(() => _storage.GetTriggerState(triggerKey),
                              string.Format("Error on getting trigger state for trigger - {0}", triggerKey)));
        }

        public Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseTrigger");
            DoWithLock(() => _storage.PauseTrigger(triggerKey),
                              string.Format("Error on pausing trigger - {0}", triggerKey));
            return TaskUtil.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseTriggers");
            return Task.FromResult(DoWithLock(() => _storage.PauseTriggers(matcher), "Error on pausing triggers"));
        }

        public Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseJob");
            DoWithLock(() => _storage.PauseJob(jobKey), string.Format("Error on pausing job - {0}", jobKey));
            return TaskUtil.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseJobs");
            return Task.FromResult(DoWithLock(() => _storage.PauseJobs(matcher), "Error on pausing jobs"));
        }

        public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeTrigger");
            DoWithLock(() => _storage.ResumeTrigger(triggerKey),
                       string.Format("Error on resuming trigger - {0}", triggerKey));
            return TaskUtil.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeTriggers");
            return Task.FromResult(DoWithLock(() => _storage.ResumeTriggers(matcher), "Error on resume triggers"));
        }

        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetPausedTriggerGroups");
            return Task.FromResult(DoWithLock(() => _storage.GetPausedTriggerGroups(), "Error on getting paused trigger groups"));
        }

        public Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeJob");
            DoWithLock(() => _storage.ResumeJob(jobKey), string.Format("Error on resuming job - {0}", jobKey));
            return TaskUtil.CompletedTask;
        }

        public Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeJobs");
            return Task.FromResult(DoWithLock(() => _storage.ResumeJobs(matcher), "Error on resuming jobs"));
        }

        public Task PauseAll(CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseAll");
            DoWithLock(() => _storage.PauseAllTriggers(), "Error on pausing all");
            return TaskUtil.CompletedTask;
        }

        public Task ResumeAll(CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeAll");
            DoWithLock(() => _storage.ResumeAllTriggers(), "Error on resuming all");
            return TaskUtil.CompletedTask;
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow, CancellationToken cancellationToken = default)
        {
            _logger.Debug("AcquireNextTriggers");
            return Task.FromResult(DoWithLock(() => _storage.AcquireNextTriggers(noLaterThan, maxCount, timeWindow),
                              "Error on acquiring next triggers"));
        }

        public Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ReleaseAcquiredTrigger");
            DoWithLock(() => _storage.ReleaseAcquiredTrigger(trigger), string.Format("Error on releasing acquired trigger - {0}", trigger));
            return TaskUtil.CompletedTask;
        }

        /// <summary>
        /// lock this next_fire_time
        /// </summary>
        /// <param name="triggers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken cancellationToken = default)
        {
            _logger.Debug("TriggersFired");
            return Task.FromResult(DoWithLock(() => _storage.TriggersFired(triggers), "Error on Triggers Fired"));
        }

        public Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode, CancellationToken cancellationToken = default)
        {
            _logger.Debug("TriggeredJobComplete");
            DoWithLock(() => _storage.TriggeredJobComplete(trigger, jobDetail, triggerInstCode),
                       string.Format("Error on triggered job complete - job:{0} - trigger:{1}", jobDetail, trigger));
            return TaskUtil.CompletedTask;
        }
        #endregion


        #region private methods

        /// <summary>
        /// crud opertion to redis with lock 
        /// </summary>
        /// <typeparam name="T">return type of the Function</typeparam>
        /// <param name="fun">Fuction</param>
        /// <param name="errorMessage">error message used to override the default one</param>
        /// <returns></returns>
        private T DoWithLock<T>(Func<T> fun, string errorMessage = "Job Storage error")
        {
            try
            {
                _storage.LockWithWait();
                return fun.Invoke();
            }
            catch (ObjectAlreadyExistsException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                _storage.Unlock();
            }
        }

        /// <summary>
        /// crud opertion to redis with lock 
        /// </summary>
        /// <param name="action">Action</param>
        /// <param name="errorMessage">error message used to override the default one</param>
        private void DoWithLock(Action action, string errorMessage = "Job Storage error")
        {
            try
            {
                _storage.LockWithWait();
                action.Invoke();
            }
            catch (ObjectAlreadyExistsException ex)
            {
                _logger.Error("key exists", ex);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                _storage.Unlock();
            }
        }

        #endregion
    }
}
