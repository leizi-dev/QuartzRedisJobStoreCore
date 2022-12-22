using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuartzRedisJobStoreCore.JobStore
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
    public class JobCronAttribute : Attribute
    {
        public string Cron { get; set; }

        public string Name { get; set; }

        public string Group { get; set; }

        public JobCronAttribute(string cron, string name = "", string group = "")
        {
            Cron = cron;
            Name = name;
            Group = group;
        }

    }
}
