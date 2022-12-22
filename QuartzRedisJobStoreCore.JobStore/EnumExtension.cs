﻿using System.ComponentModel.DataAnnotations;
using System.Reflection;

namespace QuartzRedisJobStoreCore.JobStore
{
    public static class EnumExtension
    {
        public static string GetDisplayName(this Enum enumValue)
        {
            var att = enumValue.GetType()
                            .GetMember(enumValue.ToString())
                            .First()
                            .GetCustomAttribute<DisplayAttribute>();

            if (att != null)
            {
                return att.Name;
            }
            return string.Empty;
        }
    }
}
