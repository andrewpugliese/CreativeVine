using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using CV.Global;

namespace CV.Logging
{
    public enum TraceLevels { None = 0, Level1 = 1, Level2 = 2, Level3 = 3, Level4 = 4, Level5 = 5, All = 100 };
    public enum EventPriorities { Normal = 1, Warning = 2, Critical = 4, Trace = 8, All = 0xf };
    
    public static class Constants
    {
        /// <summary>
        /// Returns string version of LoggingKey
        /// </summary>
        public const string LoggingKey = "LoggingKey";

        /// <summary>
        /// Returns the string version of TraceLevel
        /// </summary>
        public const string TraceLevel = "TraceLevel";

        internal const string LoggingConfigurations = "LoggingConfigurations";
        internal const string LoggingConfig = "LoggingConfig";
        internal const string LoggingTargets = "LoggingTargets";
        internal const string LoggingTarget = "LoggingTarget";
        internal const string LogName = "LogName";
        internal const string Priorities = "Priorities";
        internal const string TargetType = "TargetType";
        internal const string BackupLogFileName = "BackupLogFileName";
        internal const string BackupLogFileDirectory = "BackupLogFileDirectory";
        internal const string Param = "Param";
        internal const string Params = "Params";
        internal const string ParamKey = "ParamKey";
        internal const string ParamValue = "ParamValue";

        /// <summary>
        /// Returns the enumeration of trace levels
        /// </summary>
        /// <returns>Enumeration of TraceLevels</returns>
        public static IEnumerable<TraceLevels> TraceLevels()
        {
            return Functions.GetEnumValues<TraceLevels>();
        }

        /// <summary>
        /// Converts the given string version of trace level to the equivalent enumeration
        /// </summary>
        /// <param name="traceLevel">String version of trace level</param>
        /// <returns>Enumeration type of trace level</returns>
        public static TraceLevels ToTraceLevel(string traceLevel)
        {
            return Functions.EnumFromString<TraceLevels>(traceLevel, true
                , string.Format("Unknown traceLevel: {0}", traceLevel));
        }

        internal const string EventLogSource = "EventLogSource";
        internal const string MSMQPath = "MSMQPath";
        internal const string LogFileDirectory = "LogFileDirectory";
        internal const string LogFileNamePrefix = "LogFileNamePrefix";
        internal const string LogFileSize = "LogFileSize";

        //Actual windows event log has max message size of 32766 bytes(not string length). We take away 1000 bytes for 
        //extra header information we might add such as event reference and continuation msg.
        internal const int MaxMessageSize = 31766;
    }
}
