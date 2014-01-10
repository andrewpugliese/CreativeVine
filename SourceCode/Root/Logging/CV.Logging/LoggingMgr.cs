using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;

using CV.Global;
using CV.Configuration;

namespace CV.Logging
{
    /// <summary>
    /// Thread local user defined contextual data. Usefull for tracing and logging.
    /// </summary>
    /// <remarks>
    /// By adding string data to the Context stack, users can gain additional context information in tracing.
    /// <para>
    /// Given the following 2 methods:
    /// 
    /// <code>
    /// public void AddUser()
    /// {
    ///     ...
    ///     ... 
    ///     using(new LoggingContext("Doing Security Check"))
    ///     {
    ///         SecurityCheck()
    ///     }
    /// }
    ///
    /// public void SecurityCheck()
    /// {
    ///     using(new LoggingContext("Webservice call to FBI"))
    ///     {
    ///         ...
    ///         TraceMessage("OK from FBI.")
    ///     }
    /// }
    /// </code>
    /// </para>
    /// <para>
    /// A Call to AddUser() will cause the message, "OK from FBI.", to be traced. That trace will contain the Context stack of:
    /// "Webservice call to FBI", "Doing Security Check". This extra context information can help to better understand the path of execution
    /// and current context of traced message. 
    /// </para>
    /// </remarks>
    public class LoggingContext : IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        [ThreadStatic]
        private static Stack<string> _Context = new Stack<string>();

        /// <summary>
        /// Returns the entire context stack object
        /// </summary>
        public static Stack<string> Context
        {
            get
            {
                if (_Context == null)
                    _Context = new Stack<string>();

                return _Context;
            }
        }

        /// <summary>
        /// Returns the Context Level and Message as a string
        /// </summary>
        public static string ContextString
        {
            get
            {
                if (_Context == null)
                    return string.Empty;

                StringBuilder context = new StringBuilder();
                foreach (string c in _Context)
                    context.AppendFormat("{0}{1}", context.Length > 0 ? "." : "", c);

                return context.ToString();
            }
        }

        /// <summary>
        /// Returns the nested level of the context
        /// </summary>
        public static int Level
        {
            get
            {
                if (_Context == null)
                    return 1;

                return _Context.Count + 1;
            }
        }

        /// <summary>
        /// Removes the current context level
        /// </summary>
        public void Dispose()
        {
            Context.Pop();
        }

        /// <summary>
        /// Contructs a new context object with the given context message
        /// </summary>
        /// <param name="newContext"></param>
        public LoggingContext(string newContext)
        {
            Context.Push(newContext);
        }
    }

    /// <summary>
    /// Main class for logging all event type messages, exceptions, and tracing.
    /// </summary>
    /// <remarks>
    /// The class can write messages to the Windows EventLog, MSMQ, File, and soon to come, a database.
    /// The class can be constructed several ways, one of which provides a simple to get up and running default logging
    /// setup.
    /// All construction methods require a failsafe file log, so that any message that can't be written to a target
    /// will be written to this backup file log with the name of the source and a suffix of _MMDDHH.
    /// <para>
    /// In the event that the file cannot be written to, the class
    /// will throw an exception (including the original) back to the caller
    /// for those items that have critical priority.
    /// </para>
    /// </remarks>
    public class LoggingMgr
    {
        TraceLevels _configuredTraceLevel = TraceLevels.None; // default value; obtain from cacheMgr
        FileLog _backupFileLog;
        StringBuilder _configuredOptions = new StringBuilder();
        string _loggingKey = null;

        Dictionary<EventPriorities, List<ILoggingTarget>> _loggingTargets =
            new Dictionary<EventPriorities, List<ILoggingTarget>>();

        static ReaderWriterLockSlim _defaultInstanceLock = new ReaderWriterLockSlim();

        static Lazy<LoggingMgr> _default = new Lazy<LoggingMgr>(
                () => new LoggingMgr(Constants.LoggingKey),
                LazyThreadSafetyMode.ExecutionAndPublication);

        TraceLog _traceToWindowLog = new TraceLog();

        /// <summary>
        /// Get or Set value indicating if tracing to memory mapped log file is enabled.
        /// </summary>
        public bool TraceToWindow { get; set; }

        /// <summary>
        /// Get or Set trace level.
        /// </summary>
        public TraceLevels TraceLevel
        {
            get { return _configuredTraceLevel; }
            set { _configuredTraceLevel = value; }
        }

        /// <summary>
        /// Returns a string of the configured options of this instance
        /// </summary>
        public string ConfigOptions
        {
            get { return _configuredOptions.ToString(); }
        }

        /// <summary>
        /// Returns the string key used in the configuration file
        /// </summary>
        public string LoggingKey
        {
            get { return _loggingKey; }
        }


        /// <summary>
        /// Lazily created instance of LoggingMgr based on default contructor which uses the default configuration
        /// manager to initialize the logging options.
        /// Default key is app.config is LoggingKey
        /// </summary>
        public static LoggingMgr Default
        {
            get
            {
                return _default.Value;
            }
        }

        /// <summary>
        /// Creates a LoggingMgr instance using the configuration values referred to by the given config key
        /// into the LoggingConfigurations section of the app.config file
        /// </summary>
        /// <param name="loggingConfigKey">Key into LoggingConfigurations section to obtain settings</param>
        public LoggingMgr(string loggingConfigKey)
        {
            _loggingKey = loggingConfigKey;
            _configuredOptions.AppendFormat("LogKey: {0}{1}", loggingConfigKey, Environment.NewLine);
            LoggingConfiguration loggingConfigSection
                    = ConfigurationMgr.GetSection<LoggingConfiguration>(LoggingConfiguration.ConfigSectionName);
            LoggingElement loggingConfig = loggingConfigSection.GetLoggingConfig(loggingConfigKey);
            _backupFileLog = new FileLog(loggingConfig.BackupLogFileName
                , loggingConfig.BackupLogFileDirectory
                , EventPriorities.All);
            _configuredTraceLevel = GetTraceLevelFromString(loggingConfig.TraceLevel);
            _configuredOptions.AppendFormat("BackupLogFile: {0}{1}", loggingConfig.BackupLogFileName, Environment.NewLine);
            _configuredOptions.AppendFormat("BackupLogFileDir: {0}{1}", loggingConfig.BackupLogFileDirectory, Environment.NewLine);
            InitLoggingTargets(
                BuildLoggingTargetsFromConfig(_backupFileLog, loggingConfig));
        }

        /// <summary>
        /// This constructor will create a LoggingMgr that writes to the Windows Event Log and a backup file if that
        /// fails. It will verify the event source exists and the EventLogDirectory.
        /// If the directory does not exist, it will create it.
        /// If the source cannot be verified (due to insufficient privileges), 
        /// an exception will be raised and thrown back to the caller containing 
        /// the original exception for items with critical priority.
        /// </summary>
        /// <param name="logName">Name of the log to write to; e.g. Application</param>
        /// <param name="logSource">The source that will be registered to the log</param>
        /// <param name="eventLogDirectory">Directory to files containning messages that cannot be written to primary device</param>
        /// <param name="configuredTraceLevel">Trace level.</param>
        public LoggingMgr(string logName
                        , string logSource
                        , string eventLogDirectory
                        , TraceLevels configuredTraceLevel)
        {
            _backupFileLog = new FileLog(logName, eventLogDirectory, EventPriorities.All);
            _configuredTraceLevel = configuredTraceLevel;

            InitLoggingTargets(new List<ILoggingTarget>
                {
                    new WindowsEventLog(logName, logSource, _backupFileLog, EventPriorities.All)
                });
        }

        /// <summary>
        /// This Constructor will setup the logging manager based on the the Logging Targets passed in. 
        /// </summary>
        /// <param name="loggingTargets">List of logging targets</param>
        /// <param name="backupFileLog">Failsafe backup log file in case logging to any targets throws exception</param>
        /// <param name="configuredTraceLevel">What trace level to log, e.g., Level2 logs Level2, and Level1</param>
        public LoggingMgr(List<ILoggingTarget> loggingTargets
                        , FileLog backupFileLog
                        , TraceLevels configuredTraceLevel)
        {
            InitLoggingTargets(loggingTargets);
            _configuredTraceLevel = configuredTraceLevel;
            _backupFileLog = backupFileLog;
        }

        /// <summary>
        /// Writes the given exception to the log along with a reference number.
        /// It will return the reference number to the caller.
        /// If message cannot be written to log or the failover file, then an
        /// exception will be thrown back to the caller containing the original exception.
        /// </summary>
        /// <param name="e">Exception object to log</param>
        public virtual void WriteToLog(Exception e)
        {

            WriteToLog(e, EventPriorities.Normal);
        }

        /// <summary>
        /// Writes the given exception to the log along with a reference number.
        /// If message cannot be written to log or the failover file, then an
        /// exception will be thrown back to the caller containing the original exception
        /// for items with critical priority.
        /// </summary>
        /// <param name="e">An exception object or derived exception object to log</param>
        /// <param name="priority">The priority of the item to log. A critical item will
        /// throw an exception if it is not possbile to write to the primary and backup logs</param>
        public virtual void WriteToLog(Exception e, EventPriorities Priority)
        {
            if (e is ExceptionMgr)
                WriteToLog(((ExceptionMgr)e).ToString()
                        , EventLogEntryType.Error
                        , Priority
                        , null);

            else 
                WriteToLog(ExceptionMgr.ExceptionToString(e)
                    , EventLogEntryType.Error
                    , Priority
                    , null);
        }

        /// <summary>
        /// Writes the given exception to the log along with a reference number.
        /// It will return the reference number to the caller.
        /// If message cannot be written to log or the failover file, then an
        /// exception will be thrown back to the caller containing the original exception.
        /// </summary>
        /// <param name="message">Message to be written</param>
        /// <param name="entryType">Information, Warning, Error, etc</param>
        /// <param name="enumPriority">The priority of the item to log. A critical item will
        /// throw an exception if it is not possbile to write to the primary and backup logs</param>
        public void WriteToLog(string message
            , EventLogEntryType entryType
            , EventPriorities eventPriority
            , long? eventReference = null)
        {

            foreach (EventPriorities priority in _loggingTargets.Keys.Where(p => ((int)p & (int)eventPriority) > 0))
                foreach (var target in _loggingTargets[priority])
                {
                    try
                    {
                        target.Write(message, true, eventReference, entryType, priority);
                    }
                    catch (Exception e)
                    {
                        _backupFileLog.Write(string.Format("Could not write execption to target: {0}; "
                                                + "ErrorMsg: {1} received.{2}"
                                                , target.GetType().Name
                                                , e.Message
                                                , Environment.NewLine)
                                        + message
                                    , true
                                    , eventReference
                                    , EventLogEntryType.Error
                                    , eventPriority);
                    }
                }
        }

        /// <summary>
        /// Writes trace message to log.
        /// </summary>
        /// <param name="traceToken"></param>
        public void Trace(string traceToken)
        {
            Trace(traceToken, TraceLevels.All);
        }

        /// <summary>
        /// Writes trace message to log.
        /// </summary>
        /// <param name="traceToken"></param>
        /// <param name="requiredTraceLevel"></param>
        public void Trace(string traceToken, TraceLevels requiredTraceLevel)
        {
            Trace(DateTime.MinValue, traceToken, requiredTraceLevel);
        }

        /// <summary>
        /// Writes trace message to log.
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="traceToken"></param>
        /// <param name="requiredTraceLevel"></param>
        public void Trace(DateTime startTime
                , string traceToken
                , TraceLevels requiredTraceLevel)
        {
            if (startTime != DateTime.MinValue)
            {
                TimeSpan duration = DateTime.Now - startTime;
                string message = string.Format("Total Min: {0}; Sec: {1}; MSec: {2}; Token: {3}{4}"
                    , duration.TotalMinutes
                    , duration.TotalSeconds
                    , duration.TotalMilliseconds
                    , traceToken
                    , Environment.NewLine);
                TraceMessage(message, requiredTraceLevel);
            }
            else TraceMessage(traceToken, requiredTraceLevel);

        }

        /// <summary>
        /// Writes trace message to log.
        /// </summary>
        /// <param name="stopWatchStarted"></param>
        /// <param name="traceToken"></param>
        /// <param name="requiredTraceLevel"></param>
        public void Trace(Stopwatch stopWatchStarted
        , string traceToken
        , TraceLevels requiredTraceLevel)
        {
            string message = string.Format("Total Elapsed MSec: {0}; Token: {1}{2}"
                , stopWatchStarted.ElapsedMilliseconds
                , traceToken
                , Environment.NewLine);
            TraceMessage(message, requiredTraceLevel);
        }

        private void TraceMessage(string Message
        , TraceLevels requiredTraceLevel)
        {
            if (requiredTraceLevel <= _configuredTraceLevel)
            {
                System.Diagnostics.Trace.Write(Message);
                WriteToLog(Message, EventLogEntryType.Information, EventPriorities.Trace, null);

                if (TraceToWindow)
                {
                    _traceToWindowLog.Write(Message);
                }
            }
        }

        private void InitLoggingTargets(List<ILoggingTarget> loggingTargets)
        {
            if (loggingTargets.Count == 0)
                throw new ExceptionMgr(this.ToString(),
                    new ApplicationException("There must have at least one logging target."));

            TraceToWindow = true;

            foreach (ILoggingTarget target in loggingTargets)
            {

                foreach (EventPriorities priority in target.Priorities)
                {
                    if (!_loggingTargets.ContainsKey(priority))
                        _loggingTargets.Add(priority, new List<ILoggingTarget> { target });
                    else
                        _loggingTargets[priority].Add(target);
                }
            }
        }

        private TraceLevels GetTraceLevelFromString(string traceLevel)
        {
            return Functions.EnumFromString<TraceLevels>(traceLevel, true
                , string.Format("Invalid trace level: {0}", traceLevel));
        }

        private List<ILoggingTarget> BuildLoggingTargetsFromConfig(ILoggingTarget backupLog
                , LoggingElement loggingConfig)
        {
            List<ILoggingTarget> loggingTargets = new List<ILoggingTarget>();

            foreach (LoggingTargetElement logTarget in loggingConfig.LoggingTargets.Cast<LoggingTargetElement>())
            {
                string priorities = logTarget.Priorities;

                EventPriorities[] eventPriorities = GetEvenPrioritiesFromString(priorities.ToLower());

                string targetType = logTarget.TargetType.ToLower();
                _configuredOptions.AppendFormat("Target: {0}{1}", logTarget.TargetType, Environment.NewLine);

                string logName = logTarget.LogName;

                switch (targetType)
                {
                    case "windowseventlog":
                        loggingTargets.Add(new WindowsEventLog(logName,
                            logTarget.Params.GetParamValue(Constants.EventLogSource).ToString()
                                , backupLog, eventPriorities));
                        _configuredOptions.AppendFormat("Param: {0}; Value: {1}{2}"
                                    , Constants.EventLogSource
                                    , logTarget.Params.GetParamValue(Constants.EventLogSource)
                                    , Environment.NewLine);
                        break;
                    case "msmq":
                        loggingTargets.Add(new MSMQLog(logName,
                           logTarget.Params.GetParamValue(Constants.MSMQPath).ToString()
                                , backupLog, eventPriorities));
                        _configuredOptions.AppendFormat("Param: {0}; Value: {1}{2}"
                                    , Constants.MSMQPath, logTarget.Params.GetParamValue(Constants.MSMQPath)
                                    , Environment.NewLine);
                        break;
                    case "file":
                        loggingTargets.Add(new FileLog(logName,
                           logTarget.Params.GetParamValue(Constants.LogFileDirectory).ToString(), eventPriorities));
                        _configuredOptions.AppendFormat("Param: {0}; Value: {1}{2}"
                                    , Constants.LogFileDirectory
                                    , logTarget.Params.GetParamValue(Constants.LogFileDirectory)
                                    , Environment.NewLine);
                        break;
                    case "memoryfile":
                        loggingTargets.Add(new MemoryFileLog(logName,
                           logTarget.Params.GetParamValue(Constants.LogFileNamePrefix).ToString(),
                           logTarget.Params.GetParamValue(Constants.LogFileDirectory).ToString(),
                           Int32.Parse(logTarget.Params.GetParamValue(Constants.LogFileSize).ToString())
                            , eventPriorities));
                        _configuredOptions.AppendFormat("Param: {0}; Value: {1}{2}"
                                    , Constants.LogFileNamePrefix
                                    , logTarget.Params.GetParamValue(Constants.LogFileNamePrefix)
                                    , Environment.NewLine);
                        _configuredOptions.AppendFormat("Param: {0}; Value: {1}{2}"
                                    , Constants.LogFileDirectory, logTarget.Params.GetParamValue(Constants.LogFileDirectory)
                                    , Environment.NewLine);
                        _configuredOptions.AppendFormat("Param: {0}; Value: {1}{2}"
                                    , Constants.LogFileSize, logTarget.Params.GetParamValue(Constants.LogFileSize)
                                    , Environment.NewLine);
                        break;
                    default:
                        throw new ExceptionMgr(this.ToString()
                            , new ArgumentOutOfRangeException(
                            "Unkown logging target string: " + targetType));
                }
            }

            return loggingTargets;
        }

        private EventPriorities[] GetEvenPrioritiesFromString(string priorities)
        {
            List<EventPriorities> eventPriorities = new List<EventPriorities>();

            foreach (string priority in priorities.Split(','))
            {
                switch (priority.Trim().ToLower())
                {
                    case "all":
                        eventPriorities.Add(EventPriorities.All);
                        break;
                    case "critical":
                        eventPriorities.Add(EventPriorities.Critical);
                        break;
                    case "normal":
                        eventPriorities.Add(EventPriorities.Normal);
                        break;
                    case "trace":
                        eventPriorities.Add(EventPriorities.Trace);
                        break;
                    case "warning":
                        eventPriorities.Add(EventPriorities.Warning);
                        break;
                    default:
                        throw new ExceptionMgr(this.ToString()
                            , new ArgumentOutOfRangeException("Unkown priority string: " + priorities.Trim().ToLower()));
                }
            }
            return eventPriorities.ToArray();
        }
    }
}
