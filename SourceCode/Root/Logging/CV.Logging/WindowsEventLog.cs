﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

using CV.Global;

namespace CV.Logging
{
    /// <summary>
    /// ILoggingTarget implementation for logging to windows event log
    /// </summary>
    public class WindowsEventLog : ILoggingTarget
    {
        ILoggingTarget _backupLog;
        EventLog _eventLog;
        EventPriorities[] _priorities;
        static int _eventId = 0;

        /// <summary>
        /// Initializes a Windows Event Log target
        /// </summary>
        /// <param name="logName">Event Log Name</param>
        /// <param name="logSource">Event Log Source</param>
        /// <param name="backupLog">Instance of the backup log used in case instantion fails.</param>
        /// <param name="priorities">One or more priorities that this target will log</param>
        public WindowsEventLog(string logName
            , string logSource
            , ILoggingTarget backupLog
            , params EventPriorities [] priorities)
        {
            _priorities = priorities;
            try
            {
                _backupLog = backupLog;

                if (!EventLog.SourceExists(logSource))
                    EventLog.CreateEventSource(logSource, logName);
            }
            catch (System.Security.SecurityException secExc)
            {
                _backupLog.Write(string.Format("Could not write Event EventLog; ErrorMsg: {0} "
                                                    + "received because Source does not exist and could not be created.{3}"
                                                    + " Operation requires Administrator user.  To create: execute command: "
                                                    + " eventcreate /so {1} /L {2} /D SourceCreate /T Information /ID 1"
                                    , secExc.Message
                                    , logSource
                                    , logName
                                    , Environment.NewLine)
                            , true
                            , null
                            , EventLogEntryType.Error
                            , EventPriorities.Critical);

                throw new ExceptionMgr(this.ToString()
                        , new ApplicationException(string.Format("Could not verify or create EventLog source: {0} for logName {1}."
                            + " Operation requires Administrator user.  To create: execute command: "
                            + " eventcreate /so {0} /L {1} /D SourceCreate /T Information /ID 1"
                            , logSource
                            , logName), secExc));
            }

            _eventLog = new EventLog(logName, Environment.MachineName, logSource);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <param name="appendText"></param>
        /// <param name="eventReference"></param>
        /// <param name="entryType"></param>
        /// <param name="eventPriority"></param>
        public void Write(string message, bool appendText, long? eventReference
                        , EventLogEntryType entryType, EventPriorities eventPriority)
        {
            int eventId = Interlocked.Increment(ref _eventId);
            Interlocked.CompareExchange(ref _eventId, 1, Int32.MaxValue);
            Interlocked.CompareExchange(ref eventId, 1, Int32.MaxValue);
            // the eventId is not guaranteed to be unique across machine (or instances), but is is close enough for 
            // logging messages. 

            int msgPart = 0;    // event log has a max message size so we may have to divide the message up in parts

            string referencedMsg = string.Empty;
            do
            {
                referencedMsg = string.Format("{0}{1}{2}"
                    , eventReference.HasValue ? string.Format("Event Reference: {0}"
                            , eventReference.Value) : string.Empty
                    , msgPart > 0 ? string.Format("Continued Part: {0}"
                            , (msgPart + 1)) : string.Empty
                    , message.Substring(msgPart * Constants.MaxMessageSize
                        , Math.Min(Constants.MaxMessageSize,
                            message.Length - (msgPart * Constants.MaxMessageSize))));

                _eventLog.WriteEntry(referencedMsg
                        , entryType
                        , eventId);

                msgPart++;      // increase the count
            }
            while(message.Length > msgPart * Constants.MaxMessageSize);
        }

        /// <summary>
        /// Priorities of messages this target will log
        /// </summary>
        public IEnumerable<EventPriorities> Priorities
        {
            get
            {
                return _priorities;
            }
        }
    }
}
