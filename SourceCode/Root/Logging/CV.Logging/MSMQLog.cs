using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Messaging;
using System.Text;

using CV.Global;

namespace CV.Logging
{
    /// <summary>
    /// ILoggingTarget implementation for logging to MSMQ
    /// </summary>
    public class MSMQLog : ILoggingTarget
    {
        MessageQueue _msgQueue;
        ILoggingTarget _backupLog;
        EventPriorities[] _priorities;
        
        /// <summary>
        /// Initializes an MSMQ Logging Target
        /// </summary>
        /// <param name="logName">Enterprise library log name</param>
        /// <param name="queuePath">MSMQ path e.g., ".\\Private$\\NOC" (backslashes need to be escaped) </param>
        /// <param name="backupLog">Instance of the backup log used in case instantion fails.</param>
        /// <param name="priorities">One or more priorities that this target will log</param>
        public MSMQLog(string logName, string queuePath, ILoggingTarget backupLog, 
            params EventPriorities [] priorities)
        {
            _priorities = priorities;
            _backupLog = backupLog;

            try
            {
                _msgQueue = new MessageQueue(queuePath, false);
            }
            catch(Exception ex)
            {
                string errMsg = string.Format("Could not create MSMQ Logger with queuePath: {0} errorMsg: {1} \r\n" 
                                            , ex
                                            , queuePath);

                _backupLog.Write(errMsg
                            , true
                            , null
                            , EventLogEntryType.Error
                            , EventPriorities.Critical);

                throw new ExceptionMgr(this.ToString(), 
                        new ApplicationException(errMsg, ex));
            }
        }

        #region ILoggingTarget Members
        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <param name="appendText"></param>
        /// <param name="eventReference"></param>
        /// <param name="entryType"></param>
        /// <param name="Priority"></param>
        public void Write(string message, bool appendText, long? eventReference, 
            System.Diagnostics.EventLogEntryType entryType, EventPriorities Priority )
        {
            string reference = eventReference.HasValue ? 
                 "Event Reference: " + eventReference.Value
                : string.Empty;
             
            Message m = new Message(reference + message);
            m.Label = string.Format("{0}; {1}; {2}", entryType
                , message.Substring(0, Math.Min(25, message.Length-1)), reference);
            _msgQueue.Send( m );
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

        #endregion
    }
}
