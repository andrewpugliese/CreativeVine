using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace CV.Logging
{
    /// <summary>
    /// Interface used to implement logging targets for LoggingMgr class, e.g., MSMQ, Windows Event Log, DB, etc.
    /// </summary>
    public interface ILoggingTarget
    {
        /// <summary>
        /// Method used for writing message to log target.
        /// </summary>
        /// <param name="message">Message to log</param>
        /// <param name="appendText">Append text to end of log. May or may not be relevant depending on target.</param>
        /// <param name="eventReference">"Optional Reference identifier for this message. Can be usefull when trying to identify 
        /// something that happend.</param>
        /// <param name="entryType">Category of message.</param>
        /// <param name="priority">Priority of message./</param>
        void Write(string message
            , bool appendText
            , long? eventReference
            , EventLogEntryType entryType
            , EventPriorities priority);

        /// <summary>
        /// Readonly property that returns a collection of message priorities this target wants.
        /// </summary>
        IEnumerable<EventPriorities> Priorities { get; }
    }
}
