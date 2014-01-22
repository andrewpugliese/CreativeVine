using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;

using CV.Configuration;
using CV.Database;
using CV.Database.Sequence;
using CV.Logging;

namespace CV.Rest.Sequences
{
    public class SequenceDispatcher : ISequence, IDisposable
    {
        static Lazy<DatabaseMgr> _dbMgr = new Lazy<DatabaseMgr>(() => new DatabaseMgr(ConfigurationMgr.GetNonEmptyValue("ConnectionKey")));
        static Lazy<LoggingMgr> _logMgr = new Lazy<LoggingMgr>(() => new LoggingMgr(ConfigurationMgr.GetNonEmptyValue("LoggingKey")));
        static Lazy<SequenceMgr> _seqMgr = new Lazy<SequenceMgr>(() => new SequenceMgr(_dbMgr.Value, 100));

        public SequenceDispatcher()
            //: this(new DatabaseMgr(ConfigurationMgr.GetNonEmptyValue("ConnectionKey")), new LoggingMgr("BasicLogging"))
            : this(_dbMgr, _logMgr)
        {
        }

        ~SequenceDispatcher()
        {
        }

        public void Dispose()
        {
            _logMgr.Value.WriteToLog("I was Disposed");
        }

        public SequenceDispatcher(DatabaseMgr DbMgr, LoggingMgr LogMgr)
        {
            LogMgr.WriteToLog("I was constructed");
        }

        public SequenceDispatcher(Lazy<DatabaseMgr> DbMgr, Lazy<LoggingMgr> LogMgr)
        {
            _logMgr.Value.WriteToLog("I was constructed at: " + _dbMgr.Value.GetServerTime(DateTimeKind.Local).ToLongTimeString()
                + " Seq: " + _seqMgr.Value.GetNextSequence("MyTest"));
        }

        public List<SequenceItem> GetKeys()
        {
            return new List<SequenceItem>();
        }

        public SequenceItem GetKey(string SequenceKey)
        {
            return new List<SequenceItem>().FirstOrDefault<SequenceItem>(k => k.Key == SequenceKey);
        }

        public bool SetKey(string SequenceKey)
        {
            return true;
        }

        public long GenerateUniqueIdRange(string SequenceKey, int RangeSize = 1)
        {
            return 0;
        }

        public long GenerateSequence(string SequenceKey)
        {
            return 0;
        }

        public bool DeleteKey(string SequenceKey)
        {
            return false;
        }

        public SequenceItem CreateKey()
        {
            return new SequenceItem();
        }
    }
}
