using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Threading;

using CV.Database;
using CV.Cache;

namespace CV.Database.Sequence
{
    internal struct SequenceMetaData
    {
        public string UniqueIdKey;
        public uint IdCacheRange;
        public long Max;
        public long? Rollover;
    }

    public class SequenceMgr : IDisposable
    {
        static CacheMgr<string, DbCommand> _dbCmdCache = new CacheMgr<string, DbCommand>(StringComparer.CurrentCultureIgnoreCase);
        static int _count = 0;
        CacheMgr<string, SequenceMetaData> _sequences = new CacheMgr<string, SequenceMetaData>(StringComparer.CurrentCultureIgnoreCase);
        DatabaseMgr _dbMgr;
        SequenceMetaData _metaData = new SequenceMetaData();
        uint _IdCacheSize = 1;

        public SequenceMgr(DatabaseMgr dbMgr
            , uint IdCacheSize = 1)
        {
            _dbMgr = dbMgr;
            if (!_dbCmdCache.Exists(Constants.USP_UniqueIdsGetNextRange))
            {
                DbCommand dbCmd = _dbMgr.BuildStoredProcedureDbCommand(Constants.USP_UniqueIdsGetNextRange);
                _dbCmdCache.Set(Constants.USP_UniqueIdsGetNextRange, dbCmd);
            }
            if (!_dbCmdCache.Exists("SetupUniqueIdKey"))
            {
                DmlMgr dmlMgr = new DmlMgr(_dbMgr, Constants.SCHEMA_CORE, Constants.TABLE_UNIQUEIDS);
                dmlMgr.AddColumn(Constants.UniqueIdKey, MergeColumnOptions.ForInsertOnly);
                dmlMgr.AddColumn(Constants.UniqueIdValue, MergeColumnOptions.ForInsertOnly);
                dmlMgr.AddColumn(Constants.MaxIdValue);
                dmlMgr.AddColumn(Constants.RolloverIdValue);
                dmlMgr.SetWhereCondition((w) => w.Column(Constants.UniqueIdKey) == w.Parameter(Constants.UniqueIdKey));
                DbCommand dbCmd = _dbMgr.BuildMergeDbCommand(dmlMgr);
                _dbCmdCache.Set("SetupUniqueIdKey", dbCmd);
            }
        }


        public void SetupKey(string UniqueIdKey
            , long UniqueIdValue
            , long MaxIdValue
            , long RolloverIdValue)
        {
            DbCommand dbCmd = _dbMgr.CloneDbCommand(_dbCmdCache.Get("SetupUniqueIdKey"));
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdKey)].Value = UniqueIdKey;
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdValue)].Value = UniqueIdValue;
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.MaxIdValue)].Value = MaxIdValue;
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.RolloverIdValue)].Value = RolloverIdValue;
            _dbMgr.ExecuteNonQuery(dbCmd, null);
        }

        public long GetUniqueIdRange(string UniqueIdKey
            , uint RangeAmount = 1)
        {
            DbCommand dbCmd = _dbMgr.CloneDbCommand(_dbCmdCache.Get(Constants.USP_UniqueIdsGetNextRange));
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdKey)].Value = UniqueIdKey;
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.RangeAmount)].Value = RangeAmount;
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.ReturnTable)].Value = 0;
            _dbMgr.ExecuteNonQuery(dbCmd, null);
            return Convert.ToInt64(_dbMgr.GetOutParamValue(dbCmd, _dbMgr.BuildParameterName(Constants.UniqueIdValue)));
        }

        public long GetNextSequence(string UniqueIdKey)
        {
            DateTime now = _dbMgr.DbSynchTime;
            return Convert.ToInt64(now.Year.ToString()
                + now.DayOfYear.ToString("000")
                + now.Hour.ToString("00")
                + now.Minute.ToString("00")
                + now.Second.ToString("00")
                + GetUniqueIdRange(UniqueIdKey, 1).ToString("000000"));
        }

        public static long GetNextSequence()
        {
            DateTime now = DateTime.UtcNow;
            Interlocked.Increment(ref _count);
            int count = Interlocked.CompareExchange(ref _count, 0, 999);
            return Convert.ToInt64(now.Year.ToString()
                + now.DayOfYear.ToString("000")
                + now.Hour.ToString("00")
                + now.Minute.ToString("00")
                + now.Second.ToString("00")
                + now.Millisecond.ToString("000")
                + count.ToString("000"));
        }

        public void Dispose()
        {
            if (_IdCacheSize > 1)
            {
                // return unused Id numbers
            }
        }

    }
}
