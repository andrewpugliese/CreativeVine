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
    internal struct IdMetaData
    {
        public string IdKey;
        public long IdValue;
        public long OriginalIdValue;
        public long CacheSize;
        public long? Max;
        public long? Rollover;
    }

    public class SequenceMgr : IDisposable
    {
        static CacheMgr<string, DbCommand> _dbCmdCache = new CacheMgr<string, DbCommand>(StringComparer.CurrentCultureIgnoreCase);
        static int _count = 0;
        CacheMgr<string, IdMetaData> _idCache = new CacheMgr<string, IdMetaData>(StringComparer.CurrentCultureIgnoreCase);
        CacheMgr<string, object> _idCacheLocks = new CacheMgr<string, object>(StringComparer.CurrentCultureIgnoreCase);
        DatabaseMgr _dbMgr;
        uint _idCacheSize = 1;

        public SequenceMgr(DatabaseMgr dbMgr
            , uint IdCacheSize = 1)
        {
            _dbMgr = dbMgr;
            _idCacheSize = IdCacheSize;
            if (!_dbCmdCache.Exists(Constants.USP_UniqueIdsGetNextRange))
            {
                DbCommand dbCmd = _dbMgr.BuildStoredProcedureDbCommand(Constants.USP_UniqueIdsGetNextRange);
                _dbCmdCache.Set(Constants.USP_UniqueIdsGetNextRange, dbCmd);
            }
            if (!_dbCmdCache.Exists(Constants.SetupUniqueIdKey))
            {
                DmlMgr dmlMgr = new DmlMgr(_dbMgr, Constants.SCHEMA_CORE, Constants.TABLE_UNIQUEIDS);
                dmlMgr.AddColumn(Constants.UniqueIdKey, MergeColumnOptions.ForInsertOnly);
                dmlMgr.AddColumn(Constants.UniqueIdValue, MergeColumnOptions.ForInsertOnly);
                dmlMgr.AddColumn(Constants.MaxIdValue);
                dmlMgr.AddColumn(Constants.RolloverIdValue);
                dmlMgr.SetWhereCondition((w) => w.Column(Constants.UniqueIdKey) == w.Parameter(Constants.UniqueIdKey));
                DbCommandMgr dbCmdMgr = new DbCommandMgr(_dbMgr);
                dbCmdMgr.AddDbCommand(_dbMgr.BuildMergeDbCommand(dmlMgr));
                dmlMgr = new DmlMgr(_dbMgr, Constants.SCHEMA_CORE, Constants.TABLE_UNIQUEIDS);
                dmlMgr.SetWhereCondition((w) => w.Column(Constants.UniqueIdKey) == w.Parameter(Constants.UniqueIdKey));
                dbCmdMgr.AddDbCommand(_dbMgr.BuildSelectDbCommand(dmlMgr, null));
                _dbCmdCache.Set(Constants.SetupUniqueIdKey, dbCmdMgr.DbCommandBlock);
            }
            if (!_dbCmdCache.Exists(Constants.ResetUniqueIdValue))
            {
                DmlMgr dmlMgr = new DmlMgr(_dbMgr
                    , Constants.SCHEMA_CORE
                    , Constants.TABLE_UNIQUEIDS);
                dmlMgr.AddColumn(Constants.UniqueIdValue, MergeColumnOptions.ForUpdateOnly);
                dmlMgr.SetWhereCondition((w) => w.Column(Constants.UniqueIdKey) == w.Parameter(Constants.UniqueIdKey)
                    && w.Column(Constants.UniqueIdValue) == w.Parameter(Constants.UniqueIdValue));
                _dbCmdCache.Set(Constants.ResetUniqueIdValue, _dbMgr.BuildChangeDbCommand(dmlMgr, new string[] { Constants.UniqueIdValue }));
            }
        }

        public bool SetupKey(string UniqueIdKey
            , long? MaxIdValue = null
            , long? RolloverIdValue = null)
        {
            // if key was already setup
            if (_idCacheLocks.Exists(UniqueIdKey))
                return false;

                // otherwise lock entire cache to add key
            lock (_idCache)
            {
                if (!_idCacheLocks.Exists(UniqueIdKey))
                {
                    _idCacheLocks.Set(UniqueIdKey, new object());
                }
            }
            lock (_idCacheLocks.Get(UniqueIdKey))
            {
                // only if key is still not setup.
                if (!_idCache.Exists(UniqueIdKey))
                    _idCache.Set(UniqueIdKey, ConvertToMetaData(SetupNewKey(UniqueIdKey, MaxIdValue, RolloverIdValue)));
                else return false;
            }
            return true;
        }

        public long GetUniqueIdRange(string UniqueIdKey
             , uint RangeAmount = 1)
        {
            if (_idCacheSize <= 1)
                return GetUniqueIdRangeFromDb(UniqueIdKey, RangeAmount);
            return GetIdFromCache(UniqueIdKey, RangeAmount);
        }

        public long GetNextSequence(string UniqueIdKey)
        {
            DateTime now = _dbMgr.DbSynchTime;
            string sequence = now.Year.ToString()
                + now.DayOfYear.ToString("000")
                + now.Hour.ToString("00")
                + now.Minute.ToString("00")
                + now.Second.ToString("00")
                + GetUniqueIdRange(UniqueIdKey, 1).ToString("000000");
            return Convert.ToInt64(sequence);
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
            if (_idCacheSize > 1)
            {
                // return unused Id numbers
                ClearUniqueIdsCache();
            }
        }

        ~SequenceMgr()
        {
            Dispose();
        }

        DataTable SetupNewKey(string UniqueIdKey
            , long? MaxIdValue = null
            , long? RolloverIdValue = null)
        {
            DbCommand dbCmd = _dbMgr.CloneDbCommand(_dbCmdCache.Get(Constants.SetupUniqueIdKey));
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdKey)].Value = UniqueIdKey;
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdValue)].Value = 0; // ALWAYS DEFAULTS TO 0
            if (MaxIdValue.HasValue)
                dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.MaxIdValue)].Value = MaxIdValue.Value;
            if (RolloverIdValue.HasValue)
                dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.RolloverIdValue)].Value = RolloverIdValue.Value;
            return _dbMgr.ExecuteDataSet(dbCmd, null).Tables[0];
        }

        IdMetaData ConvertToMetaData(DataTable UniqueIds)
        {
            IdMetaData metaData = new IdMetaData();
            metaData.IdKey = UniqueIds.Rows[0][Constants.UniqueIdKey].ToString();
            metaData.IdValue = metaData.OriginalIdValue = Convert.ToInt64(UniqueIds.Rows[0][Constants.UniqueIdValue]);
            metaData.Max = _dbMgr.GetValueOrDefault<long?>(UniqueIds.Rows[0][Constants.MaxIdValue], null);
            metaData.Rollover = _dbMgr.GetValueOrDefault<long?>(UniqueIds.Rows[0][Constants.RolloverIdValue], null);
            metaData.CacheSize = 0;
            return metaData;
        }

        long GetIdFromCache(string UniqueIdKey
            , uint RangeAmount = 1)
        {
            // if not caching 
            // , get from database
            if (_idCacheSize <= 1)
                return GetUniqueIdRangeFromDb(UniqueIdKey, RangeAmount);

            // if key does not exist in cache
            if (!_idCacheLocks.Exists(UniqueIdKey))
                SetupKey(UniqueIdKey);
            
            // otherwise, lock the key
            lock (_idCacheLocks.Get(UniqueIdKey))
            {
                // get the metadata for the key
                IdMetaData metadata = _idCache.Get(UniqueIdKey);
                // verify the cache size, now that we've obtained lock
                // if there is not enough cache, get from database
                if (metadata.CacheSize < RangeAmount)
                {
                    uint cacheAmount = RangeAmount + _idCacheSize;
                    metadata.OriginalIdValue = GetUniqueIdRangeFromDb(UniqueIdKey, cacheAmount);
                    metadata.IdValue = metadata.OriginalIdValue - _idCacheSize;
                    metadata.CacheSize = cacheAmount - RangeAmount;
                }
                else
                {
                    // otherwise, get from cache
                    metadata.IdValue += RangeAmount;
                    metadata.CacheSize -= RangeAmount;
                }
                _idCache.Set(UniqueIdKey, metadata);
                return metadata.IdValue;
            }
        }

        long GetUniqueIdRangeFromDb(string UniqueIdKey
             , uint RangeAmount = 1)
        {
            DbCommand dbCmd = _dbMgr.CloneDbCommand(_dbCmdCache.Get(Constants.USP_UniqueIdsGetNextRange));
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdKey)].Value = UniqueIdKey;
            dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.RangeAmount)].Value = RangeAmount;
            _dbMgr.ExecuteNonQuery(dbCmd, null);

            return Convert.ToInt64(_dbMgr.GetOutParamValue(dbCmd, _dbMgr.BuildParameterName(Constants.UniqueIdValue)));
        }

        /// <summary>
        /// Function to return any remaining cached UniqueId numbers if caching UniqueId numbers
        /// and the last obtained UniqueId value is still there (no other requests were made)
        /// NOTE: If caching UniqueId numbers is true, then this method will reset the current
        /// UniqueIdValue for ht UniqueIdKey if the Value was not changed from when it was originally
        /// retrieved.
        /// </summary>
        void ClearUniqueIdsCache()
        {
            // now check the cache for any sequences that were used
            DbCommandMgr dbCmdMgr = new DbCommandMgr(_dbMgr);
            foreach (string idKey in _idCache.Keys)
            {
                IdMetaData metaData = _idCache.Get(idKey);
                if (metaData.IdValue != 0 && metaData.IdValue != metaData.OriginalIdValue)
                {
                    DbCommand dbCmd = _dbCmdCache.Get(Constants.ResetUniqueIdValue);
                    dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdKey)].Value = metaData.IdKey;
                    dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdValue)].Value = metaData.OriginalIdValue;
                    dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.UniqueIdValue, true)].Value = metaData.OriginalIdValue - metaData.CacheSize;
                    dbCmdMgr.AddDbCommand(dbCmd);
                }
            }
            if (!dbCmdMgr.IsNoOpDbCommand)
                dbCmdMgr.ExecuteNonQuery();
        }



    }
}
