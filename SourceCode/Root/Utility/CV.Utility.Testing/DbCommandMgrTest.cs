using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Threading;

using CV.Database;
using CV.Database.Sequence;
using CV.Database.Provider;
using CV.Cache;
using CV.Logging;

namespace CV.Utility.Testing
{
    internal class DbCommandMgrTest
    {
        static bool _stopInsert = false;
        static CacheMgr<string, DbCommand> _dbCmdCache = new CacheMgr<string, DbCommand>(StringComparer.CurrentCultureIgnoreCase);
        DatabaseMgr _dbMgr = null;
        LoggingMgr _logMgr = null;
        byte _numThreads;
        byte _numCommands;

        internal DbCommandMgrTest(DatabaseMgr DbMgr, LoggingMgr LogMgr, byte NumThreads = 4, byte NumCommands = 4)
        {
            _dbMgr = DbMgr;
            _logMgr = LogMgr;
            _numThreads = NumThreads;
            _numCommands = NumCommands;
        }

        internal static void Stop()
        {
            _stopInsert = true;
        }

        internal void Insert(bool start)
        {
            _stopInsert = !start;
            if (!_stopInsert)
            {
                for (int i = 0; i < _numThreads; i++)
                {
                    Thread t = new Thread(BeginInsert);
                    t.IsBackground = true;
                    t.Start();
                }
                /*
                Action insertAction = new Action(() =>
                {
                    BeginInsert();
                });

                ThreadPool.SetMinThreads(_numThreads, _numThreads);
                System.Threading.Tasks.Parallel.b.Invoke(insertAction);
                 * */
            }
        }

        void BeginInsert()
        {
            // generate a compound command
            using (SequenceMgr seqMgr = new SequenceMgr(_dbMgr, 1))
            {
                while (!_stopInsert)
                {
                    DbCommandMgr dbCmdMgr = new DbCommandMgr(_dbMgr);
                    for (int i = 0; i < _numCommands; i++)
                    {
                        DbCommand dbCmd = GetDbCommandFromCache(Constants.InsertCommand);

                        Int64 appSequenceId = seqMgr.GetNextSequence(Constants.AppSequenceId);

                        Random r = new Random();
                        dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.AppSequenceId)].Value = appSequenceId;
                        dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.AppSequenceName)].Value = GenerateRandomName();
                        dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.AppLocalTime)].Value = DateTime.Now;
                        dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.AppSynchTime)].Value = _dbMgr.DbSynchTime;

                        string randomText = "Test String: " + "".PadRight(r.Next(1, 20), 'X');
                        dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.Remarks)].Value = string.Format(
                                "{0}:{1};{2}"
                                , Environment.MachineName
                                , System.Threading.Thread.CurrentThread.ManagedThreadId
                                , randomText);
                        randomText = "Test clob: " + "".PadRight(r.Next(1, 100), 'X');
                        dbCmd.Parameters[_dbMgr.BuildParameterName(Constants.ExtraData)].Value = randomText;

                        dbCmdMgr.AddDbCommand(dbCmd);
                    }

                    _logMgr.Trace(string.Format("ThreadID: {0} Executing dbCmd"
                                , Thread.CurrentThread.ManagedThreadId)
                                , Logging.TraceLevels.Level1);
                    dbCmdMgr.ExecuteNonQuery();
                    System.Threading.Thread.Sleep(100);
                }
            }
        }

        DbCommand GetDbCommandFromCache(string cacheKey)
        {
            if (!_dbCmdCache.Exists(cacheKey))
            {
                switch (cacheKey)
                {
                    case Constants.InsertCommand:
                        {
                            DmlMgr dmlInsert = _dbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                                    , Constants.TABLE_TestData);
                            DbFunctionStructure autogenerate = new DbFunctionStructure();
                            if (_dbMgr.DatabaseType == DatabaseTypeName.SqlServer
                                || _dbMgr.DatabaseType == DatabaseTypeName.Db2)
                                autogenerate.AutoGenerate = true; // identity column
                            else
                            { // oracle sequence
                                autogenerate.AutoGenerate = false;
                                autogenerate.FunctionBody = Database.Constants.SCHEMA_CORE + ".DbSequenceId_Seq.nextVal";
                            }

                            dmlInsert.AddColumn(Constants.AppSequenceId, _dbMgr.BuildParameterName(Constants.AppSequenceId));
                            dmlInsert.AddColumn(Constants.AppSequenceName, _dbMgr.BuildParameterName(Constants.AppSequenceName));
                            dmlInsert.AddColumn(Constants.AppLocalTime, _dbMgr.BuildParameterName(Constants.AppLocalTime));
                            dmlInsert.AddColumn(Constants.AppSynchTime, _dbMgr.BuildParameterName(Constants.AppSynchTime));
                            dmlInsert.AddColumn(Constants.Remarks, _dbMgr.BuildParameterName(Constants.Remarks));
                            dmlInsert.AddColumn(Constants.ExtraData, _dbMgr.BuildParameterName(Constants.ExtraData));
                            dmlInsert.AddColumn(Constants.DbSequenceId, autogenerate);
                            dmlInsert.AddColumn(Constants.DbServerTime, DateTimeKind.Unspecified); // will default to ddl function
                            DbCommand dbCmd = _dbMgr.BuildInsertDbCommand(dmlInsert);
                            _dbCmdCache.Set(cacheKey, dbCmd);
                            break;
                        }
                    default:
                        throw new ArgumentOutOfRangeException(string.Format("CacheKey: {0} not found in cache.", cacheKey));
                }
            }
            return _dbMgr.CloneDbCommand(_dbCmdCache.Get(cacheKey));
        }

        static string GenerateRandomName()
        {
            Random r = new Random();
            return "".PadRight(r.Next(6, 12), (char)r.Next(65, 90)) + " " +
                    "".PadRight(r.Next(6, 18), (char)r.Next(65, 90));
        }
    }
}
