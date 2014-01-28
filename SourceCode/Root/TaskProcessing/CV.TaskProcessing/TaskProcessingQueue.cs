using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;

using CV.Global;
using CV.Database;
using CV.Logging;
using CV.Cache;

namespace CV.TaskProcessing
{
    public class TaskProcessingQueue
    {
        /// <summary>
        /// Enumeration for returning the Task Processing Queue Records
        /// </summary>
#pragma warning disable 1591 // disable the xmlComments warning
        public enum ListEnum { All, NotQueued, Queued, InProcess, Failed, Succeeded };
        public enum StatusCodeEnum { NotQueued = 0, Queued = 32, InProcess = 64, Failed = 128, Succeeded = 255 };

        static CacheMgr<string, DbCommand> _dbCmdCache
        = new CacheMgr<string, DbCommand>(StringComparer.CurrentCultureIgnoreCase);
#pragma warning restore 1591 // restore the xmlComments warning

        static DbCommand GetDbCommandFromCache(DatabaseMgr DbMgr, string CacheKey)
        {
            if (!_dbCmdCache.Exists(CacheKey))
                switch (CacheKey)
                {
                    case Constants.BuildCmdGetTPQListByWaitDateTime:
                        {
                            DmlMgr dmlSelectMgr = DbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                                     , Constants.TaskProcessingQueue);
                            string joinTable = dmlSelectMgr.AddJoin(Database.Constants.SCHEMA_CORE
                                    , Constants.TaskStatusCodes
                                    , DbTableJoinType.Inner
                                    , j => j.AliasedColumn(Constants.StatusCode)
                                        == j.JoinAliasedColumn(Constants.StatusCode)
                                    , Constants.StatusName);
                            dmlSelectMgr.SetWhereCondition(w => w.AliasedColumn(Constants.StatusCode)
                                    == w.Parameter(Constants.StatusCode));
                            dmlSelectMgr.AddOrderByColumnAscending(Constants.StatusCode);
                            dmlSelectMgr.AddOrderByColumnAscending(Constants.WaitForDateTime);
                            dmlSelectMgr.AddOrderByColumnAscending(Constants.TaskQueueCode);
                            _dbCmdCache.Set(CacheKey, DbMgr.BuildSelectDbCommand(dmlSelectMgr, null));
                            break;
                        }
                    case Constants.BuildCmdGetTPQListByStatusDateTime:
                        {
                            DmlMgr dmlSelectMgr = DbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                                     , Constants.TaskProcessingQueue);
                            string joinTable = dmlSelectMgr.AddJoin(Database.Constants.SCHEMA_CORE
                                    , Constants.TaskStatusCodes
                                    , DbTableJoinType.Inner
                                    , j => j.AliasedColumn(Constants.StatusCode)
                                        == j.JoinAliasedColumn(Constants.StatusCode)
                                    , Constants.StatusName);
                            dmlSelectMgr.SetWhereCondition(w => w.AliasedColumn(Constants.StatusCode)
                                    == w.Parameter(Constants.StatusCode));
                            dmlSelectMgr.AddOrderByColumnAscending(Constants.StatusCode);
                            dmlSelectMgr.AddOrderByColumnDescending(Constants.StatusDateTime);
                            dmlSelectMgr.AddOrderByColumnAscending(Constants.TaskQueueCode);
                            _dbCmdCache.Set(CacheKey, DbMgr.BuildSelectDbCommand(dmlSelectMgr, null));
                            break;
                        }
                    case Constants.BuildCmdGetTPQListByTaskId:
                        {
                            DmlMgr dmlSelectMgr = DbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                                     , Constants.TaskProcessingQueue);
                            string joinTable = dmlSelectMgr.AddJoin(Database.Constants.SCHEMA_CORE
                                    , Constants.TaskStatusCodes
                                    , DbTableJoinType.Inner
                                    , j => j.AliasedColumn(Constants.StatusCode)
                                        == j.JoinAliasedColumn(Constants.StatusCode)
                                    , Constants.StatusName);
                            dmlSelectMgr.AddOrderByColumnAscending(Constants.TaskId);
                            _dbCmdCache.Set(CacheKey, DbMgr.BuildSelectDbCommand(dmlSelectMgr, null));
                            break;
                        }
                    case Constants.BuildCmdGetTaskDependencies:
                        {
                            DmlMgr dmlSelectMgr = DbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                                     , Constants.TaskDependencies);
                            string jointable = dmlSelectMgr.AddJoin(Database.Constants.SCHEMA_CORE
                                    , Constants.TaskProcessingQueue
                                    , DbTableJoinType.Inner
                                    , j => j.AliasedColumn(Constants.WaitTaskQueueCode)
                                        == j.JoinAliasedColumn(Constants.TaskQueueCode)
                                    , Constants.TaskId);
                            dmlSelectMgr.SetWhereCondition(w => w.Column(Constants.TaskQueueCode) == w.Parameter(Constants.TaskQueueCode));
                            dmlSelectMgr.AddOrderByColumnAscending(Constants.TaskQueueCode);
                            dmlSelectMgr.AddOrderByColumnAscending(Database.Constants.SCHEMA_CORE, Constants.TaskProcessingQueue, Constants.TaskId);
                            _dbCmdCache.Set(CacheKey, DbMgr.BuildSelectDbCommand(dmlSelectMgr, null));
                            break;
                        }
                    case Constants.GetDependentTasksCmd:
                        {
                            StringBuilder sb = new StringBuilder();
                            // we do not have any helper functions to build a recursive query; so we
                            // are building this manually.
                            sb.AppendFormat("WITH Dependencies ({0}, {1}, Level){2} AS{2} ({2}"
                                    , TaskProcessing.Constants.TaskQueueCode
                                    , TaskProcessing.Constants.WaitTaskQueueCode
                                    , Environment.NewLine);
                            sb.AppendFormat("SELECT tpq.{0}, {1}, 1 AS Level{2}"
                                    , TaskProcessing.Constants.TaskQueueCode
                                    , TaskProcessing.Constants.WaitTaskQueueCode
                                    , Environment.NewLine);
                            sb.AppendFormat("FROM {0}.{1} tpq{2}"
                                    , Database.Constants.SCHEMA_CORE
                                    , TaskProcessing.Constants.TaskProcessingQueue
                                    , Environment.NewLine);
                            sb.AppendFormat("INNER JOIN {0}.{1} td{2}"
                                    , Database.Constants.SCHEMA_CORE
                                    , TaskProcessing.Constants.TaskDependencies
                                    , Environment.NewLine);
                            sb.AppendFormat("ON tpq.{0} = td.{0}{1}UNION ALL{1}"
                                    , TaskProcessing.Constants.TaskQueueCode
                                    , Environment.NewLine);
                            sb.AppendFormat("SELECT d.{0}, td.{1}, Level + 1{2}"
                                    , TaskProcessing.Constants.TaskQueueCode
                                    , TaskProcessing.Constants.WaitTaskQueueCode
                                    , Environment.NewLine);
                            sb.AppendFormat("FROM Dependencies d{0}"
                                    , Environment.NewLine);
                            sb.AppendFormat("INNER JOIN {0}.{1} td{2}"
                                    , Database.Constants.SCHEMA_CORE
                                    , TaskProcessing.Constants.TaskDependencies
                                    , Environment.NewLine);
                            sb.AppendFormat("ON d.{0} = td.{1}{2})"
                                    , TaskProcessing.Constants.WaitTaskQueueCode
                                    , TaskProcessing.Constants.TaskQueueCode
                                    , Environment.NewLine);
                            sb.AppendFormat("SELECT {0}, {1}, Level{2}"
                                    , TaskProcessing.Constants.TaskQueueCode
                                    , TaskProcessing.Constants.WaitTaskQueueCode
                                    , Environment.NewLine);
                            sb.AppendFormat("FROM Dependencies d{0}"
                                    , Environment.NewLine);
                            sb.AppendFormat("WHERE {0} = {1}{2};"
                                    , TaskProcessing.Constants.WaitTaskQueueCode
                                    , DbMgr.BuildBindVariableName(TaskProcessing.Constants.WaitTaskQueueCode)
                                    , Environment.NewLine);
                            DbCommand dbCmd = DbMgr.BuildSelectDbCommand(sb.ToString(), null);
                            DbParameterCollection dbParams = DbMgr.BuildNoOpDbCommand().Parameters;
                            DbMgr.CopyParameterToCollection(dbCmd.Parameters
                                , DbMgr.CreateParameter(TaskProcessing.Constants.WaitTaskQueueCode
                                , DbType.Int32
                                , null
                                , 0
                                , ParameterDirection.Input
                                , DBNull.Value));
                            _dbCmdCache.Set(CacheKey, dbCmd);
                            break;
                        }
                    default:
                        throw new ExceptionMgr(typeof(TaskProcessingQueue).ToString()
                                ,new ArgumentOutOfRangeException(
                                    string.Format("CacheKey: {0}, not found in cache.", CacheKey)));
                }
            return DbMgr.CloneDbCommand(_dbCmdCache.Get(CacheKey));
        }

        /// <summary>
        /// Returns the entire data table of application session records in the 
        /// database based on the given enumeration.
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr object</param>
        /// <param name="tpqList">Enumeration indicating what type of session records to return</param>
        /// <returns>DataTable of Task Processing Queue records</returns>
        public static DataTable TaskProcessingQueueList(DatabaseMgr DbMgr, ListEnum tpqList)
        {
            DbCommand dbCmd;
            switch (tpqList)
            {
                case ListEnum.Queued:
                    dbCmd = GetDbCommandFromCache(DbMgr, Constants.BuildCmdGetTPQListByWaitDateTime);
                    dbCmd.Parameters[DbMgr.BuildParameterName(Constants.StatusCode)].Value = Convert.ToByte(tpqList);
                    break;
                case ListEnum.Failed:
                case ListEnum.Succeeded:
                case ListEnum.InProcess:
                case ListEnum.NotQueued:
                    dbCmd = GetDbCommandFromCache(DbMgr, Constants.BuildCmdGetTPQListByStatusDateTime);
                    dbCmd.Parameters[DbMgr.BuildParameterName(Constants.StatusCode)].Value = Convert.ToByte(tpqList);
                    break;
                default:
                    dbCmd = GetDbCommandFromCache(DbMgr, Constants.BuildCmdGetTPQListByTaskId);
                    break;
            }

            return DbMgr.ExecuteDataSet(dbCmd, null, null).Tables[0];
        }


        /// <summary>
        /// Builds the DbCommand to get a list of the given task's dependency records
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr object</param>
        /// <param name="taskQueueCode">Unique identifier of the task whose dependencies will be returned</param>
        /// <returns>DateTable of the given task's dependencies</returns>
        public static DataTable TaskDependenciesList(DatabaseMgr DbMgr, Int32 taskQueueCode)
        {
            DbCommand dbCmd = GetDbCommandFromCache(DbMgr, Constants.BuildCmdGetTaskDependencies);
            dbCmd.Parameters[DbMgr.BuildParameterName(Constants.TaskQueueCode)].Value = taskQueueCode;
            return DbMgr.ExecuteDataSet(dbCmd, null, null).Tables[0];
        }

        /// <summary>
        /// Builds the DbCommand to get a list of a task's dependencies
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr object</param>
        /// <returns>DbCommand Object with DbNull Parameter values</returns>
        static DbCommand BuildCmdGetTaskDependencies(DatabaseMgr dbMgr)
        {
            DmlMgr dmlSelectMgr = dbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                     , Constants.TaskDependencies);
            string jointable = dmlSelectMgr.AddJoin(Database.Constants.SCHEMA_CORE
                    , Constants.TaskProcessingQueue
                    , DbTableJoinType.Inner
                    , j => j.AliasedColumn(Constants.WaitTaskQueueCode)
                        == j.JoinAliasedColumn(Constants.TaskQueueCode)
                    , Constants.TaskId);
            dmlSelectMgr.SetWhereCondition(w => w.Column(Constants.TaskQueueCode) == w.Parameter(Constants.TaskQueueCode));
            dmlSelectMgr.AddOrderByColumnAscending(Constants.TaskQueueCode);
            dmlSelectMgr.AddOrderByColumnAscending(Database.Constants.SCHEMA_CORE, Constants.TaskProcessingQueue, Constants.TaskId);
            return dbMgr.BuildSelectDbCommand(dmlSelectMgr, null);
        }

        /// <summary>
        /// Builds the DbCommand to delete a dependency record for the given taskQueueItem from the
        /// TaskDependencies table.
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr object</param>
        /// <param name="taskQueueItem">Unique Identifier of the TaskDependencies record to delete</param>
        /// <returns>DbCommand Object with given Parameter values</returns>
        public static DbCommand GetDeleteDependencyTaskCmd(DatabaseMgr dbMgr, DataRow taskQueueItem)
        {
            if (taskQueueItem == null
                || !taskQueueItem.Table.Columns.Contains(TaskProcessing.Constants.TaskQueueCode))
                throw new ExceptionMgr(typeof(TaskProcessingQueue).ToString()
                    , new ArgumentNullException("DataRow (taskQueueItem) containing TaskProcessingQueue data was empty"));

            DmlMgr dmlMgr = dbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                        , TaskProcessing.Constants.TaskDependencies);
            dmlMgr.SetWhereCondition(w => w.Column(TaskProcessing.Constants.TaskQueueCode)
                    == w.Parameter(TaskProcessing.Constants.TaskQueueCode));
            DbCommand dbCmd = dbMgr.BuildDeleteDbCommand(dmlMgr);
            dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.TaskQueueCode)].Value
                    = Convert.ToInt32(taskQueueItem[TaskProcessing.Constants.TaskQueueCode]);
            return dbCmd;
        }

        /// <summary>
        /// Builds the DbCommand to delete a task queue record for the given taskQueueItem from the
        /// TaskProcessingQueue table.
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr object</param>
        /// <param name="taskQueueItem">Unique Identifier of the TaskProcessingQueue record to delete</param>
        /// <returns>DbCommand Object with given Parameter values</returns>
        public static DbCommand GetDeleteQueueItemCmd(DatabaseMgr dbMgr, DataRow taskQueueItem)
        {
            if (taskQueueItem == null
                || !taskQueueItem.Table.Columns.Contains(TaskProcessing.Constants.TaskQueueCode))
                throw new ExceptionMgr(typeof(TaskProcessingQueue).ToString(), new ArgumentNullException(
                    "DataRow (taskQueueItem) containing TaskProcessingQueue data was empty"));

            DmlMgr dmlMgr = dbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                        , TaskProcessing.Constants.TaskProcessingQueue);
            dmlMgr.SetWhereCondition(w => w.Column(TaskProcessing.Constants.TaskQueueCode)
                    == w.Parameter(TaskProcessing.Constants.TaskQueueCode));
            DbCommand dbCmd = dbMgr.BuildDeleteDbCommand(dmlMgr);
            dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.TaskQueueCode)].Value
                    = Convert.ToInt32(taskQueueItem[TaskProcessing.Constants.TaskQueueCode]);
            return dbCmd;
        }

        /// <summary>
        /// Builds the DbCommand to either insert or change (update) the TaskProcessingQueue
        /// based on the given set of editedColumns.
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr object</param>
        /// <param name="taskQueueItem">A Datarow containing the parameter values; If empty, then it is an insert.</param>
        /// <param name="editedColumns">The Dictionary of edited columns</param>
        /// <param name="userCode">The userCode of the person who changed the data</param>
        /// <returns>DbCommand Object with DbNull Parameter values</returns>
        public static DbCommand GetDmlCmd(DatabaseMgr dbMgr
                , DataRow taskQueueItem
                , Dictionary<string, object> editedColumns
                , Int32? userCode = null)
        {
            DbCommand dbCmd = null;
            DmlMgr dmlMgr = dbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                        , TaskProcessing.Constants.TaskProcessingQueue);

            foreach (string column in editedColumns.Keys)
                dmlMgr.AddColumn(column);
            if (taskQueueItem == null) // add new item
            {
                dmlMgr.AddColumn(TaskProcessing.Constants.LastModifiedUserCode);
                dmlMgr.AddColumn(TaskProcessing.Constants.LastModifiedDateTime);
                dbCmd = dbMgr.BuildInsertDbCommand(dmlMgr);
            }

            else dbCmd = dbMgr.BuildChangeDbCommand(dmlMgr, TaskProcessing.Constants.LastModifiedUserCode
                    , TaskProcessing.Constants.LastModifiedDateTime);

            foreach (string column in editedColumns.Keys)
                dbCmd.Parameters[dbMgr.BuildParameterName(column)].Value
                        = editedColumns[column];

            if (taskQueueItem == null) // add new
            {
                if (userCode.HasValue)
                {
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode)].Value
                        = userCode.Value;
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime)].Value
                        = dbMgr.DbSynchTime;
                }
                else
                {
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode)].Value
                        = DBNull.Value;
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime)].Value
                        = DBNull.Value;
                }
            }
            else  // change; where condition params
            {
                dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode)].Value
                    = taskQueueItem[TaskProcessing.Constants.LastModifiedUserCode];
                dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime)].Value
                    = taskQueueItem[TaskProcessing.Constants.LastModifiedDateTime];
                // set portion of the update
                if (userCode.HasValue)
                {
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode, true)].Value
                        = userCode.Value;
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime, true)].Value
                        = dbMgr.DbSynchTime;
                }
                else
                {
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode, true)].Value
                        = DBNull.Value;
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime, true)].Value
                        = DBNull.Value;
                }
            }

            return dbCmd;
        }


        /// <summary>
        /// Builds the DbCommand to delete the task dependency(s) for the given taskQueueCode.
        /// </summary>
        /// <param name="DbMgr">DatabaseMgr object</param>
        /// <param name="TaskQueueItem">Datarow containing the parameter values</param>
        /// <param name="DeleteAll">Indicates whether or not to delete all dependencies.</param>
       /// <returns>DbCommand Object with DbNull Parameter values</returns>
         public static DbCommand GetDeleteQueueItemCmd(DatabaseMgr DbMgr, DataRow TaskQueueItem, bool DeleteAll)
        {
            if (TaskQueueItem == null
                || !TaskQueueItem.Table.Columns.Contains(TaskProcessing.Constants.TaskQueueCode))
                throw new ExceptionMgr(typeof(TaskProcessingQueue).ToString()
                    , new ArgumentNullException( "DataRow (taskQueueItem) containing TaskDependency data was empty"));

            DmlMgr dmlMgr = DbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                        , TaskProcessing.Constants.TaskDependencies);
            dmlMgr.SetWhereCondition(w => w.Column(TaskProcessing.Constants.TaskQueueCode)
                    == w.Parameter(TaskProcessing.Constants.TaskQueueCode));
            if (!DeleteAll)
            {
                System.Linq.Expressions.Expression waitTaskExp =
                    DbPredicate.CreatePredicatePart(w => w.Column(TaskProcessing.Constants.WaitTaskQueueCode)
                            == w.Parameter(TaskProcessing.Constants.WaitTaskQueueCode));
                dmlMgr.AddToWhereCondition(System.Linq.Expressions.ExpressionType.AndAlso, waitTaskExp);
            }
            DbCommand dbCmd = DbMgr.BuildDeleteDbCommand(dmlMgr);
            dbCmd.Parameters[DbMgr.BuildParameterName(TaskProcessing.Constants.TaskQueueCode)].Value
                    = Convert.ToInt32(TaskQueueItem[TaskProcessing.Constants.TaskQueueCode]);
            if (!DeleteAll)
            {
                dbCmd.Parameters[DbMgr.BuildParameterName(TaskProcessing.Constants.WaitTaskQueueCode)].Value
                        = Convert.ToInt32(TaskQueueItem[TaskProcessing.Constants.WaitTaskQueueCode]);
            }
            return dbCmd;
        }


        /// <summary>
        /// Builds the DbCommand to Change the value of the edited columns found in the given dictionary.
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr object</param>
        /// <param name="taskQueueItem">The data row containing the values of the parameters</param>
        /// <param name="editedColumns">The columns that need to be updated</param>
        /// <param name="userCode">The userCode that updated the data</param>
        /// <returns>DbCommand Object with DbNull Parameter values</returns>
        public static DbCommand GetDependencyDmlCmd(DatabaseMgr dbMgr
                , DataRow taskQueueItem
                , Dictionary<string, object> editedColumns
                , Int32? userCode = null)
        {
            DbCommand dbCmd = null;
            DmlMgr dmlMgr = dbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                        , TaskProcessing.Constants.TaskDependencies);

            foreach (string column in editedColumns.Keys)
                dmlMgr.AddColumn(column);
            if (taskQueueItem == null) // add new item
            {
                dmlMgr.AddColumn(TaskProcessing.Constants.LastModifiedUserCode);
                dmlMgr.AddColumn(TaskProcessing.Constants.LastModifiedDateTime);
                dbCmd = dbMgr.BuildInsertDbCommand(dmlMgr);
            }

            else dbCmd = dbMgr.BuildChangeDbCommand(dmlMgr, TaskProcessing.Constants.LastModifiedUserCode
                    , TaskProcessing.Constants.LastModifiedDateTime);

            foreach (string column in editedColumns.Keys)
                dbCmd.Parameters[dbMgr.BuildParameterName(column)].Value
                        = editedColumns[column];

            if (taskQueueItem == null) // add new
            {
                if (userCode.HasValue)
                {
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode)].Value
                        = userCode.Value;
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime)].Value
                        = dbMgr.DbSynchTime;
                }
                else
                {
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode)].Value
                        = DBNull.Value;

                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime)].Value
                        = DBNull.Value;
                }
            }
            else  // change; where condition params
            {
                dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode)].Value
                    = taskQueueItem[TaskProcessing.Constants.LastModifiedUserCode];
                dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime)].Value
                    = taskQueueItem[TaskProcessing.Constants.LastModifiedDateTime];
                // set portion of the update
                if (userCode.HasValue)
                {
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode, true)].Value
                        = userCode.Value;
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime, true)].Value
                        = dbMgr.DbSynchTime;
                }
                else
                {
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedUserCode, true)].Value
                        = DBNull.Value;
                    dbCmd.Parameters[dbMgr.BuildParameterName(TaskProcessing.Constants.LastModifiedDateTime, true)].Value
                        = DBNull.Value;
                }
            }

            return dbCmd;
        }

        /// <summary>
        /// Returns the set of dependency relationships where the given task is the dependent task
        /// </summary>
        /// <param name="dbMgr">DbAccessMgr object instance</param>
        /// <param name="WaitTaskQueueCode">The taskQueueCode that other tasks may be dependent on</param>
        /// <returns>The DataTable of dependency relationhips</returns>
        public static DataTable GetDependentTasks(DatabaseMgr DbMgr
                , Int32 WaitTaskQueueCode)
        {
            DbCommand dbCmd = GetDbCommandFromCache(DbMgr, Constants.GetDependentTasksCmd);

            dbCmd.Parameters[DbMgr.BuildParameterName(TaskProcessing.Constants.WaitTaskQueueCode)].Value
                    = WaitTaskQueueCode;

            return DbMgr.ExecuteDataSet(dbCmd, null, null).Tables[0];
        }

    }
}
