using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Data;
using System.Data.Common;

using CV.Global;
using CV.Database;
using CV.Cache;

namespace CV.TaskProcessing
{
    /// <summary>
    /// Class which handles all the task registrations for the given assembly.
    /// All tasks found in the given assembly which derive from the TaskProcess abstract class
    /// are 'merged' into the database table TaskRegistrations
    /// </summary>
    public class TaskRegistration
    {
        static CacheMgr<string, DbCommand> _dbCmdCache
                = new CacheMgr<string, DbCommand>(StringComparer.CurrentCultureIgnoreCase);

        static DbCommand GetDbCommandFromCache(DatabaseMgr DbMgr, string CacheKey)
        {
            if (!_dbCmdCache.Exists(CacheKey))
                switch (CacheKey)
                {
                    case Constants.BuildCmdGetRegisteredTasksList:
                        {
                            // get all the columns and all the rows
                            DmlMgr dmlSelectMgr = DbMgr.DbCatalogGetDmlMgr(Database.Constants.SCHEMA_CORE
                                     , Constants.TaskRegistrations);
                            // return the DbCommand object
                            _dbCmdCache.Set(CacheKey, DbMgr.BuildSelectDbCommand(dmlSelectMgr, null));
                            break;
                        }
                    default:
                        throw new ExceptionMgr(typeof(TaskRegistration).ToString()
                                ,new ArgumentOutOfRangeException(
                            string.Format("CacheKey: {0}, not found in cache.", CacheKey)));
                }
            return DbMgr.CloneDbCommand(_dbCmdCache.Get(CacheKey));
        }

        /// <summary>
        /// Constructor accepting the assembly and a usercode performing the registration
        /// </summary>
        /// <param name="dbMgr">DataAccess manager object</param>
        /// <param name="assemblyName">Fully qualified assembly name</param>
        /// <param name="assemblyFileName">Fully qualified assembly path and filename</param>
        /// <param name="userCode">Usercode of user performing registration</param>
        /// <returns></returns>
        public static Int32 RegisterAssemblyTasks(DatabaseMgr dbMgr
                , string assemblyName
                , string assemblyFileName
                , Int32? userCode)
        {
            // Load the assemlb
            Assembly asm = Assembly.LoadFrom(assemblyFileName);
            DbCommandMgr cmdMgr = new DbCommandMgr(dbMgr);
            DmlMgr dmlMgr = new DmlMgr(dbMgr, Database.Constants.SCHEMA_CORE, Constants.TaskRegistrations);
            dmlMgr.AddColumn(Constants.TaskId, dbMgr.BuildParameterName(Constants.TaskId), MergeColumnOptions.ForInsertOnly);
            dmlMgr.AddColumn(Constants.AssemblyName, dbMgr.BuildParameterName(Constants.AssemblyName));
            dmlMgr.AddColumn(Constants.TaskDescription, dbMgr.BuildParameterName(Constants.TaskDescription));
            dmlMgr.AddColumn(Constants.LastRegisteredDate, DateTimeKind.Unspecified);
            // if usercode was provided add it to last mod key
            if (userCode.HasValue)
            {
                dmlMgr.AddColumn(Constants.LastModifiedUserCode, dbMgr.BuildParameterName(Constants.LastModifiedUserCode));
                dmlMgr.AddColumn(Constants.LastModifiedDateTime, DateTimeKind.Unspecified);
            }
            dmlMgr.SetWhereCondition((j) =>
                    j.Column(Constants.TaskId) ==
                        j.Parameter(Constants.TaskRegistrations
                        , Constants.TaskId
                        , dbMgr.BuildParameterName(Constants.TaskId)));
            // build a merge statement
            DbCommand dbCmd = dbMgr.BuildMergeDbCommand(dmlMgr);

            int typesFound = 0;
            // set the values for the dbCommand
            foreach (Type t in ObjectFactory.SearchTypes<TaskProcess>(asm))
            {
                // we must create the task object in or to have access to the TaskDesve
                TaskProcess tp = ObjectFactory.Create<TaskProcess>(assemblyFileName, t.FullName, null, null, null, null, null, null);
                dbCmd.Parameters[dbMgr.BuildParameterName(Constants.TaskId)].Value = t.FullName; 
                dbCmd.Parameters[dbMgr.BuildParameterName(Constants.AssemblyName)].Value = assemblyName;
                dbCmd.Parameters[dbMgr.BuildParameterName(Constants.TaskDescription)].Value = tp.TaskDescription();
                if (userCode.HasValue)
                    dbCmd.Parameters[dbMgr.BuildParameterName(Constants.LastModifiedUserCode)].Value = userCode.Value;
                cmdMgr.AddDbCommand(dbCmd);
                ++typesFound;
            }
            // register the task (update it if exists otherwise insert)
            cmdMgr.ExecuteNonQuery();
            return typesFound;
        }

        /// <summary>
        /// Returns the list of registered tasks as a datatable
        /// </summary>
        /// <param name="dbMgr">DataAccess manager object</param>
        /// <returns></returns>
        public static DataTable GetRegisteredTasks(DatabaseMgr dbMgr)
        {
            DbCommand dbCmd = GetDbCommandFromCache(dbMgr, Constants.BuildCmdGetRegisteredTasksList);
            return dbMgr.ExecuteDataSet(dbCmd, null, null).Tables[0];
        }
 
    }
}
