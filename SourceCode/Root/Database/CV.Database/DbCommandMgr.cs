﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Xml;

using CV.Global;
using CV.Database.Provider;

namespace CV.Database
{
    /// <summary>
    /// This class is used to build Compound SQL DbCommands.
    /// Compound SQL can contain any combination of stored procedure calls
    /// and dynamically defined SQL statements.  However these statements will
    /// use Named Parameters and the DbParameterCollection of the DbCommand will
    /// contain the definition and values of those parameters.  
    /// <para>
    /// The class automatically checks for the existance of a parameter and will 
    /// automatically remain the new one if it has a different value.  It will then
    /// update the SQL to match the new parameter name.
    /// </para>
    /// <para>
    /// The advantages of using this class and compound sql is that you can send
    /// multiple statements to the database in a single round trip.  In addition
    /// to the communication efficiency, you can wrap your compound statements within
    /// nested transactions, so there is no need to perform round trips to the db
    /// while holding on to transactions and resources.
    /// </para>
    /// <para>
    /// All arguments in the compound SQL are fully typed parameter bind variables
    /// so performance is equal to that of stored procedures and the statements
    /// can be cached because the command text remains the same only the data in the
    /// parameters is transmitted with each call.
    /// </para>
    /// <para>
    /// One advantage that these DbCommands have over stored procedures is that they
    /// are database Independant.  The class will generate the appropriate code for 
    /// the specific back-end.  The interface to the programmer remains the same.
    /// </para>
    /// <para>
    /// This class has one constructor and should NOT be shared between threads.
    /// Consider this class like you would a stringBuilder.
    /// </para>
    /// <para>
    /// Example: Create a compound SQL that contains a delete and an insert statement.
    /// </para>
    /// <para>
    /// Build the DbCommands
    /// </para>
    /// <para>
    /// DbCommand dbCmdDelete = _dbMgr.BuildDeleteDbCommand(dmlDeleteMgr);
    /// </para>
    /// <para>
    /// DbCommand dbCmdInsert = _dbMgr.BuildInsertDbCommand(dmlInsertMgr);
    /// </para>
    /// <para>
    /// Build the CommandMgr
    /// </para>
    /// <para>
    /// DbCommandMgr dbCmdMgr = new DbCommandMgr(_dbMgr);
    /// </para>
    /// <para>
    /// Set the parameter values
    /// </para>
    /// <para>
    /// dbCmdDelete.Parameters[_dbMgr.BuildParamName(DataAccess.Constants.ApplicationInstance)].Value = tbAppCtrlAppKey.Text;
    /// </para>
    /// <para>
    /// Add the command to the Compound SQL 
    /// </para>
    /// <para>
    /// dbCmdMgr.AddDbCommand(dbCmdDelete);
    /// </para>
    /// <para>
    /// Set the other commands parameters
    /// </para>
    /// <para>
    /// dbCmdInsert.Parameters[_dbMgr.BuildParamName(DataAccess.Constants.ApplicationInstance)].Value = tbAppCtrlAppKey.Text;
    /// </para>
    /// <para>
    /// dbCmdInsert.Parameters[_dbMgr.BuildParamName(DataAccess.Constants.HostAddress)].Value = Environment.MachineName;
    /// </para>
    /// <para>
    /// dbCmdInsert.Parameters[_dbMgr.BuildParamName(DataAccess.Constants.StatusMessage)].Value = "Application Startup";
    /// </para>
    /// <para>
    /// 
    /// Add the insert command to the Compound SQL
    /// </para>
    /// <para>
    /// dbCmdMgr.AddDbCommand(dbCmdInsert);
    /// </para>
    /// <para>
    /// 
    /// Execute the Compound SQL Command
    /// </para>
    /// <para>
    /// dbCmdMgr.ExecuteNonQuery();
    /// </para>
    /// 
    /// </summary>
    public class DbCommandMgr
    {
        private DatabaseMgr _dbMgr = null;
        private DbCommand _dbCommand = null;

        private int _commandCount = 0;  //counter used identify empty dbCommands
        private int _incompleteTransactionBlockCount = 0; //counter for keeping track of nested transactions.
        private bool _commandBlockReady = true;
        private string _noOpDbCommandText;
        private Dictionary<string, List<DbParameter>> _paramAliases = null;


        /// <summary>
        /// Default constructor for the DbCommand Manager class.  
        /// It expects a DatabaseMgr class instance in the constructor.
        /// </summary>
        /// <param name="databaseManager">string entry in configuration file for connection string</param>
        public DbCommandMgr(DatabaseMgr databaseManager)
        {
            _dbMgr = databaseManager;

            _noOpDbCommandText = _dbMgr.DbProvider.NoOpDbCommandText;
            _paramAliases = new Dictionary<string, List<DbParameter>>(StringComparer.CurrentCultureIgnoreCase);
            _dbCommand = _dbMgr.BuildNoOpDbCommand();
            if (_dbMgr.DatabaseType == DatabaseTypeName.Oracle)
                _commandBlockReady = false;
        }

        /// <summary>
        /// Returns the current DbCommand for the CommandBlock
        /// 
        /// NOTE: If a transaction has been begun, but not committed an exception will be thrown.
        /// For Oracle, once this property is accessed, the CommandBlock cannot be changed 
        /// otherwise an exception will be thrown.
        /// </summary>
        public DbCommand DbCommandBlock
        {
            get
            {
                if (_incompleteTransactionBlockCount > 0)
                    throw new ExceptionMgr(this.ToString()
                        , new ApplicationException(string.Format(
                            "There is a missing Commit Transaction statement in one of the nested transactions.")));

                // all oracle & Db2 compound commands must be wrapped in a begin end; block
                if ((_dbMgr.DatabaseType == DatabaseTypeName.Oracle
                    || _dbMgr.DatabaseType == DatabaseTypeName.Db2)
                    && !_commandBlockReady)
                {
                    if (_commandCount == 0)
                        _dbCommand.CommandText = _dbMgr.BuildNoOpDbCommand().CommandText;
                    else
                    {
                        _commandBlockReady = true;
                        if (_dbMgr.DatabaseType == DatabaseTypeName.Oracle)
                        {
                            _dbCommand.CommandText = _dbMgr.DbProvider.FormatCommandText(_dbCommand.CommandText);
                            return _dbMgr.DbProvider.FormatDbCommand(_dbCommand);
                        }
                        else _dbCommand.CommandText = _dbMgr.DbProvider.FormatCommandText(_dbCommand.CommandText);
                    }
                }
                return _dbCommand;  // return the command
            }
        }

        /// <summary>
        /// Returns a copy of the given DbCommand and parameter collection
        /// </summary>
        /// <param name="dbCmd">DbCommand object to copy</param>
        /// <returns>A copy of the DbCommand object and parameter collection</returns>
        internal DbCommand Clone(DbCommand dbCmd)
        {
            if (_commandCount > 0)
            {
                DbCommandMgr cmdMgr = new DbCommandMgr(_dbMgr);
                return cmdMgr.Clone(dbCmd);
            }
            else
            {
                AddDbCommand(dbCmd);
                return _dbCommand;
            }
        }

        /// <summary>
        /// Returns a  string of the DbCommandBlock expanded so that it can be
        /// executed in a Query tool (e.g. SQL Mgmt Studio, or SQLDeveloper)
        /// for debugging.
        /// </summary>
        public string DbCommandBlockDebugText
        {
            get
            {
                // The script version of the DbCommand is back-end specific
                return _dbMgr.DbProvider.GetCommandDebugScript(_dbCommand);
            }

        }

        /// <summary>
        /// Returns true/false depending on whether a DbCommand has been added
        /// to the CommandBlock yet.  CommandBlocks default to NullDbCommand.
        /// </summary>
        public bool IsNoOpDbCommand
        {
            get { return _commandCount == 0; }
        }

        /// <summary>
        /// Used for defining a new nested transactions within the CommandBlock in addition to the
        /// main block defined by the transactional property.
        /// </summary>
        public void TransactionBeginBlock()
        {
            UpdateCommandText(_dbMgr.DbProvider.BeginTransaction(_incompleteTransactionBlockCount++));
            if (_dbMgr.DatabaseType == DatabaseTypeName.Db2)
                _commandBlockReady = false;
        }

        /// <summary>
        /// Used for ending a transaction block.  It will provide code for committing or rollback
        /// </summary>
        public void TransactionEndBlock()
        {
            UpdateCommandText(_dbMgr.DbProvider.CommitTransaction(_incompleteTransactionBlockCount--));
        }

        /// <summary>
        /// Executes the given DbCommand and returns the result.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <returns>Returns int return value.</returns>
        private int ExecuteNonQuery(DbCommand dbCommand)
        {
            return _dbMgr.ExecuteNonQuery(dbCommand, dbCommand.Transaction, null);
        }

        /// <summary>
        /// Calls ExecuteNonQuery with current DbCommand
        /// </summary>
        /// <returns>Returns Int return value.</returns>
        public int ExecuteNonQuery()
        {
            // return the execution of the current compound command
            return ExecuteNonQuery(DbCommandBlock);
        }

        /// <summary>
        /// Executes the given DbCommand and returns the result.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <returns>Returns object.</returns>
        public XmlReader ExecuteXmlReader(DbCommand dbCommand)
        {
            return _dbMgr.ExecuteXmlReader(dbCommand, dbCommand.Transaction, null);
        }

        /// <summary>
        /// Executes the given DbCommand and returns the result.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <returns>Returns object.</returns>
        public object ExecuteScalar(DbCommand dbCommand)
        {
            return _dbMgr.ExecuteScalar(dbCommand, dbCommand.Transaction, null);
        }

        /// <summary>
        /// Calls ExecuteScalar with current DbCommand
        /// </summary>
        /// <returns>Returns object</returns>
        public object ExecuteScalar()
        {
            // return the execution of the current compound command
            return ExecuteScalar(DbCommandBlock);
        }

        /// <summary>
        /// Executes the given DbCommand and returns the result.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// NOTE:
        /// This function should be avoided and instead caller should use ConsumeReader
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <returns>Returns a DbDataReader</returns>
        public IDataReader ExecuteReader(DbCommand dbCommand)
        {
            return _dbMgr.ExecuteReader(dbCommand, dbCommand.Transaction, null);
        }

        /// <summary>
        /// Calls ExecuteReader with current DbCommand
        /// NOTE:
        /// This function should be avoided and instead caller should use ConsumeReader
        /// </summary>
        /// <returns></returns>
        public IDataReader ExecuteReader()
        {
            // return the execution of the current compound command
            return ExecuteReader(DbCommandBlock);
        }


        /// <summary>
        /// Executes the given DbCommand and returns the result.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// If the string[] of TableNames is provided, then the tables in the
        /// resulting dataset will be renamed appropriately.
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <param name="tableNames">List of table names for that tables of the dataset.</param>
        /// <returns>Returns Dataset with named tables.</returns>
        private DataSet ExecuteDataSet(DbCommand dbCommand, List<string> tableNames)
        {
            return _dbMgr.ExecuteDataSet(dbCommand, dbCommand.Transaction, tableNames, null);
        }

        /// <summary>
        /// Calls ExecuteDataSet with current DbCommand
        /// </summary>
        /// <param name="tableNames">A list of table names for resulting DataSet.</param>
        /// <returns>DataSet with named tables.</returns>
        public DataSet ExecuteDataSet(List<string> tableNames)
        {
            // return the execution of the current compound command
            return ExecuteDataSet(DbCommandBlock, tableNames);
        }

        /// <summary>
        /// Calls ExecuteDataSet with DbCommand and will return a dataset
        /// with unnamed tables.
        /// </summary>
        /// <returns>DataSet with unnamed tables.</returns>
        public DataSet ExecuteDataSet()
        {
            // return the execution of the current compound command
            return ExecuteDataSet(DbCommandBlock, null);
        }

        /// <summary>
        /// Executes the given DbCommand and returns the result.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <param name="tableName">Name that result table will be named to.</param>
        /// <returns>Returns a named DataTable.</returns>
        private DataTable ExecuteDataTable(DbCommand dbCommand, string tableName)
        {
            // return the execution of the current compound command
            if (tableName != null && tableName != string.Empty)
            {
                List<string> TableNames = new List<string>(1);
                TableNames.Add(tableName);
                return ExecuteDataSet(dbCommand, TableNames).Tables[0].Copy();
            }
            else return ExecuteDataSet(dbCommand, null).Tables[0].Copy();
        }

        /// <summary>
        /// Calls ExecuteDataTable with the current DbCommand
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns>Returns a named DataTable.</returns>
        public DataTable ExecuteDataTable(string tableName)
        {
            return ExecuteDataTable(DbCommandBlock, tableName);
        }

        /// <summary>
        /// Calls ExecuteDataTable with the current DbCommand
        /// </summary>
        /// <returns>Returns an unnamed DataTable.</returns>
        public DataTable ExecuteDataTable()
        {
            // return the execution of the current compound command
            return ExecuteDataTable(DbCommandBlock, null);
        }

        /// <summary>
        /// Executes the given DbCommand and returns the result of the dataReaderHandler 
        /// function delegate.  It will be given the DataReader and after its execution,
        /// the DataReader will be destroyed. This prevents from caller to have an active
        /// DataReader where they can leave a connection open. If there are errors in the
        /// delegate functions, the datareader is still closed.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <param name="dataReaderHandler">A caller supplied delegate which will be called to consume the DataReader.</param>
        /// <returns>Returns the result of the DataReaderConsumerFunction.</returns>      
        public T ConsumeReader<T>(DbCommand dbCommand
            , Func<DbDataReader, T> dataReaderHandler)
        {
            using (DbDataReader dbRdr = (DbDataReader)ExecuteReader(dbCommand))
            {
                return (T)dataReaderHandler(dbRdr);
            }
        }

        /// <summary>
        /// Calls ConsumeReader with current DbCommand.
        /// </summary>
        /// <param name="dataReaderHandler"></param>
        /// <returns>Returns the result of the DataReaderConsumerFunction.</returns>
        public T ConsumeReader<T>(Func<DbDataReader, T> dataReaderHandler)
        {
            return (T)ConsumeReader(DbCommandBlock, dataReaderHandler);
        }

        /// <summary>
        /// Appends newly added command text to the current command text and returns a DbCommand object.
        /// It then resets the cache of new command text.
        /// </summary>
        /// <param name="newCommandText">The command text to be added</param>
        /// <returns></returns>
        private void UpdateCommandText(string newCommandText)
        {
            // if there were any changes to the dbCommandText 
            if (!string.IsNullOrEmpty(newCommandText))
            {
                // build a new dbCompoundCommand CommandText
                _dbCommand.CommandText = _commandCount == 0
                                                && _incompleteTransactionBlockCount == 0
                                                    ? newCommandText
                                                    : string.Format("{0}{1}{2}"
                                                        , _dbCommand.CommandText
                                                        , Environment.NewLine
                                                        , newCommandText);

            }
        }


        private void UpdateDbCommandBlock(string dynamicSQL
            , DbParameterCollection dbParameters)
        {
            if (_dbMgr.DatabaseType == DatabaseTypeName.Oracle
                && _commandBlockReady)
                throw new ExceptionMgr(this.ToString()
            , new ArgumentException(
                "HINT: Do not access the Property DbCommandBlock until after you are done "
                        + "adding blocks to the DbCommand."));

            // are there any parameters to check against that were already added to the compound command
            if (dbParameters != null && dbParameters.Count > 0)
            {
                foreach (DbParameter newParam in dbParameters)
                {
                    // check to see if the param is in the Compound Command List
                    if (_dbCommand.Parameters.Contains(newParam.ParameterName))
                    {
                        // if the collection contains this parameter
                        // get the param definition and see if it is the same value and can be reused
                        DbParameter existingParam = _dbCommand.Parameters[newParam.ParameterName];
                        if (CompareParamEquality(existingParam, newParam))
                            continue;  // dont add it as it exists and can be reused.

                        if (_paramAliases.ContainsKey(newParam.ParameterName))
                        {
                            List<DbParameter> paramAliases = _paramAliases[newParam.ParameterName];
                            bool matchFound = false;
                            foreach (DbParameter paramAlias in paramAliases)
                                if (CompareParamEquality(paramAlias, newParam))
                                {
                                    bool paramReplaced;
                                    dynamicSQL = ReplaceParam(dynamicSQL
                                            , BuildBindVariableName(newParam.ParameterName)
                                            , BuildBindVariableName(paramAlias.ParameterName)
                                            , string.Format(Constants.ParamDelimiters, Environment.NewLine)
                                            , out paramReplaced);
                                    if (!paramReplaced)
                                        throw new ExceptionMgr(this.ToString()
                                    , new ArgumentException(
                                                 string.Format("Unable to replace param {0} with alias {1} in new db Command {2}{3}"
                                                    , BuildBindVariableName(newParam.ParameterName)
                                                    , BuildBindVariableName(paramAlias.ParameterName)
                                                    , dynamicSQL
                                                    , Environment.NewLine)));

                                    else
                                    {
                                        matchFound = true;
                                        break;
                                    }
                                }
                            if (matchFound)
                                continue;
                        }

                        // if it is a different value, then we need to create a new instance of the parameter
                        // obtain the backend specific parameter name (either for the original name or new instance)
                        // now we need to make sure that the instance does not belong to the original set
                        // because there is a finite size on a param to 
                        int newCount = _dbCommand.Parameters.Count + dbParameters.Count;
                        string newParamInstance;
                        do
                        {
                            string copySuffix = Constants.ParamAliasSuffix + newCount.ToString();
                            newParamInstance = newParam.ParameterName + copySuffix;
                            if (newParamInstance.Length > Constants.ParamNameOracleMaxLength)
                                newParamInstance = newParam.ParameterName.Substring(0, Constants.ParamNameOracleMaxLength - copySuffix.Length)
                                                                            + copySuffix;
                        }
                        while ((_dbCommand.Parameters.Contains(newParamInstance)
                                || dbParameters.Contains(newParamInstance))
                                && ++newCount > 0);

                        {
                            string origParamName = newParam.ParameterName;
                            bool paramReplaced;
                            dynamicSQL = ReplaceParam(dynamicSQL
                                                       , BuildBindVariableName(newParam.ParameterName)
                                                       , BuildBindVariableName(newParamInstance)
                                                       , string.Format(Constants.ParamDelimiters, Environment.NewLine)
                                                       , out paramReplaced);
                            if (paramReplaced)
                                newParam.ParameterName = newParamInstance;
                            else throw new ExceptionMgr(this.ToString()
                                    , new ArgumentException(string.Format("Unable to replace param {0} with {1} in new db Command {2}{3}"
                                                    , BuildBindVariableName(newParam.ParameterName)
                                                    , BuildBindVariableName(newParamInstance)
                                                    , dynamicSQL
                                                    , Environment.NewLine)));

                            if (!_paramAliases.ContainsKey(origParamName))
                                _paramAliases.Add(origParamName, new List<DbParameter>());
                            List<DbParameter> newParamAliases = _paramAliases[origParamName];
                            newParamAliases.Add(newParam);
                            _paramAliases[origParamName] = newParamAliases;
                        }
                    }
                    // if the collection contains this parameter
                    //otherwise add the new parameter to the collection
                    _dbMgr.DbProvider.CopyParameterToCollection(_dbCommand.Parameters, newParam);
                }
            }

            UpdateCommandText(dynamicSQL + (((_dbMgr.DatabaseType == DatabaseTypeName.Oracle
                    || _dbMgr.DatabaseType == DatabaseTypeName.Db2)
                        && !Functions.IsLastCharInText(dynamicSQL, ';')) ? "; " : ""));
            ++_commandCount;    // bump up the command count
        }

        bool CompareParamEquality(DbParameter param1, DbParameter param2)
        {
            if (param1.Direction == ParameterDirection.ReturnValue
                || param2.Direction == ParameterDirection.ReturnValue)
                return false;
            // if they are different types or type object, we cant compare them
            if (param1.DbType == param2.DbType
                && param1.DbType != DbType.Object
                && param1.Direction == param2.Direction
                && param1.Direction == ParameterDirection.Input)
            {
                if (param1.Value == null && param2.Value == null)
                    return true;  // equal.
                else if (param1.Value == DBNull.Value && param2.Value == DBNull.Value)
                    return true;  // equal.
                else if ((param1.Value == null || param1.Value == DBNull.Value)
                   && !(param2.Value == null || param2.Value == DBNull.Value))
                    return false;   // cant be the same
                else if ((param2.Value == null || param2.Value == DBNull.Value)
                   && !(param1.Value == null || param1.Value == DBNull.Value))
                    return false;   // cant be the same

                return _dbMgr.DbProvider.CompareParamEquality(param1, param2);
            }
            else
            {
                // only if they are all still int (but different sizes) we can try to compare them by converting up.
                // NOTE: this was a condition where the SqlServer version is defined as tinyint in the db which is
                // a byte in .NET; but Oracle only has Number(3) so the types are different, but values are same
                if ((param1.DbType == DbType.Int16 || param1.DbType == DbType.Int32 || param1.DbType == DbType.Int64
                        || param1.DbType == DbType.Byte || param1.DbType == DbType.SByte)
                        &&
                        (param2.DbType == DbType.Int16 || param2.DbType == DbType.Int32 || param2.DbType == DbType.Int64
                        || param2.DbType == DbType.Byte || param2.DbType == DbType.SByte))
                {
                    return Convert.ToInt64(param1.Value) == Convert.ToInt64(param2.Value);
                }
                if ((param1.DbType == DbType.UInt16
                        || param1.DbType == DbType.UInt32
                        || param1.DbType == DbType.UInt64)
                     && (param2.DbType == DbType.UInt16
                        || param2.DbType == DbType.UInt32
                        || param2.DbType == DbType.UInt64))
                {
                    return Convert.ToUInt64(param1.Value) == Convert.ToUInt64(param2.Value);
                }
                return false;// cant be the same
            }
        }

        string ReplaceParam(string input
                    , string oldPattern
                    , string newPattern
                    , string delimiters
                    , out bool paramReplaced)
        {
            paramReplaced = false;
            //DateTime startTime = DateTime.Now;
            StringBuilder output = new StringBuilder(input.Length);
            oldPattern = oldPattern.ToLower();
            bool newWholeWord = true;
            int lenOldPattern = oldPattern.Length;
            for (int i = 0; i < input.Length; i++)
            {
                if (delimiters.IndexOf(input[i]) >= 0)
                {
                    newWholeWord = true;
                    output.Append(input[i]);
                }
                else
                {
                    if (newWholeWord
                        && input.Substring(i).ToLower().StartsWith(oldPattern))
                    // we have found the OldPattern in a whole word
                    // we can replace it
                    {
                        int lenRemainingString = input.Substring(i).Length;
                        if (lenRemainingString == lenOldPattern
                            || delimiters.IndexOf(input[i + lenOldPattern]) >= 0)
                        // copy the new pattern
                        {
                            output.Append(newPattern);
                            // now move the old string pointer
                            i += (lenOldPattern - 1);
                            // newWholeWord remains true
                            paramReplaced = true;
                            continue;
                        }
                    }
                    // we do not have a newWholWord matching our pattern
                    newWholeWord = false;
                    output.Append(input[i]);

                }
            }
            return output.ToString();
        }


        /// <summary>
        /// Adds the given Parameterized Dynamic SQL statement and Parameter Collection
        /// to the current DbCommand.
        /// </summary>
        /// <param name="dynamicSQL">A parameterized SQL statement.</param>
        /// <param name="dbParameters">A collection of DbParameters matching those referred to in the SQL.</param>
        public void AddDynamicSQL(string dynamicSQL, DbParameterCollection dbParameters)
        {
            if (dynamicSQL == null || dynamicSQL == string.Empty)
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                                "DynamicSQL, Cannot be null or empty"));

            UpdateDbCommandBlock(dynamicSQL, dbParameters);
        }

        /// <summary>
        /// Adds the given DbCommand and parameters to the current CommandBlock.
        /// </summary>
        /// <param name="dbCommand">Given DAAB DbCommand object.</param>
        public void AddDbCommand(DbCommand dbCommand)
        {
            if (dbCommand == null)
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                    "dbCommand", "cannot add a dbCommand that is null"));

            if (dbCommand.CommandType == System.Data.CommandType.StoredProcedure)
                AddStoredProcedure(dbCommand.CommandText, _dbMgr.DbProvider.CloneParameterCollection(dbCommand.Parameters));

            else if (dbCommand.Parameters.Count > 0
                        || dbCommand.CommandText != _dbMgr.BuildNoOpDbCommand().CommandText) // dont add the empty command
                AddDynamicSQL(dbCommand.CommandText, _dbMgr.DbProvider.CloneParameterCollection(dbCommand.Parameters));
        }

        /// <summary>
        /// Calls AddDynamicSQL with a back-end specific parameterized sql statement executing
        /// the given stored procedure and DbParameter collection.
        /// </summary>
        /// <param name="storedProcedure">The fully qualified name of the stored procedure.</param>
        /// <param name="dbParameters">A collection of DbParameters matching those referred to in the SQL.</param>
        public void AddStoredProcedure(string storedProcedure
            , DbParameterCollection dbParameters)
        {
            AddDynamicSQL(_dbMgr.DbProvider.GenerateStoredProcedureCall(storedProcedure
                    , dbParameters), dbParameters);
        }

        /// <summary>
        /// Returns the proper parameter name based upon back end db type.
        /// For commands that Set a Value only where its current value is a specific value
        /// e.g. Set x = 1 where x = 2
        /// We have to name 1 of the parameters differently, we have chosen the SetParam (NewValue)
        /// If IsNewValueParam is true, we will use a special suffix
        /// NOTE: For SQLServer this is the same as BindVariable, but not so for oracle.
        /// </summary>
        /// <param name="variableName"></param>
        /// <param name="isNewValueParam"></param>
        /// <returns></returns>
        public string BuildParamName(string variableName, bool isNewValueParam)
        {
            return _dbMgr.DbProvider.BuildParameterName(variableName, isNewValueParam);
        }

        /// <summary>
        /// Returns the proper parameter name based upon back end db type.
        /// NOTE: For SQLServer this is the same as BindVariable, but not so for oracle.
        /// </summary>
        /// <param name="variableName"></param>
        /// <returns></returns>
        public string BuildParamName(string variableName)
        {
            return BuildParamName(variableName, false);
        }

        /// <summary>
        /// Returns a proper bind variable name based upon back end db type.
        /// NOTE: For SQLServer this is the same as ParamName, but not so for oracle.
        /// </summary>
        /// <param name="variableName"></param>
        /// <returns></returns>
        public string BuildBindVariableName(string variableName)
        {
            return _dbMgr.DbProvider.BuildBindVariableName(variableName);
        }

    }
}
