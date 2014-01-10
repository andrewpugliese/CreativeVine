using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Xml;
using System.Dynamic;


using CV.Database;
using CV.Database.Provider;
using CV.Configuration;
using CV.Global;
using CV.Cache;

namespace CV.Database
{
    public class DatabaseMgr
    {
        DatabaseProviderBase _dbProvider = null;

        private TimeSpan _timeSpanFromDb = new TimeSpan(0, 0, 0);
        // manages the database catalog metadata
        private DbCatalogMgr _dbCatalogMgr = null;

        public DatabaseMgr(string connectionKey)
        {
            _dbProvider = LoadDatabaseProvider(connectionKey);
            _dbCatalogMgr = new DbCatalogMgr(this);
        }

        /// <summary>
        /// Returns the current time from the current machine adjusted with the offset of
        /// the db server time taken at construction.  Does not use a call to the db; but is
        /// not as accurate.
        /// </summary>
        public DateTime DbSynchTime
        {
            get { return DateTime.UtcNow.AddMilliseconds(_timeSpanFromDb.TotalMilliseconds); }
        }

        /// <summary>
        /// Returns the difference in time measured as a TimeSpan between the database
        /// and the server that the application resides.  Time is calculated as universal time.
        /// </summary>
        public TimeSpan DbSynchTimeOffSet
        {
            get { return _timeSpanFromDb; }
        }

        /// <summary>
        /// Returns an enum indicating whether the Database (e.g. Oracle, SqlServer)
        /// </summary>
        public DatabaseTypeName DatabaseType
        {
            get { return _dbProvider.TypeName; }
        }

         /// <summary>
        /// Returns an enum indicating who the provider is for the database (e.g. Microsoft, Oracle)
        /// </summary>
        public DatabaseProviderName ProviderName
        {
            get { return _dbProvider.ProviderName; }
        }

        /// <summary>
        /// Returns the provider object for the database (e.g. Microsoft, Oracle)
        /// </summary>
        internal DatabaseProviderBase DbProvider
        {
            get { return _dbProvider; }
        }

        /// <summary>
        /// Returns the backend specific function for current datetime
        /// as a string e.g. sysdate or getdate() to be used in a seperate command
        /// if ReturnAsAlias is not null, it will be the alias
        /// </summary>
        /// <param name="dbDateType">The format type of the date function</param>
        /// <param name="returnAsAlias">What the return column will be called</param>
        /// <returns>Backend specific function for current date time</returns>
        public string GetDbTimeAs(DateTimeKind dbDateType, string returnAsAlias)
        {
            return _dbProvider.GetDbTimeAs(dbDateType, returnAsAlias) + (string.IsNullOrEmpty(returnAsAlias)
                            ? "" : " as " + returnAsAlias);
        }

        /// <summary>
        /// Returns the DateTime from the database.
        /// Note: This operation will make a database call.
        /// </summary>
        /// <param name="dbDateType">Enumeration value indicating whether time is local or UTC;
        /// default is UTC.</param>
        /// <returns>The database time</returns>
        public DateTime GetServerTime(DateTimeKind dbDateType)
        {
            string sql = _dbProvider.GetServerTimeCommandText(dbDateType, "Now");
            DataTable dt = this.ExecuteDataSet(BuildSelectDbCommand(sql, null), null, null).Tables[0];
            return Convert.ToDateTime(dt.Rows[0]["Now"]);
        }

        DatabaseProviderBase LoadDatabaseProvider(string connectionKey)
        {
            // opens the ObjectFactories configuration section
            ObjectFactoryConfiguration dbProviderFactory
                    = ConfigurationMgr.GetSection<ObjectFactoryConfiguration>
                    (ObjectFactoryConfiguration.ConfigSectionName);

            ObjectFactoryElement ofe = dbProviderFactory.GetFactoryObject(connectionKey, true);
            string providerDll = string.Format("{0}\\{1}.dll"
            , ofe.AssemblyPath
            , ofe.AssemblyName);
            return ObjectFactory.Create<DatabaseProviderBase>(providerDll
                    , ofe.ObjectClass
                    , ConfigurationMgr.GetConnectionString(connectionKey));
        }

        /// <summary>
        /// If an object is DBNull will return default, otherwise returns the object casted to type T.
        /// </summary>
        /// <param name="dbValue"></param>
        /// <param name="defaultVal"></param>
        /// <returns></returns>
        public T GetValueOrDefault<T>(object dbValue, T defaultVal)
        {
            return GetValueOrDefault_<T>(dbValue, defaultVal);
        }


        /// <summary>
        /// Returns the back-end compliant variable name (with BindVariablePrefix)
        /// </summary>
        /// <param name="variableName">Variable name (with or without prefix)</param>
        /// <returns>Variable name with prefix)</returns>
        public virtual string BuildBindVariableName(string variableName)
        {
            return _dbProvider.BuildBindVariableName(variableName);
        }


        /// <summary>
        /// Returns a back-end compliant sql syntax for com
        /// Returns the back-end compliant parameter name (with ParameterPrefix)
        /// </summary>
        /// <param name="paramName">Parameter Name (with or without prefix)</param>
        /// <returns>Parameter name with prefix</returns>
        public string BuildParameterName(string paramName)
        {
            return _dbProvider.BuildParameterName(paramName);
        }

        /// <summary>
        /// Returns the proper parameter name based upon back end db type.
        /// For commands that Set a Value only where its current value is a specific value
        /// e.g. Set x = 1 where x = 2
        /// We have to name 1 of the parameters differently, we have chosen the SetParam (NewValue)
        /// If IsNewValueParam is true, we will use a special suffix
        /// NOTE: For SQLServer this is the same as BindVariable, but not so for oracle.
        /// </summary>
        /// <param name="variableName">ColumnName or VariableName to become
        /// a parameter (WITHOUT ANY BACKEND SPECIFIC SYMBOL; e.g. @)</param>
        /// <param name="isNewValueParam">Indicates whether is part of a Set clause and a Where clause</param>
        /// <returns>string representation of the back-end specific parameter</returns>
        public string BuildParameterName(string variableName, bool isNewValueParam)
        {
            return _dbProvider.BuildParameterName(variableName, isNewValueParam);
        }

        /// <summary>
        /// If an object is DBNull will return default, otherwise returns the object casted to type T.
        /// </summary>
        /// <param name="dbValue"></param>
        /// <param name="defaultVal"></param>
        /// <returns></returns>
        public static T GetValueOrDefault_<T>(object dbValue, T defaultVal)
        {
            return dbValue != DBNull.Value && dbValue is T ? (T)dbValue : defaultVal;
        }
        #region DbParameters

        /// <summary>
        /// Function will create a new parameter with the given properties
        /// </summary>
        /// <param name="paramName">Name of the new parameter.</param>
        /// <param name="paramType">DbType of the new parameter.</param>
        /// <param name="nativeDbType">NativeDbType of the new parameter at backend database e.g. varchar instead of string.</param>
        /// <param name="maxLength">Maximum length of the new parameter (for strings).</param>
        /// <param name="paramDirection">Direction of the new parameter.</param>
        /// <param name="paramValue">Value of the new parameter.</param>
        /// <returns>Returns the newly created parameter.</returns>
        public DbParameter CreateParameter(string paramName
            , DbType paramType
            , string nativeDbType
            , Int32 maxLength
            , ParameterDirection paramDirection
            , object paramValue)
        {
            return _dbProvider.CreateNewParameter(paramName
                    , paramType
                    , nativeDbType
                    , maxLength
                    , paramDirection
                    , paramValue);
        }

        /// <summary>
        /// Returns a clone of the given parameter
        /// </summary>
        /// <param name="dbParam">The DbParameter to clone</param>
        /// <returns>A copy of the DbParameter</returns>
        public DbParameter CloneParameter(DbParameter dbParam)
        {
            return _dbProvider.CloneParameter(dbParam);
        }

        /// <summary>
        /// Returns a clone of the given DbParameter collection.
        /// </summary>
        /// <param name="dbParameters">The collection to clone</param>
        /// <returns>A copy of the DbParameter collection</returns>
        public DbParameterCollection CloneParameterCollection(DbParameterCollection dbParameters)
        {
            return _dbProvider.CloneParameterCollection(dbParameters);
        }

        /// <summary>
        /// Copies the given DbParameterCollection to the target collection.
        /// </summary>
        /// <param name="dbSourceParameters">A DbParameter collection to add the parameter clone to</param>
        /// <param name="dbDestinationParameters">A DbParameter collection to add the parameter clone to</param>
        public void CopyParameters(DbParameterCollection dbSourceParameters
                , DbParameterCollection dbDestinationParameters)
        {
            _dbProvider.CopyParameters(dbSourceParameters, dbDestinationParameters);
        }

        /// <summary>
        /// Returns a copy of the given DbParameter that was added to the given collection.
        /// </summary>
        /// <param name="dbParameters">A DbParameter collection to add the parameter clone to</param>
        /// <param name="dbParam">A DbParameter to clone</param>
        /// <returns>The DbParameter clone</returns>
        public DbParameter CopyParameterToCollection(DbParameterCollection dbParameters
                , DbParameter dbParam)
        {
            return _dbProvider.CopyParameterToCollection(dbParameters, dbParam);
        }
        
 
        #endregion

        #region DbCommand Methods

        /// <summary>
        /// Function takes any select statement and will turn it into a select statement
        /// that will return only the number of rows defined by parameter BufferSize.
        /// <para>
        /// If BufferSize is a string, then it will be assumed be a bind variable.
        /// If it is an Int, then the constant will be used.
        /// </para>
        /// <para>
        /// NOTE: If for some executions you want a full result set without rewriting query
        ///         set BufferSize Param Value = 0;
        ///         Value CANNOT BE SET TO NULL
        ///</para>
        /// <para>
        /// DB2 USERS: In order to implement a dynamic buffer size, the row_number() function was applied
        /// however, this column would then be returned in the result set as (row_num);  
        /// </para>
        /// <para>
        /// This function
        /// will attempt to remove it from the statement.  In order to do this, we require a unique set
        /// a column names so if there are joins with the same column then they must be uniquely aliased.
        /// </para>
        /// </summary>
        /// <param name="selectStatement">A valid SQL select statement with UNIQUE column names</param>
        /// <param name="bufferSize">Limits the number of rows returned.  If the param is a constant number
        /// , then it will be a fixed number of records returned each time.  
        /// <para>If the param is a string
        /// , then a parameter will be created with the name equal to the string provided.  This
        /// can be used to change the buffer size for each execution of the dbCommand.
        /// </para>
        /// <para>Null indicates all rows are returned.
        /// </para>
        /// </param>
        /// <returns>Select statement with max rows</returns>
        public string FormatSQLSelectWithMaxRows(string selectStatement, object bufferSize)
        {
            return _dbProvider.FormatSQLSelectWithMaxRows(selectStatement, bufferSize);
        }

        /// <summary>
        /// Method to indicate whether the given DbCommand object is a No Operation DbCommand
        /// </summary>
        /// <param name="dbCmd">DAAB DbCommand object</param>
        /// <returns>true or false</returns>
        public bool IsNoOpDbCommand(DbCommand dbCmd)
        {
            return dbCmd.CommandText == _dbProvider.NoOpDbCommandText;
        }

        /// <summary>
        /// Returns a dbCommand with the equivalent of a No Operation.
        /// </summary>
        /// <returns>DAAB DbCommand Object with NO DbParameters which corresponds to a NOOP.
        /// Adding a NoOpDbCommand to a CommandBlock has no effect.  It is useful for
        /// having recursive calls to add to a CommandBlock.
        /// The CommandType is Text.</returns>
        public DbCommand BuildNoOpDbCommand()
        {
            return _dbProvider.BuildNoOpDbCommand();
        }

        public DbCommand CloneDbCommand(DbCommand dbCmd)
        {
            return _dbProvider.CloneDbCommand(dbCmd);
        }

        /// <summary>
        /// Returns a dbCommand for the execution of the given stored procedure name
        /// </summary>
        /// <param name="StoredProcedureName">Name of stored procedure</param>
        /// <returns></returns>
        public DbCommand BuildStoredProcedureDbCommand(string StoredProcedureName)
        {
            return _dbProvider.BuildStoredProcedureDbCommand(StoredProcedureName);
        }

        /// <summary>
        /// Builds a non query (insert/update/delete) DbCommand object that is compliant with the back-end database
        /// for the given SQL compliant Select Statement and parameter collection.
        /// </summary>
        /// <param name="sqlStatement">An Ansi standard compliant SQL select statement with optional bind variable parameters
        /// to match the </param>
        /// <param name="dbParams">A DbParameter Collection</param>
        /// <returns>DAAB DbCommand Object with DbParameters (initialized to the values provided
        /// or DbNull.  The CommandType is Text.</returns>
        public DbCommand BuildNonQueryDbCommand(string sqlStatement
                                    , DbParameterCollection dbParams)
        {
            return _dbProvider.BuildNonQueryDbCommand(sqlStatement, dbParams);
        }


        /// <summary>
        /// Builds a Select DbCommand object that is compliant with the back-end database
        /// for the given SQL compliant Select Statement and parameter collection.
        /// </summary>
        /// <param name="selectStatement">An Ansi standard compliant SQL select statement with optional bind variable parameters
        /// to match the </param>
        /// <param name="dbParams">A DbParameter Collection</param>
        /// <returns>DAAB DbCommand Object with DbParameters (initialized to the values provided
        /// or DbNull.  The CommandType is Text.</returns>
        public DbCommand BuildSelectDbCommand(string selectStatement
                                    , DbParameterCollection dbParams)
        {
            return _dbProvider.BuildSelectDbCommand(selectStatement, dbParams);
        }

        /// <summary>
        /// Builds a select DbCommand and parameter collection
        /// which can be executed against the multiple back-end
        /// supported databases.  
        /// <para>The number of rows returned can be limited to the 
        /// given buffersize parameter. Null indicates all rows.
        /// </para>
        /// </summary>
        /// <param name="dmlSelect">MetaData Structure describing the select columns and conditions</param>
        /// <param name="bufferSize">Limits the number of rows returned.  If the param is a constant number
        /// , then it will be a fixed number of records returned each time.  If the param is a string
        /// , then a parameter will be created with the name equal to the string provided.  This
        /// can be used to change the buffer size for each execution of the dbCommand.  Null indicates
        /// all rows are returned.</param>
        /// <returns>DAAB DbCommand Object with DbParameters (initialized to the values provided
        /// or DbNull.  The CommandType is Text.</returns>
        public DbCommand BuildSelectDbCommand(DmlMgr dmlSelect
                                            , object bufferSize)
        {
            Tuple<string, DbParameterCollection> result = dmlSelect.BuildSelect(dmlSelect, bufferSize, null);
            return BuildSelectDbCommand(result.Item1, result.Item2);
        }

        public DbCommand BuildInsertDbCommand(DmlMgr dmlInsert)
        {
            if (dmlInsert.ColumnsForUpdateOrInsert == null || dmlInsert.ColumnsForUpdateOrInsert.Count == 0)
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                            "Cant build insert dbcommand with no columns"));

            // start with the statement
            StringBuilder sqlInsertVars = new StringBuilder();
            StringBuilder sqlInsertVals = new StringBuilder();
            DbParameterCollection dbParams = BuildNoOpDbCommand().Parameters;
            bool firstColumn = true;
            foreach (KeyValuePair<DbQualifiedObject<string>, object> columnUpdate in dmlInsert.ColumnsForUpdateOrInsert)
            {
                DbQualifiedObject<string> qualifiedColumn = columnUpdate.Key;
                string columnName = qualifiedColumn.DbObject;
                string bindVarName = "";

                // dbDateFunctions do not require an insert syntax
                if (columnUpdate.Value is DateTimeKind)
                    continue;
                DbFunctionStructure? fn = null;
                if (columnUpdate.Value is DbFunctionStructure)
                {
                    fn = (DbFunctionStructure)columnUpdate.Value;
                    if (fn.Value.AutoGenerate) // if it is an identity, or timestamp, or a trigger, then skip
                        continue;
                }
                else if (columnUpdate.Value is string) // parameterName
                {
                    bindVarName = BuildBindVariableName((string)columnUpdate.Value);

                    DbColumn column = DbCatalogGetColumn(dmlInsert.MainTable.SchemaName
                                            , dmlInsert.MainTable.TableName
                                            , columnName);
                    // only add parameter if it does not exist in the whereCondition
                    // because it will be added in the where clause processing
                    if (!(dmlInsert._whereCondition != null && dmlInsert._whereCondition.Parameters.ContainsKey(
                            BuildParameterName(columnName))))
                        CopyParameterToCollection(dbParams
                            , CreateParameter(columnName
                                            , column.DataTypeGenericDb
                                            , column.DataTypeNativeDb
                                            , column.MaxLength
                                            , ParameterDirection.Input
                                            , DBNull.Value));
                }

                if (firstColumn)
                {
                    // if it is not autogenerated then we need to add it to the insert
                    if (!fn.HasValue || !fn.Value.AutoGenerate)
                    {
                        sqlInsertVars.AppendFormat("insert into {0}.{1} ({2}{3}"
                            , dmlInsert.MainTable.SchemaName
                            , dmlInsert.MainTable.TableName
                            , columnName
                            , Environment.NewLine);

                        // if it is not autogenerated then we need to add the function body insert values
                        sqlInsertVals.AppendFormat("values ({0}{1}"
                                , fn.HasValue && !fn.Value.AutoGenerate
                                    ? fn.Value.FunctionBody : bindVarName
                                , Environment.NewLine);

                        firstColumn = false;
                    }
                }
                else
                {
                    // if it is not autogenerated then we need to add it to the insert
                    if (!fn.HasValue || !fn.Value.AutoGenerate)
                    {
                        sqlInsertVars.AppendFormat(", {0}{1}", columnName, Environment.NewLine);
                        // if there is a functionbody, we need to add it otherwise add a bind variable
                        sqlInsertVals.AppendFormat(", {0}{1}"
                                    , fn.HasValue && !fn.Value.AutoGenerate
                                        ? fn.Value.FunctionBody : bindVarName
                                , Environment.NewLine);
                    }
                }
            }

            if (sqlInsertVars.Length > 0)
            {
                sqlInsertVals.AppendFormat(") {0}", Environment.NewLine);
                sqlInsertVars.AppendFormat(") {0}", Environment.NewLine);
            }

            string sqlInsert = sqlInsertVars.ToString()
                        + Environment.NewLine + sqlInsertVals.ToString() + Environment.NewLine;

            // return the new dbCommand
            return BuildNonQueryDbCommand(sqlInsert, dbParams);
        }

        public DbCommand BuildUpdateDbCommand(DmlMgr dmlUpdate)
        {
            if (dmlUpdate.ColumnsForUpdateOrInsert == null || dmlUpdate.ColumnsForUpdateOrInsert.Count == 0)
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                            "Cant build update dbcommand with no columns"));

            string updateTable = null;

            DbParameterCollection dbParams = null;

            if (DatabaseType != DatabaseTypeName.SqlServer)
            {
                Tuple<string, DbParameterCollection> selectResult = dmlUpdate.BuildSelect(dmlUpdate, null, null);

                updateTable = string.Format("{0}({1}) {2}"
                        , Environment.NewLine
                            , selectResult.Item1
                            , _dbProvider.DefaultTableAlias);

                dbParams = selectResult.Item2;
            }
            else
            {
                updateTable = dmlUpdate.MainTable.TableAlias;
                dbParams = BuildNoOpDbCommand().Parameters;
            }

            string updateSet = string.Format("update {0} set ", updateTable);


            StringBuilder updateClause = new StringBuilder();
            foreach (KeyValuePair<DbQualifiedObject<string>, object> columnUpdate in dmlUpdate.ColumnsForUpdateOrInsert)
            {
                DbQualifiedObject<string> qualifiedColumn = columnUpdate.Key;
                string columnName = qualifiedColumn.DbObject;
                string columnValue = "";

                if (columnUpdate.Value is DbFunctionStructure)
                {
                    columnValue = ((DbFunctionStructure)columnUpdate.Value).FunctionBody;
                }
                else if (columnUpdate.Value is DbParameter)
                {
                    columnValue = BuildBindVariableName(((DbParameter)columnUpdate.Value).ParameterName);
                }
                else if (columnUpdate.Value is DateTimeKind)
                {
                    columnValue = GetDbTimeAs((DateTimeKind)columnUpdate.Value, null);
                }
                else if (columnUpdate.Value is DbConstValue)
                {
                    columnValue = ((DbConstValue)columnUpdate.Value).GetQuotedValue();
                }
                else if (columnUpdate.Value is string)
                {
                    columnValue = BuildBindVariableName((string)columnUpdate.Value);

                    DbColumn column = DbCatalogGetColumn(qualifiedColumn.SchemaName
                                                    , qualifiedColumn.TableName
                                                    , columnName);

                    // only add parameter if it does not exist in the whereCondition
                    // because it will be added in the where clause processing
                    if (!(dmlUpdate._whereCondition != null && dmlUpdate._whereCondition.Parameters.ContainsKey(
                            _dbProvider.BuildParameterName(columnName))))
                        CopyParameterToCollection(dbParams
                            , CreateParameter((string)columnUpdate.Value
                                        , column.DataTypeGenericDb
                                        , column.DataTypeNativeDb
                                        , column.MaxLength
                                        , ParameterDirection.Input
                                        , DBNull.Value));
                }

                string tableAlias = null;

                if (DatabaseType != DatabaseTypeName.SqlServer)
                    tableAlias = _dbProvider.DefaultTableAlias;
                else
                    tableAlias = dmlUpdate.GetTable(qualifiedColumn.SchemaName, qualifiedColumn.TableName).TableAlias;

                // add to update clause
                updateClause.AppendFormat("{0}{1}.{2} = {3}"
                        , updateClause.Length > 0 ? string.Format("{0}, ", Environment.NewLine) : updateSet
                        , tableAlias
                        , columnName
                        , columnValue);
            }

            if (DatabaseType == DatabaseTypeName.SqlServer)
            {
                updateClause.AppendFormat("{0} from {1} "
                            , Environment.NewLine
                            , dmlUpdate.BuildJoinClause(dmlUpdate, null).Item1);

                StringBuilder whereClause = new StringBuilder();
                if (dmlUpdate._whereCondition != null)
                    whereClause.AppendFormat("{0}where {1}", Environment.NewLine, dmlUpdate._whereCondition.ToString(this));

                if (whereClause.Length > 0)
                {
                    dbParams = dmlUpdate.BuildWhereClauseParams(dmlUpdate._whereCondition.Parameters.Values, dbParams);
                    updateClause.Append(whereClause);
                }
            }

            updateClause.Append(Environment.NewLine);

            return BuildNonQueryDbCommand(updateClause.ToString(), dbParams);
        }

        public DbCommand BuildChangeDbCommand(DmlMgr dmlChange, params DbQualifiedObject<string>[] lastModColumns)
        {
            if (lastModColumns.Length == 0)
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                    "BuildChangeDbCommand Error: lastModFieldValues must contain at least one field/value pair"));

            foreach (var lastModCol in lastModColumns)
            {
                if (!dmlChange.ColumnsForUpdateOrInsert.ContainsKey(lastModCol))
                    dmlChange.AddColumn(lastModCol.DbObject, BuildParameterName(lastModCol.DbObject, true));
                else if (dmlChange.ColumnsForUpdateOrInsert[lastModCol].ToString()
                        != BuildParameterName(lastModCol.DbObject, true))
                    dmlChange.ColumnsForUpdateOrInsert[lastModCol] = BuildParameterName(lastModCol.DbObject, true);

            }

            Expression lastModKeyExpression = null;
            foreach (var lastModCol in lastModColumns)
            {
                var lastModColFinal = lastModCol;

                Expression partialEqualExpression = DbPredicate.CreatePredicatePart(t => t.Column(lastModColFinal.SchemaName,
                        lastModColFinal.TableName, lastModColFinal.DbObject) == t.Parameter(lastModColFinal.TableName,
                        lastModColFinal.DbObject, BuildParameterName(lastModColFinal.DbObject)));

                Expression partialNullExpression = DbPredicate.CreatePredicatePart(t => t.Parameter(lastModColFinal.TableName,
                        lastModColFinal.DbObject, BuildParameterName(lastModColFinal.DbObject)) == null && t.Column(lastModColFinal.SchemaName,
                        lastModColFinal.TableName, lastModColFinal.DbObject) == null);

                Expression partialExpression = Expression.OrElse(partialEqualExpression, partialNullExpression);

                if (lastModKeyExpression == null)
                {
                    lastModKeyExpression = partialExpression;
                }
                else
                {
                    lastModKeyExpression = Expression.AndAlso(lastModKeyExpression, partialExpression);
                }

            }

            if (dmlChange._whereCondition == null)
                dmlChange.SetWhereCondition(lastModKeyExpression);
            else
                dmlChange.AddToWhereCondition(ExpressionType.AndAlso, lastModKeyExpression);

            return BuildUpdateDbCommand(dmlChange);
        }

        /// <param name="dmlChange">DmlMgr representing table to be updated. Can have where condition.</param>
        /// <param name="lastModColumns">NON qualified column names that MUST belong to the MainTable.</param>
        /// <returns>DbCommand</returns>
        public DbCommand BuildChangeDbCommand(DmlMgr dmlChange, params string[] lastModColumns)
        {
            DbQualifiedObject<string>[] qualifiedColumns = new DbQualifiedObject<string>[lastModColumns.Length];
            for (int i = 0; i < lastModColumns.Length; i++)
                qualifiedColumns[i] = new DbQualifiedObject<string>(
                        dmlChange.MainTable.SchemaName, dmlChange.MainTable.TableName, lastModColumns[i]);
            return BuildChangeDbCommand(dmlChange, qualifiedColumns);
        }

        public DbCommand BuildMergeDbCommand(DmlMgr dmlMerge)
        {
            if ((dmlMerge.ColumnsForUpdateOrInsert == null || dmlMerge.ColumnsForUpdateOrInsert.Count == 0)
                && ((dmlMerge.ColumnsForInsert == null || dmlMerge.ColumnsForInsert.Count == 0)
                    && ((dmlMerge.ColumnsForUpdate == null || dmlMerge.ColumnsForUpdate.Count == 0))))
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException("Cant build Merge dbcommand with no columns"));

            string mergeTable = null;
            string tableAlias = null;
            StringBuilder sqlMerge = new StringBuilder();
            StringBuilder whereClause = new StringBuilder();
            DbParameterCollection dbParams = null;

            mergeTable = string.Format("{0}.{1}",
                    dmlMerge.MainTable.SchemaName,
                    dmlMerge.MainTable.TableName);

            tableAlias = dmlMerge.GetTable(dmlMerge.MainTable.SchemaName, dmlMerge.MainTable.TableName).TableAlias;
            dbParams = BuildNoOpDbCommand().Parameters;

            if (dmlMerge._whereCondition != null)
                whereClause.Append(dmlMerge._whereCondition.ToString(this));

            if (whereClause.Length > 0)
            {
                dbParams = dmlMerge.BuildWhereClauseParams(dmlMerge._whereCondition.Parameters.Values, dbParams);
            }

            int columnNum = 0;
            bool bColumnPresentInWhereClause;
            string columnName;
            string columnValue;
            StringBuilder updateClause = new StringBuilder();
            StringBuilder insertColumns = new StringBuilder();
            StringBuilder insertValues = new StringBuilder();
            DbQualifiedObject<string> qualifiedColumn;
            Dictionary<DbQualifiedObject<string>, object> columnsCollection = null;

            // for each of the column types, determine whether they require parameters
            foreach (MergeColumnOptions columnType in Enum.GetValues(typeof(MergeColumnOptions)))
            {
                if (columnType == MergeColumnOptions.None)
                    continue;

                if (columnType == MergeColumnOptions.ForInsertOnly)
                    columnsCollection = dmlMerge.ColumnsForInsert;
                else if (columnType == MergeColumnOptions.ForUpdateOnly)
                    columnsCollection = dmlMerge.ColumnsForUpdate;
                else
                    columnsCollection = dmlMerge.ColumnsForUpdateOrInsert;

                foreach (KeyValuePair<DbQualifiedObject<string>, object> columnMerge in columnsCollection)
                {
                    qualifiedColumn = columnMerge.Key;
                    columnName = qualifiedColumn.DbObject;
                    columnValue = "";

                    columnNum++;
                    bColumnPresentInWhereClause = false;

                    if (columnMerge.Value is DbFunctionStructure)
                    {
                        columnValue = ((DbFunctionStructure)columnMerge.Value).FunctionBody;
                    }
                    else if (columnMerge.Value is DbParameter)
                    {
                        columnValue = BuildBindVariableName(((DbParameter)columnMerge.Value).ParameterName);
                    }
                    else if (columnMerge.Value is DateTimeKind)
                    {
                        columnValue = GetDbTimeAs((DateTimeKind)columnMerge.Value, null);
                    }
                    else if (columnMerge.Value is DbConstValue)
                    {
                        columnValue = ((DbConstValue)columnMerge.Value).GetQuotedValue();
                    }
                    else if (columnMerge.Value is string)
                    {
                        columnValue = BuildBindVariableName((string)columnMerge.Value);

                        DbColumn column = DbCatalogGetColumn(qualifiedColumn.SchemaName
                                                        , qualifiedColumn.TableName
                                                        , columnName);

                        if (dmlMerge._whereCondition != null && dmlMerge._whereCondition.Parameters.ContainsKey(
                                BuildParameterName(columnName)))
                        {
                            bColumnPresentInWhereClause = true;
                        }
                        else
                        {
                            CopyParameterToCollection(dbParams
                                , CreateParameter((string)columnMerge.Value
                                                , column.DataTypeGenericDb
                                                , column.DataTypeNativeDb
                                                , column.MaxLength
                                                , ParameterDirection.Input
                                                , DBNull.Value));
                        }
                    }

                    if (columnType != MergeColumnOptions.ForUpdateOnly)
                    {
                        insertColumns.AppendFormat("{0}{1}"
                                , columnNum > 1 ? string.Format("{0}, ", Environment.NewLine) : ""
                                , columnName);

                        insertValues.AppendFormat("{0}{1}"
                            , columnNum > 1 ? string.Format("{0}, ", Environment.NewLine) : ""
                            , columnValue);
                    }

                    if (columnType != MergeColumnOptions.ForInsertOnly)
                    {
                        if (!bColumnPresentInWhereClause)
                        {
                            updateClause.AppendFormat("{0}{1} = {2}"
                                    , updateClause.Length > 0 ? string.Format("{0}, ", Environment.NewLine) : "UPDATE SET "
                                    , columnName, columnValue);
                        }
                    }
                }
            }

            // build sql body
            if (DatabaseType == DatabaseTypeName.Oracle)
            {
                sqlMerge.AppendFormat("MERGE INTO {0} {1}{2}", mergeTable, tableAlias, Environment.NewLine);
                sqlMerge.AppendFormat("USING (SELECT 1 DummyCol FROM dual) Source{0}", Environment.NewLine);
            }
            else
            {
                sqlMerge.AppendFormat("MERGE INTO {0} AS {1}{2}", mergeTable, tableAlias, Environment.NewLine);
                sqlMerge.AppendFormat("USING (VALUES ( 1 )) AS Source ( DummyCol ){0}", Environment.NewLine);
            }

            // append where clause
            sqlMerge.AppendFormat("ON ({0}){1}", whereClause.ToString(), Environment.NewLine);

            // add update portion
            if (updateClause.Length > 0)
            {
                sqlMerge.AppendFormat("WHEN MATCHED THEN{0}{1}{2}", Environment.NewLine, updateClause, Environment.NewLine);
            }

            // append insert portion
            if (insertColumns.Length > 0)
            {
                sqlMerge.AppendFormat("WHEN NOT MATCHED THEN{0} INSERT ( {1} ){2}VALUES ( {3} )",
                        Environment.NewLine, insertColumns, Environment.NewLine, insertValues);
            }

            sqlMerge.AppendFormat("{0};{1}", Environment.NewLine, Environment.NewLine);

            // return the new dbCommand
            return BuildNonQueryDbCommand(sqlMerge.ToString(), dbParams);
        }

        public DbCommand BuildDropTableDbCommand(string schema, string table)
        {
            return _dbProvider.BuildDropTableDbCommand(schema, table);
        }

        public DbCommand BuildTruncateTableDbCommand(string schema, string table)
        {
            return _dbProvider.BuildTruncateTableDbCommand(schema, table);
        }

        public DbCommand BuildSelectEmptyTableDbCommand(string schema, string table)
        {
            string sql = string.Format("select * from {0}.{1} where 1 = 2", schema, table);
            return _dbProvider.BuildSelectDbCommand(sql, null);
        }

        List<DbIndexMetaData> GetIndexMetaData(string targetSchema
            , string targetTable
            , CacheMgr<string, DbIndex> indexes)
        {
            List<DbIndexMetaData> indexMetaData = null;
            if (indexes != null)
            {
                Dictionary<string, DbIndex> tableIndexes = null;
                if (TableExists(targetSchema, targetTable))
                    tableIndexes = DbCatalogGetTable(targetSchema, targetTable).Indexes;
                foreach (string indexName in indexes.Keys)
                {
                    DbIndex index = indexes.Get(indexName);
                    DbIndexMetaData indexCopy = new DbIndexMetaData();
                    indexCopy.SchemaName = targetSchema.ToUpper();
                    indexCopy.TableName = targetTable.ToUpper();
                    indexCopy.IndexName = index.IndexName.ToUpper().Replace(index.TableName.ToUpper(), targetTable.ToUpper());
                    if (tableIndexes != null && tableIndexes.ContainsKey(indexCopy.IndexName))
                            continue;
                    indexCopy.IsClustered = index.IsClustered;
                    indexCopy.IsPrimaryKey = index.IsPrimaryKey;
                    indexCopy.IsUnique = index.IsUnique;
                    indexCopy.ColumnOrder = new SortedDictionary<short, DbIndexColumnMetaData>();
                    foreach (short order in index.ColumnOrder.Keys)
                    {
                        DbIndexColumnMetaData columnCopy = new DbIndexColumnMetaData();
                        columnCopy.ColumnFunction = index.ColumnOrder[order].ColumnFunction;
                        columnCopy.ColumnName = index.ColumnOrder[order].ColumnName;
                        columnCopy.IsDescending = index.ColumnOrder[order].IsDescending;
                        indexCopy.ColumnOrder.Add(order, columnCopy);
                    }
                    indexCopy.IncludeColumns = new List<string>();
                    foreach (string column in index.IncludeColumns)
                        indexCopy.IncludeColumns.Add(column);
                    if (indexMetaData == null)
                        indexMetaData = new List<DbIndexMetaData>();
                    indexMetaData.Add(indexCopy);
                }
            }
            return indexMetaData;
        }

        public DbCommand BuildCreateTableDbCommand(string sourceSchema, string sourceTable
            , string targetSchema, string targetTable, CacheMgr<string, DbIndex> indexes = null)
        {
            List<DbIndexMetaData> indexMetaData = GetIndexMetaData(targetSchema, targetTable, indexes);
            return _dbProvider.BuildCreateTableDbCommand(sourceSchema, sourceTable
                , targetSchema, targetTable, indexMetaData);
        }

        public DbCommand BuildAddIndexesToTableDbCommand(string targetSchema, string targetTable
            , CacheMgr<string, DbIndex> indexes = null)
        {
            List<DbIndexMetaData> indexMetaData = GetIndexMetaData(targetSchema, targetTable, indexes);
            return _dbProvider.BuildAddIndexesToTableDbCommand(targetSchema, targetTable, indexMetaData);
        }

        /// <summary>
        /// This command is not yet completed;
        /// </summary>
        /// <param name="table">DataTable to derive the meta data for creating table columns</param>
        /// <returns></returns>
        public DbCommand BuildCreateTableDbCommand(DataTable table)
        {
            return _dbProvider.BuildCreateTableDbCommand(table);
        }

 
        #endregion

        #region Database Catalog Methods (Data Dictionary)

        /// <summary>
        /// Returns the DbColumn (database catalog data) for the given 
        /// database table name, Schema and, Table.
        /// </summary>
        /// <param name="schemaName">Schema that table belongs to</param>
        /// <param name="tableName">Table that column belongs to</param>
        /// <param name="columnName">Column to lookup in catalog</param>
        /// <returns>Database Catalog Meta Data for a column of table Structure</returns>
        public DbColumn DbCatalogGetColumn(string schemaName, string tableName, string columnName)
        {
            return _dbCatalogMgr.GetDbColumn(schemaName, tableName, columnName);
        }


        /// <summary>
        /// Returns the DbTable (database catalog data) for the given 
        /// database table name.
        /// </summary>
        /// <param name="schemaName">Schema that table belongs to</param>
        /// <param name="tableName">Table to lookup in catalog</param>
        /// <returns>Database Catalog Meta Data for a table Structure</returns>
        public DbTable DbCatalogGetTable(string schemaName, string tableName)
        {
            return _dbCatalogMgr.GetDbTable(schemaName, tableName);
        }

        /// <summary>
        /// Returns the DbTable (database catalog data) for the given 
        /// fully qualified database table name.
        /// </summary>
        /// <param name="fullyQualifiedTableName">SchemaName.TableName</param>
        /// <returns>Database Catalog Meta Data for a table Structure</returns>
        public DbTable DbCatalogGetTable(string fullyQualifiedTableName)
        {
            return _dbCatalogMgr.GetDbTable(fullyQualifiedTableName);
        }

        /// <summary>
        /// Returns a DmlMgr class used for defining Dynamic sql. The meta data of the table that is passed in 
        /// is included in the instance.
        /// </summary>
        /// <param name="schemaName">Schema that table belongs to</param>
        /// <param name="tableName">Table to lookup in catalog</param>
        /// <param name="selectColumns">Columns to include in a select, if this will be used for a select.
        /// If non are included, all will be returned for a select.</param>
        /// <returns>Meta Data structure (with empty collection structures) to be used for building dynamic sql></returns>
        public DmlMgr DbCatalogGetDmlMgr(string schemaName, string tableName, params object[] selectColumns)
        {
            DbTable tableStructure = _dbCatalogMgr.GetDbTable(schemaName, tableName);

            return new DmlMgr(this, tableStructure, selectColumns);
        }

        /// <summary>
        /// Returns a DmlMgr class used for defining Dynamic sql. The meta data of the table that is passed in 
        /// is included in the instance.
        /// </summary>
        /// <param name="fullyQualifiedTableName">SchemaName.TableName</param>
        /// <param name="selectColumns">Columns to include in a select, if this will be used for a select.
        /// If non are included, all will be returned for a select.</param>
        /// <returns>Meta Data structure (with empty collection structures) to be used for building dynamic sql></returns>
        public DmlMgr DbCatalogGetDmlMgr(string fullyQualifiedTableName, params object[] selectColumns)
        {
            DbTable tableStructure = _dbCatalogMgr.GetDbTable(fullyQualifiedTableName);

            return new DmlMgr(this, tableStructure, selectColumns);
        }


        /// <summary>
        /// Determines if the given database table exists.
        /// </summary>
        /// <param name="SchemaName"></param>
        /// <param name="TableName"></param>
        /// <returns>bool indicating if give table exists</returns>
        public bool TableExists(string SchemaName, string TableName)
        {
            return _dbCatalogMgr.TableExists(SchemaName, TableName);
        }

        // Reloads Cache MetaData for the give table
        public void RefreshCache(string SchemaName, string TableName)
        {
            _dbCatalogMgr.RefreshCache(SchemaName, TableName);
        }

        // Reloads Cache MetaData for the give table names
        public void RefreshCache(List<string> FullyQualifiedTableNames)
        {
            foreach (string tableName in FullyQualifiedTableNames)
                _dbCatalogMgr.RefreshCache(tableName);
        }


        #endregion

        #region Execution Methods


        /// <summary>
        /// Executes the given DbCommand object after setting the given parameter values.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.        
        /// </summary>
        /// <param name="dbCommand">DbCommand object</param>
        /// <param name="dbTrans">A valid DbTransaction or null</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>The return value of the Execute</returns>
        public int ExecuteNonQuery(DbCommand dbCommand
                                , DbTransaction dbTrans = null
                                , params object[] parameterNameValues)
        {
            SetParameterValues(dbCommand, parameterNameValues);

            // dbCmdDebug will not have any runtime overhead and is used only when you are debugging
            // and there is an exception executing the dbCommand.
            // Then if you would like to see a formatted representation of the SQL with parameter declarataions
            // (except binary objects unfortunately), then right click on the dbCmdDebug object.
            // there is a property that will return a formatted string.  
            DbCommandDebug dbCmdDebug = new DbCommandDebug(dbCommand, _dbProvider.GetCommandDebugScript);

            if (dbTrans != null)
                return _dbProvider.ExecuteNonQuery(dbCommand, dbTrans);
            else return _dbProvider.ExecuteNonQuery(dbCommand);
        }

        /// <summary>
        /// Returns a collection of dynamic object (Expando objects) which can be used with MVC 3 newer controls
        /// such as WebGrid.
        /// </summary>
        /// <param name="dbCommand">DAAB DbCommand object for select</param>
        /// <param name="dbTrans">Database transaction object or null</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>Collection of dynamic object</returns>
        public IEnumerable<dynamic> ExecuteDynamic(DbCommand dbCommand
                        , DbTransaction dbTrans = null
                        , params object[] parameterNameValues)
        {
            return ExecuteReader(dbCommand, dbTrans,
                (rdr) =>
                {
                    // Get all the fields and their ordinals from the DataReader
                    var fields = Enumerable.Range(0, rdr.FieldCount)
                        .Select(i => new KeyValuePair<int, string>(i, rdr.GetName(i)));

                    // Read the DataReader till we find a valid row. Convert each row into dynamic Expando
                    // object with field name as the property and set the value to the field value.
                    // ExpandoObject: an object whose members can be dynamically added and removed at run time.
                    // ExpandoObject are like JavaScript objects which are associative array. 
                    //?? Compiler is converting code for the dynamic object member access to the indexer lookup
                    //?? to the internally maintained dictionary?
                    // .ToArray() Realizes the collection. Lazy collection can not have function with DataReader references.
                    return EnumerateDataReader(rdr).Select(row =>
                        fields.Aggregate((IDictionary<string, object>)new ExpandoObject(),
                                (o, kv) => { o[kv.Value] = rdr.GetValue(kv.Key); return o; }))
                              .ToArray();
                },
                parameterNameValues);
        }

        /// <summary>
        /// Creates IEnumerable interface for DataReader enumeration. Each call to the GetNext reads
        /// the next row.
        /// </summary>
        /// <param name="rdr">IDaraReader object which need to be enumerated</param>
        /// <returns>IEnumerable collection</returns>
        private static IEnumerable<int> EnumerateDataReader(IDataReader rdr)
        {
            int i = 0;
            while (rdr.Read()) yield return i++;
        }

        /// <summary>
        /// Returns a collection of type T based upon the results of the given DbCommand query.
        /// </summary>
        /// <typeparam name="T">Type to create</typeparam>
        /// <param name="dbCommand">DAAB DbCommand object for select</param>
        /// <param name="dbTrans">Database transaction object or null</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>Collection of Type T</returns>
        public IEnumerable<T> ExecuteCollection<T>(DbCommand dbCommand
                        , DbTransaction dbTrans = null
                        , params object[] parameterNameValues) where T : new()
        {
            return ExecuteCollection<T>(dbCommand, dbTrans, null, parameterNameValues);
        }

        /// <summary>
        /// Returns a collection of type T based upon the results of the given DbCommand query.
        /// </summary>
        /// <typeparam name="T">Type to create</typeparam>
        /// <param name="dbCommand">DAAB DbCommand object for select</param>
        /// <param name="dbTrans">Database transaction object or null</param>
        /// <param name="dataReaderHandler">An optional function accepting a datareader, dictionary of 
        /// properties for type T, an object context and entity set name, will populate an IEnumerable of T.
        /// To allow programmer to write custom handler.
        /// <para>If null, a default handler will be used.</para></param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>Collection of Type T</returns>
        public IEnumerable<T> ExecuteCollection<T>(DbCommand dbCommand
                        , DbTransaction dbTrans = null
                        , Func<IDataReader, List<KeyValuePair<int, System.Reflection.PropertyInfo>>,
                                IEnumerable<T>> dataReaderHandler = null
                        , params object[] parameterNameValues) where T : new()
        {
            // dbCmdDebug will not have any runtime overhead and is used only when you are debugging
            // and there is an exception executing the dbCommand.
            // Then if you would like to see a formatted representation of the SQL with parameter declarataions
            // (except binary objects unfortunately), then right click on the dbCmdDebug object.
            // there is a property that will return a formatted string.  
            DbCommandDebug dbCmdDebug = new DbCommandDebug(dbCommand, _dbProvider.GetCommandDebugScript);
            using (IDataReader rdr = ExecuteReader(dbCommand, dbTrans, parameterNameValues))
            {
                // Loop throught the columns in the resultset and lookup the properties and ordinals 
                List<KeyValuePair<int, System.Reflection.PropertyInfo>> props =
                        new List<KeyValuePair<int, System.Reflection.PropertyInfo>>();
                Type t = typeof(T);
                for (int i = 0; i < rdr.FieldCount; i++)
                {
                    string fieldName = rdr.GetName(i);
                    // Ignore case of the property name
                    System.Reflection.PropertyInfo pinfo = t.GetProperties()
                        .Where(p => p.Name.ToLower() == fieldName.ToLower()).FirstOrDefault();
                    if (pinfo != null)
                        props.Add(new KeyValuePair<int, System.Reflection.PropertyInfo>(i, pinfo));
                }

                if (dataReaderHandler == null)
                {
                    List<T> items = new List<T>();
                    while (rdr.Read())
                    {

                        T obj = new T();
                        props.ForEach(kv => kv.Value.SetValue(obj,
                                GetValueOrNull(Convert.ChangeType(rdr.GetValue(kv.Key), kv.Value.PropertyType)), null));
                        items.Add(obj);
                    }
                    return items;
                }
                else
                    return dataReaderHandler(rdr, props);
            }
        }


        /// <summary>
        /// If an object is DBNull will return null, otherwise returns the object.
        /// </summary>
        /// <param name="dbValue"></param>
        /// <returns></returns>
        public static object GetValueOrNull(object dbValue)
        {
            return dbValue != DBNull.Value ? dbValue : null;
        }


        /// <summary>
        /// Method used to retrieve the value of an out parameter referred to 
        /// by the given parameter name.
        /// NOTE: Numeric Return Value from Oracle Driver (ODP.NET) must be 
        /// Cast to OracleDecimal, then to .Net Decimal before they can be converted
        /// by the caller.
        /// This method provides a consistent interface for testing out params for null
        /// </summary>
        /// <param name="dbCommand">DbCommand object</param>
        /// <param name="paramName">The name of the parameter to test</param>
        /// <returns>The out param's value as an object</returns>
        public object GetOutParamValue(DbCommand dbCommand, string paramName)
        {
            return _dbProvider.GetOutParamValue(dbCommand, paramName);
        }

        /// <summary>
        /// Executes the given DbCommand object after setting the given parameter values.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.        
        /// </summary>
        /// <param name="dbCommand">DbCommand object</param>
        /// <param name="dbTrans">A valid DbTransaction or null</param>
        /// <param name="tableNames">An arrary names to rename the dataset's tables</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>The dataset of the DbCommand execution</returns>
        public DataSet ExecuteDataSet(DbCommand dbCommand
                        , DbTransaction dbTrans = null
                        , List<string> tableNames = null
                        , params object[] parameterNameValues)
        {
            SetParameterValues(dbCommand, parameterNameValues);

            // dbCmdDebug will not have any runtime overhead and is used only when you are debugging
            // and there is an exception executing the dbCommand.
            // Then if you would like to see a formatted representation of the SQL with parameter declarataions
            // (except binary objects unfortunately), then right click on the dbCmdDebug object.
            // there is a property that will return a formatted string.  
            DbCommandDebug dbCmdDebug = new DbCommandDebug(dbCommand, _dbProvider.GetCommandDebugScript);

            DataSet returnValue = null;
            if (dbTrans != null)
                returnValue = _dbProvider.ExecuteDataSet(dbCommand, dbTrans);
            else returnValue = _dbProvider.ExecuteDataSet(dbCommand);

            if (tableNames != null)
                if (tableNames.Count != returnValue.Tables.Count)
                    throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                                     string.Format("The TableNames count: {0} provided did not match with the table "
                                        + "count returned: {1} in the dataset"
                                            , tableNames.Count, returnValue.Tables.Count)));
                else
                {
                    int tableCount = 0;
                    foreach (DataTable table in returnValue.Tables)
                        table.TableName = tableNames[tableCount++];
                }
            return returnValue;
        }


        /// <summary>
        /// Executes the given DbCommand object after setting the given parameter values.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.        
        /// </summary>
        /// <param name="dbCommand">DbCommand object</param>
        /// <param name="dbTrans">A valid DbTransaction or null</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>The return value of the Execute</returns>
        public object ExecuteScalar(DbCommand dbCommand
                                , DbTransaction dbTrans = null
                        , params object[] parameterNameValues)
        {
            SetParameterValues(dbCommand, parameterNameValues);

            // dbCmdDebug will not have any runtime overhead and is used only when you are debugging
            // and there is an exception executing the dbCommand.
            // Then if you would like to see a formatted representation of the SQL with parameter declarataions
            // (except binary objects unfortunately), then right click on the dbCmdDebug object.
            // there is a property that will return a formatted string.  
            DbCommandDebug dbCmdDebug = new DbCommandDebug(dbCommand, _dbProvider.GetCommandDebugScript);

            if (dbTrans != null)
                return _dbProvider.ExecuteScalar(dbCommand, dbTrans);
            else return _dbProvider.ExecuteScalar(dbCommand);
        }


        /// <summary>
        /// Executes the given DbCommand object after setting the given parameter values.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.        
        /// <para>
        /// NOTE:
        /// A dataReader requires that a connection remain open and there is no
        /// control over whether the client using the reader will close it.
        /// So it is recommended to use the overload function ExecuteReader which
        /// accepts a function as a parameter.  Then the function consumes the dataReader
        /// and the ExecuteReader function closes the dataReader.
        /// </para>
        /// </summary>
        /// <param name="dbCommand">DbCommand object</param>
        /// <param name="dbTrans">A valid DbTransaction or null</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>A data reader object</returns>
        public IDataReader ExecuteReader(DbCommand dbCommand
                                , DbTransaction dbTrans = null
                        , params object[] parameterNameValues)
        {
            SetParameterValues(dbCommand, parameterNameValues);

            // dbCmdDebug will not have any runtime overhead and is used only when you are debugging
            // and there is an exception executing the dbCommand.
            // Then if you would like to see a formatted representation of the SQL with parameter declarataions
            // (except binary objects unfortunately), then right click on the dbCmdDebug object.
            // there is a property that will return a formatted string.  
            DbCommandDebug dbCmdDebug = new DbCommandDebug(dbCommand, _dbProvider.GetCommandDebugScript);

            if (dbTrans != null)
                return _dbProvider.ExecuteReader(dbCommand, dbTrans);
            else return _dbProvider.ExecuteReader(dbCommand);
        }


        /// <summary>
        /// Executes the given DbCommand and returns the result of the dataReaderHandler 
        /// function delegate.  It will be given the DataReader and after its execution,
        /// the DataReader will be destroyed. This prevents from caller to have an active
        /// DataReader where they can leave a connection open. If there are errors in the
        /// delegate functions, the datareader is still closed.
        /// <para>
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// </para>
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <param name="dataReaderHandler">Delegate which will be called to consume the DataReader.</param>
        /// <param name="dbTrans">A valid DbTransaction or null</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>Returns the result of the DataReaderConsumerFunction.</returns>      
        public T ExecuteReader<T>(DbCommand dbCommand
            , DbTransaction dbTrans = null
            , Func<IDataReader, T> dataReaderHandler = null
            , params object[] parameterNameValues)
        {
            if (dataReaderHandler == null)
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException("dataReaderHandler cannot be null"));

            // dbCmdDebug will not have any runtime overhead and is used only when you are debugging
            // and there is an exception executing the dbCommand.
            // Then if you would like to see a formatted representation of the SQL with parameter declarataions
            // (except binary objects unfortunately), then right click on the dbCmdDebug object.
            // there is a property that will return a formatted string.  
            DbCommandDebug dbCmdDebug = new DbCommandDebug(dbCommand, _dbProvider.GetCommandDebugScript);

            using (IDataReader dbRdr = (IDataReader)ExecuteReader(dbCommand
                    , dbTrans
                    , parameterNameValues))
            {
                return (T)dataReaderHandler(dbRdr);
            }
        }
    


        /// <summary>
        /// Executes the given DbCommand object after setting the given parameter values.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.  
        /// <para>
        /// NOTE:
        /// An xmlReader requires that a connection remain open and there is no
        /// control over whether the client using the reader will close it.
        /// So it is recommended to use the overload function ExecuteXmlReader which
        /// accepts a function as a parameter.  Then the function consumes the xmlReader
        /// and the ExecuteXmlReader function closes the xmlReader.
        /// </para>
        /// </summary>
        /// <param name="dbCommand">DbCommand object</param>
        /// <param name="dbTrans">A valid DbTransaction or null</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>A xmlReader object</returns>
        public XmlReader ExecuteXmlReader(DbCommand dbCommand
                                , DbTransaction dbTrans = null
                        , params object[] parameterNameValues)
        {
            SetParameterValues(dbCommand, parameterNameValues);

            // dbCmdDebug will not have any runtime overhead and is used only when you are debugging
            // and there is an exception executing the dbCommand.
            // Then if you would like to see a formatted representation of the SQL with parameter declarataions
            // (except binary objects unfortunately), then right click on the dbCmdDebug object.
            // there is a property that will return a formatted string.  
            DbCommandDebug dbCmdDebug = new DbCommandDebug(dbCommand, _dbProvider.GetCommandDebugScript);

            return _dbProvider.ExecuteXmlReader(dbCommand, dbTrans);
        }


        /// <summary>
        /// Executes the given DbCommand and returns the result of the xmlReaderHandler 
        /// function delegate.  It will be given the DataReader and after its execution,
        /// the xmlReader will be destroyed. This prevents from caller to have an active
        /// xmlReader where they can leave a connection open. If there are errors in the
        /// delegate functions, the xmlareader is still closed.
        /// If a DbException is raised and a logger class had been provided,
        /// the method will attempt to Log a debug text version of the dbCommand
        /// that is backend specific or just log the exception.
        /// In either case, the exception will be thrown.
        /// </summary>
        /// <param name="dbCommand">Database Command Object to execute.</param>
        /// <param name="xmlReaderHandler">Delegate which will be called to consume the xmlReader.</param>
        /// <param name="dbTrans">A valid DbTransaction or null</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        /// <returns>Returns the result of the DataReaderConsumerFunction.</returns>      
        public T ExecuteXmlReader<T>(DbCommand dbCommand
            , Func<XmlReader, T> xmlReaderHandler
            , DbTransaction dbTrans = null
            , params object[] parameterNameValues)
        {
            // dbCmdDebug will not have any runtime overhead and is used only when you are debugging
            // and there is an exception executing the dbCommand.
            // Then if you would like to see a formatted representation of the SQL with parameter declarataions
            // (except binary objects unfortunately), then right click on the dbCmdDebug object.
            // there is a property that will return a formatted string.  
            DbCommandDebug dbCmdDebug = new DbCommandDebug(dbCommand, _dbProvider.GetCommandDebugScript);

            using (XmlReader xmlRdr = (XmlReader)ExecuteXmlReader(dbCommand
                    , dbTrans
                    , parameterNameValues))
            {
                return (T)xmlReaderHandler(xmlRdr);
            }
        }

        /// <summary>
        /// Returns the rowcount of the given database table name
        /// </summary>
        /// <param name="tableName">Fully qualified table name</param>
        /// <returns>Number of rows found in table.</returns>
        public Int64 ExecuteRowCount(string tableName)
        {

            if (!string.IsNullOrEmpty(tableName))
            {
                string selectRowcount = "select count(*) from " + tableName;
                return Convert.ToInt64(ExecuteScalar(_dbProvider.BuildSelectDbCommand(selectRowcount, null)
                    , (DbTransaction)null, null));
            }
            else throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                    "tableName cannot be null or empty."));
        }


        /// <summary>
        /// Sets the parameters in the dbCommand object.
        /// </summary>
        /// <param name="dbCommand">DbCommand object containing parameters that need values.</param>
        /// <param name="parameterNameValues">A set of parameter names and values or null. 
        /// Example: "FirstName", "Ernest", "LastName", "Hemingway"</param>
        private void SetParameterValues(DbCommand dbCommand, params object[] parameterNameValues)
        {
            foreach (DbParameter dbParam in dbCommand.Parameters)
                if (dbParam.Value == null)
                    dbParam.Value = DBNull.Value;

            if (parameterNameValues == null || parameterNameValues.Count() == 0)
                return;

            if (parameterNameValues.Count() % 2 != 0)
                throw new ExceptionMgr(this.ToString()
                    , new ArgumentException(
                        "dbCommand parameters and Name/value parameters are not evenly matched."));

            for (int i = 0; i < parameterNameValues.Count(); i++)
            {
                string paramName = (string)parameterNameValues[i];
                object paramValue = parameterNameValues[i + 1] == null ? DBNull.Value : parameterNameValues[i + 1];
                dbCommand.Parameters[paramName].Value = paramValue;

                i++;
            }
        }

        #endregion

    }
}
