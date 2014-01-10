using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data;
using System.Xml;

using CV.Global;
using CV.Cache;

namespace CV.Database.Provider
{
    public enum DatabaseTypeName { SqlServer, Oracle, Db2 };
    public enum DatabaseProviderName { Microsoft, Oracle, IBM };

    public abstract class DatabaseProviderBase
    {
        protected string _connectionString = null;
        protected string _name = null;
        protected string _dbName = null;
        protected string _dbVersion = null;
        protected string _dbServer = null;
        protected DatabaseTypeName _dbTypeName = DatabaseTypeName.SqlServer;
        protected DatabaseProviderName _dbProviderName = DatabaseProviderName.Microsoft;

        protected string Name
        {
            get { return _name; }
        }

        protected string Version
        {
            get { return _dbVersion; }
        }

        protected string Server
        {
            get { return _dbServer; }
        }

        protected string ConnectionString
        {
            get { return _connectionString; }
        }

        public DatabaseProviderName ProviderName
        {
            get { return _dbProviderName; }
        }

        public DatabaseTypeName TypeName
        {
            get { return _dbTypeName; }
        }

        public DatabaseProviderBase(string connectionString)
        {
            _connectionString = connectionString;
            VerifyConnectionString();
        }

        public abstract void VerifyConnectionString();

        /// <summary>
        /// Returns the back-end compliant syntax for a command that performs no operation.
        /// e.g: -- in SqlServer and Db2.
        /// </summary>
        public virtual string NoOpDbCommandText { get { return Constants.NoOpDbCommandText; } }

        /// <summary>
        /// Returns a string to be used as an alias when joining tables (e.g. T )
        /// </summary>
        public virtual string DefaultTableAlias { get { return Constants.DefaultTableAlias; } }

        /// <summary>
        /// Returns the string character that prefixes parameters for the specific back-end
        /// e.g. @ in SqlServer and Db2
        /// </summary>
        public virtual string ParameterPrefix { get { return Constants.ParameterPrefix; } }

        /// <summary>
        /// Returns the string character that prefixes bind variables for the specific back-end
        /// e.g. @ in SqlServer and Db2
        /// </summary>
        public virtual string BindValuePrefix { get { return Constants.BindValuePrefix; } }

        /// <summary>
        /// Derives the parameters of the given DbCommand object
        /// </summary>
        /// <param name="dbCmd">A DbCommand object</param>
        public abstract void DeriveParameters(DbCommand dbCmd);

        /// <summary>
        /// Adjusts the given command text so that it is back-end compliant.
        /// e.g. wraps in Begin / End block
        /// </summary>
        /// <param name="commandText">Command Text of a DbCommand object</param>
        /// <returns></returns>
        public virtual string FormatCommandText(string commandText)
        {
            return commandText;
        }

        /// <summary>
        /// Provides an opportunity to make any property settings to the given DbCommand object.
        /// e.g. InitialLOBFetchSize for Oracle
        /// </summary>
        /// <param name="dbCmd">A DbCommand object</param>
        /// <returns>A DbCommand object</returns>
        public virtual DbCommand FormatDbCommand(DbCommand dbCmd)
        {
            return dbCmd;
        }

        public abstract DbCommand BuildNoOpDbCommand();

        public abstract DbCommand BuildNonQueryDbCommand(string NonQueryStatement
                            , DbParameterCollection DbParams);


        public abstract DbCommand BuildSelectDbCommand(string SelectStatement
                                , DbParameterCollection DbParams);

        public virtual DbCommand CloneDbCommand(DbCommand dbCmd)
        {
            DbCommand dbCmdClone = BuildNoOpDbCommand();
            dbCmdClone.CommandText = dbCmd.CommandText;
            dbCmdClone.CommandType = dbCmd.CommandType;
            dbCmdClone.CommandTimeout = dbCmd.CommandTimeout;
            CopyParameters(dbCmd.Parameters, dbCmdClone.Parameters);
            return dbCmdClone;
        }

        public abstract DbCommand BuildStoredProcedureDbCommand(string storedProcedure);

        public abstract DbCommand BuildCreateTableDbCommand(DataTable datatable);

        public abstract DbCommand BuildAddIndexesToTableDbCommand(string schema, string table
            , List<DbIndexMetaData> indexes = null);

        public abstract DbCommand BuildCreateTableDbCommand(string sourceSchema, string sourceTable
            , string targetSchema, string targetTable, List<DbIndexMetaData> indexes = null);

        public abstract DbCommand BuildTruncateTableDbCommand(string schema, string table);

        public abstract DbCommand BuildDropTableDbCommand(string schema, string table);


        /// <summary>
        /// Returns the back-end compliant sql fragment for getting the row count for the last operation.
        /// This is not the same as COUNT(*);  It is more like @@RowCount of SQLServer
        /// </summary>
        /// <param name="rowCountParam">A parameter name to store the result of the rowcount function</param>
        /// <returns>A code fragment which will store the rowcount into the given parameter</returns>
        public abstract string FormatRowCountSql(string rowCountParam);

        /// <summary>
        /// Returns the back-end compliant sql fragment for performing Date Arithametic.
        /// Depending on the parameters, the function will add (Days, Hours, ... , milliseconds)
        /// </summary>
        /// <param name="dateDiffInterval">Enumeration of the possible intervals (Days, Hours, Minutes.. MilliSeconds)</param>
        /// <param name="duration">If duration is a string, it will be parameterized; otherwise it will be a constant</param>
        /// <param name="startDate">If startDate is a string, it will be assumed to be a column name;
        /// if it is a dateEnumeration, then it can be either UTC, Local or default.</param>
        /// <returns>A code fragment which will perform the appropriate date add operation.</returns>
        public abstract string FormatDateMathSql(DateTimeInterval dateDiffInterval
                , object duration
                , object startDate);

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
        public abstract string FormatSQLSelectWithMaxRows(string selectStatement, object bufferSize);

        /// <summary>
        /// Returns a back-end compliant sql syntax for beggining a command block in a transaction.
        /// Note this is used for Compound SQL where multiple statements are formatted in a single
        /// DbCommand.CommandText.
        /// </summary>
        /// <param name="tranCount">Used for nested transactions as a comment for readability</param>
        /// <returns>A code fragment which will perform the appropriate operation.</returns>
        public abstract string BeginTransaction(int tranCount);

        /// Note this is used for Compound SQL where multiple statements are formatted in a single
        /// DbCommand.CommandText.
        /// </summary>
        /// <param name="tranCount">Used for nested transactions as a comment for readability</param>
        /// <returns>A code fragment which will perform the appropriate operation.</returns>
        public abstract string CommitTransaction(int tranCount);

        /// <summary>
        /// Returns a back-end compliant sql syntax for com
        /// Returns the back-end compliant parameter name (with ParameterPrefix)
        /// </summary>
        /// <param name="paramName">Parameter Name (with or without prefix)</param>
        /// <returns>Parameter name with prefix</returns>
        public virtual string BuildParameterName(string paramName)
        {
            return paramName.Contains(ParameterPrefix) ? paramName : ParameterPrefix + paramName;
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
            return isNewValueParam ? BuildParameterName(variableName + Constants.ParamSetValueSuffix)
                                    : BuildParameterName(variableName);
        }

        /// <summary>
        /// Returns the back-end compliant variable name (with BindVariablePrefix)
        /// </summary>
        /// <param name="variableName">Variable name (with or without prefix)</param>
        /// <returns>Variable name with prefix)</returns>
        public virtual string BuildBindVariableName(string variableName)
        {
            return variableName.Contains(BindValuePrefix) ? variableName : BindValuePrefix + variableName;
        }

        /// <summary>
        /// Returns the backend specific function for current datetime
        /// to be used in an sql command.
        /// if ReturnAsAlias is not null, it will be the alias for the function
        /// </summary>
        /// <param name="dbDateType">The format type of the date function(local, UTC, Unspecified (UTC))</param>
        /// <param name="returnAsAlias">What the return column will be called</param>
        /// <returns>Backend specific function for current date time including milliseconds</returns>
        public abstract string GetDbTimeAs(DateTimeKind dbDateType, string returnAsAlias);

        /// <summary>
        /// Creates a DbParameter from the given attributes.
        /// </summary>
        /// <param name="paramName">The name of the parameter</param>
        /// <param name="paramType">The data type of the parameter</param>
        /// <param name="nativeDbType">The back-end specific data type</param>
        /// <param name="maxLength">The maximum length of the param for out parameters; 0 otherwise</param>
        /// <param name="paramDirection">The parameter direction</param>
        /// <param name="paramValue">The value of the parameter.</param>
        /// <returns>New DbParameter object</returns>
        public abstract DbParameter CreateNewParameter(string paramName
                , DbType paramType
                , string nativeDbType
                , Int32 maxLength
                , ParameterDirection paramDirection
                , object paramValue);

        /// <summary>
        /// Creates a DbParameterCollection from the given attributes for the first parameter.
        /// </summary>
        /// <param name="paramName">The name of the parameter</param>
        /// <param name="paramType">The data type of the parameter</param>
        /// <param name="nativeDbType">The back-end specific data type</param>
        /// <param name="maxLength">The maximum length of the param for out parameters; 0 otherwise</param>
        /// <param name="paramDirection">The parameter direction</param>
        /// <param name="paramValue">The value of the parameter.</param>
        /// <returns>New DbParameterCollection object</returns>
        public abstract DbParameterCollection CreateNewParameterAndCollection(string paramName
                , DbType paramType
                , string nativeDbType
                , Int32 maxLength
                , ParameterDirection paramDirection
                , object paramValue);

        /// <summary>
        /// Returns a clone of the given parameter
        /// </summary>
        /// <param name="dbParam">The DbParameter to clone</param>
        /// <returns>A copy of the DbParameter</returns>
        public abstract DbParameter CloneParameter(DbParameter dbParam);

        /// <summary>
        /// Returns a clone of the given DbParameter collection.
        /// </summary>
        /// <param name="dbParameters">The collection to clone</param>
        /// <returns>A copy of the DbParameter collection</returns>
        public abstract DbParameterCollection CloneParameterCollection(DbParameterCollection dbParameters);

        /// <summary>
        /// Copies the given DbParameterCollection to the target collection.
        /// </summary>
        /// <param name="dbSourceParameters">A DbParameter collection to add the parameter clone to</param>
        /// <param name="dbDestinationParameters">A DbParameter collection to add the parameter clone to</param>
        public abstract void CopyParameters(DbParameterCollection dbSourceParameters
                , DbParameterCollection dbDestinationParameters);

        /// <summary>
        /// Returns a copy of the given DbParameter that was added to the given collection.
        /// </summary>
        /// <param name="dbParameters">A DbParameter collection to add the parameter clone to</param>
        /// <param name="dbParam">A DbParameter to clone</param>
        /// <returns>The DbParameter clone</returns>
        public abstract DbParameter CopyParameterToCollection(DbParameterCollection dbParameters
                , DbParameter dbParam);
        
        /// <summary>
        /// Returns a boolean indicating if the two parameters are equivalent
        /// (same direction, type, and value);  Out params are always false.
        /// </summary>
        /// <param name="param1">DbParameter1</param>
        /// <param name="param2">DbParameter2</param>
        /// <returns>true or false</returns>
        public abstract bool CompareParamEquality(DbParameter param1, DbParameter param2);

        /// <summary>
        /// Returns the back-end compliant sql syntax for calling the given
        /// stored procedure with the given parameters.
        /// </summary>
        /// <param name="storedProcedure">The stored procedure to call</param>
        /// <param name="dbParameters">The DbParameter collection to use as arguments</param>
        /// <returns>A code fragment which will perform the appropriate operation.</returns>
        public abstract string GenerateStoredProcedureCall(string storedProcedure
                , DbParameterCollection dbParameters);

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
        public virtual object GetOutParamValue(DbCommand dbCommand, string paramName)
        {
            return dbCommand.Parameters[BuildParameterName(paramName)].Value;
        }

        /// <summary>
        /// With Oracle ODP.NET, we must call built in function IsNull instead of comparing to DBNull.Value
        /// This method provides a consistent interface for testing out params for null
        /// </summary>
        /// <param name="dbCommand">DbCommand object</param>
        /// <param name="paramName">The name of the parameter to test</param>
        /// <returns>Boolean indicating if the parameter's value is null</returns>
        public virtual bool IsOutParamValueNull(DbCommand dbCommand, string paramName)
        {
            return dbCommand.Parameters[BuildParameterName(paramName)].Value == DBNull.Value
                   || dbCommand.Parameters[BuildParameterName(paramName)].Value == null;
        }

        /// <summary>
        /// Returns the command text for a DbCommand to obtain the DateTime from the database.
        /// Note: This operation will make a database call.
        /// if ReturnAsAlias is not null, it will be the alias
        /// </summary>
        /// <param name="dbDateType">Enumeration value indicating whether time is local or UTC;
        /// default is UTC.</param>
        /// <param name="returnAsAlias">What the return column will be called</param>
        /// <returns>Back-end compliant command text for returning server time</returns>
        public abstract string GetServerTimeCommandText(DateTimeKind dbDateType, string returnAsAlias);

        /// <summary>
        /// Returns the Data Access Application Block's dataType for the given
        /// database native dataType.
        /// </summary>
        /// <param name="nativeDataType">Database specific dataType</param>
        /// <returns>Data Access Application Block DataType equivalent</returns>
        public abstract DbType GetGenericDbTypeFromNativeDataType(string nativeDataType);

        /// <summary>
        /// Returns the Data Access Application Block's dataType for the given
        /// database numeric dataType.
        /// NOTE: This if for Oracle Only; For other Db's, the size and scale
        /// parameters will be ignored.
        /// </summary>
        /// <param name="nativeDataType">Database specific dataType</param>
        /// <param name="size">Numeric size of the dataType</param>
        /// <param name="scale">Numeric scale of the dataType</param>
        /// <returns>Data Access Application Block DataType equivalent</returns>
        public virtual DbType GetGenericDbTypeFromNativeDataType(string nativeDataType, short size, short scale)
        {
            return GetGenericDbTypeFromNativeDataType(nativeDataType);
        }


        /// <summary>
        /// Returns the database's native dataType for the given
        /// dot net dataType.
        /// </summary>
        /// <param name="dotNetDataType">Dot Net dataType</param>
        /// <returns>Database's Native DataType equivalent</returns>
        public abstract string GetNativeDataTypeFromDotNetDataType(Type dotNetDataType, int size);

        /// <summary>
        /// Returns the .Net dataType for the given
        /// database native dataType.
        /// </summary>
        /// <param name="nativeDataType">String representation of the database native data type</param>
        /// <returns>String representation of the .Net data type</returns>
        public abstract string GetDotNetDataTypeFromNativeDataType(string nativeDataType);

        /// <summary>
        /// Returns the .Net dataType for the given
        /// database numeric dataType.
        /// NOTE: This if for Oracle Only
        /// </summary>
        /// <param name="nativeDataType">String representation of the database native data type</param>
        /// <param name="size">Numeric size of the dataType</param>
        /// <param name="scale">Numeric scale of the dataType</param>
        /// <returns>String representation of the .Net data type</returns>
        public virtual string GetDotNetDataTypeFromNativeDataType(string nativeDataType, short size, short scale)
        {
            return GetDotNetDataTypeFromNativeDataType(nativeDataType);
        }

        /// <summary>
        /// Returns an XmlReader object from the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <param name="dbTran">DbTransaction or null</param>
        /// <returns>XmlReader</returns>
        public abstract XmlReader ExecuteXmlReader(DbCommand dbCommand
                , DbTransaction dbTran);

        /// <summary>
        /// Returns an XmlReader object from the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <returns>XmlReader</returns>
        public abstract XmlReader ExecuteXmlReader(DbCommand dbCommand);

        /// <summary>
        /// Returns a DataSet object from the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <param name="dbTran">DbTransaction or null</param>
        /// <returns>DataSet</returns>
        public abstract DataSet ExecuteDataSet(DbCommand dbCommand
                , DbTransaction dbTran);

        /// <summary>
        /// Returns a DataSet object from the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <returns>DataSet</returns>
        public abstract DataSet ExecuteDataSet(DbCommand dbCommand);

        /// <summary>
        /// Returns an object from the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <param name="dbTran">DbTransaction or null</param>
        /// <returns>Scalar object</returns>
        public abstract object ExecuteScalar(DbCommand dbCommand
                , DbTransaction dbTran);

        /// <summary>
        /// Returns an object from the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <returns>Scalar Object</returns>
        public abstract object ExecuteScalar(DbCommand dbCommand);

        /// <summary>
        /// Executes the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <param name="dbTran">DbTransaction or null</param>
        /// <returns>rows affected</returns>
        public abstract int ExecuteNonQuery(DbCommand dbCommand
                , DbTransaction dbTran);

        /// <summary>
        /// Executes the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <returns>rows affected</returns>
        public abstract int ExecuteNonQuery(DbCommand dbCommand);

        /// <summary>
        /// Executes the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <param name="dbTran">DbTransaction or null</param>
        /// <returns>DataReader</returns>
        public abstract IDataReader ExecuteReader(DbCommand dbCommand
                , DbTransaction dbTran);

        /// <summary>
        /// Executes the database command object
        /// </summary>
        /// <param name="dbCommand">DbCommand Object</param>
        /// <returns>DataReader</returns>
        public abstract IDataReader ExecuteReader(DbCommand dbCommand);

        /// <summary>
        /// Returns a back-end compliant script that can be executed in an interactive editor
        /// such as Management Studio or SQLDeveloper for the given DbCommand.
        /// Since the DbCommands are parameterized, the command text will only contain bind variables
        /// This function will provide variable declarations and initalizations so that the results
        /// can be tested.
        /// </summary>
        /// <param name="dbCmd">DbCommand object</param>
        /// Returns a back-end compliant script that can be executed in an interactive editor
        public abstract string GetCommandDebugScript(DbCommand dbCmd);

        /// <summary>
        /// Returns a boolean indicating whether or not the given dbException is for a primary key constraint
        /// </summary>
        /// <param name="dbe">DbException object</param>
        /// <returns>True if dbException is a primary key violation</returns>
        public abstract bool IsPrimaryKeyViolation(DbException dbe);
    }

}
