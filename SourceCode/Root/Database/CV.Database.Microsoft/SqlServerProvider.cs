using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data.Common;
using System.Data;
using System.Xml;

using CV.Global;
using SqlServer = CV.Database.Provider.Microsoft;

namespace CV.Database
{
    public class SqlServerProvider : DatabaseProviderBase
    {
        public SqlServerProvider(string connectionString)
            : base(connectionString) { }

        public override void VerifyConnectionString()
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                conn.Open();
                _dbName = conn.Database;
                _dbVersion = conn.ServerVersion;
                _dbServer = conn.DataSource;
                _dbProviderName = DatabaseProviderName.Microsoft;
                _dbTypeName = DatabaseTypeName.SqlServer;
                conn.Close();
            }
       }


        SqlCommand CastDbCommand(DbCommand dbCmd)
        {
            SqlCommand sqlCmd = dbCmd as SqlCommand;
            if (sqlCmd == null)
                throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                    string.Format("Invalided SqlCommand Object; could not be cast: {0}"
                    , dbCmd.ToString())));
            return sqlCmd;
        }

        /// <summary>
        /// Returns the back-end compliant sql fragment for getting the row count for the last operation.
        /// This is not the same as COUNT(*);  It is more like @@RowCount of SQLServer
        /// </summary>
        /// <param name="rowCountParam">A parameter name to store the result of the rowcount function</param>
        /// <returns>A code fragment which will store the rowcount into the given parameter</returns>
        public override string FormatRowCountSql(string rowCountParam)
        {
            StringBuilder rowCount = new StringBuilder();
            rowCount.AppendFormat("set {0} = @@rowcount {1};", rowCountParam, Environment.NewLine);
            return rowCount.ToString();
        }
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
        public override string FormatSQLSelectWithMaxRows(string selectStatement, object bufferSize)
        {
            if (bufferSize == null
                || bufferSize.ToString() == string.Empty)
                return selectStatement;

            if (!(bufferSize is string || bufferSize is int))
                throw new ExceptionMgr(this.ToString()
                            , new ArgumentNullException(string.Format("Must be non null string or int data type only: BufferSize:{0}", bufferSize)));

            return string.Format("set rowcount {0}{2}{1}{2}"
                , bufferSize is string ? BuildBindVariableName(bufferSize.ToString()) : bufferSize.ToString()
                            , selectStatement
                            , Environment.NewLine);
        }

        /// <summary>
        /// Returns the SqlServer compliant sql fragment for performing Date Arithametic.
        /// Depending on the parameters, the function will add (Days, Hours, ... , milliseconds)
        /// </summary>
        /// <param name="dateDiffInterval">Enumeration of the possible intervals (Days, Hours, Minutes.. MilliSeconds)</param>
        /// <param name="duration">If duration is a string, it will be parameterized; otherwise it will be a constant</param>
        /// <param name="startDate">If startDate is a string, it will be assumed to be a column name;
        /// if it is a dateEnumeration, then it can be either UTC, Local or default.</param>
        /// <returns>A code fragment which will perform the appropriate date add operation.</returns>
        public override string FormatDateMathSql(Global.DateTimeInterval dateDiffInterval
                , object duration
                , object startDate)
        {
            // is the startDate one of the default parameters
            string startDateParam = GetDbTimeAs(DateTimeKind.Unspecified, null);
            if (startDate is DateTimeKind)
                startDateParam = GetDbTimeAs((DateTimeKind)startDate, null);
            if (startDate is string)    // columnName
                startDateParam = startDate.ToString();

            // determine if the Duration parameter should be a bind variable
            // or is a constant
            object durationParam = 0;    // default is 0 duration.
            if (duration is string
                && !string.IsNullOrEmpty(duration.ToString()))
                BuildBindVariableName(duration.ToString());

            durationParam = duration;
            return string.Format("DateAdd({0}, {1}, {2})", dateDiffInterval.ToString(), durationParam, startDateParam);
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
        public override string GetServerTimeCommandText(DateTimeKind dbDateType, string returnAsAlias)
        {
            return string.Format("select {0}", GetDbTimeAs(dbDateType, returnAsAlias));
        }

        /// <summary>
        /// Returns the backend specific function for current datetime
        /// to be used in an sql command.
        /// if ReturnAsAlias is not null, it will be the alias for the function
        /// </summary>
        /// <param name="dbDateType">The format type of the date function(local, UTC, Unspecified (UTC))</param>
        /// <param name="returnAsAlias">What the return column will be called</param>
        /// <returns>Backend specific function for current date time including milliseconds</returns>
        public override string GetDbTimeAs(DateTimeKind dbDateType, string returnAsAlias)
        {
            string dbTime = dbDateType == DateTimeKind.Local ? "getdate()" : "getutcdate()";
            return dbTime + (string.IsNullOrEmpty(returnAsAlias) ? "" : " as " + returnAsAlias);
        }

        /// <summary>
        /// Return the SQLServer specific statement for executing a stored procedure with a CommandBlock
        /// </summary>
        /// <param name="storedProcedure">Name of stored procedure.</param>
        /// <param name="dbParameters">SqlParameter collection</param>
        /// <returns>A SQLServer compliant statement for executing the given stored procedure and parameters.</returns>
        public override string GenerateStoredProcedureCall(string storedProcedure, DbParameterCollection dbParameters)
        {
            SqlParameterCollection sqlParameters = (SqlParameterCollection)dbParameters;
            StringBuilder commandText = new StringBuilder(string.Format("execute {0} ", storedProcedure));
            if (sqlParameters != null && sqlParameters.Count > 0)
            {
                bool firstParam = true;
                foreach (SqlParameter param in sqlParameters)
                {
                    if (param.Direction == ParameterDirection.ReturnValue)
                        commandText.Insert(0, param.ParameterName + " = ");
                    commandText.AppendFormat("{0} {1}{2}"
                            , firstParam ? "" : ", "
                            , param.ParameterName
                            , param.Direction == ParameterDirection.Output
                                || param.Direction == ParameterDirection.InputOutput ? " out" : "");
                    firstParam = false;
                }
            }
            return commandText.ToString();
        }

        public override string BeginTransaction(int tranCount)
        {
            return string.Format("begin transaction tran{0} /* tran_{0} */ {1}set xact_abort on{1}"
                , tranCount, Environment.NewLine);
        }

        /// <summary>
        /// Returns a SqlServer compliant sql syntax for completing a command block in a transaction.
        /// Note this is used for Compound SQL where multiple statements are formatted in a single
        /// DbCommand.CommandText.
        /// </summary>
        /// <param name="tranCount">Used for nested transactions as a comment for readability</param>
        /// <returns>A code fragment which will perform the appropriate operation.</returns>
        public override string CommitTransaction(int tranCount)
        {
            return string.Format("commit transaction tran{0} /* tran_{0} */ {1}", tranCount, Environment.NewLine);
        }

        /// <summary>
        /// Derives the parameters of the given DbCommand object
        /// </summary>
        /// <param name="dbCmd">A DbCommand object</param>
        public override void DeriveParameters(DbCommand dbCmd)
        {
            //if (dbCmd.Connection != null && dbCmd.ConnectionState.
            SqlCommand sqlCmd = CastDbCommand(dbCmd);
            if (sqlCmd.Connection.State != ConnectionState.Open)
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    conn.Open();
                    sqlCmd.Connection = conn;
                    SqlCommandBuilder.DeriveParameters(sqlCmd);
                    sqlCmd.Connection = null;
                    conn.Close();
                }
            else SqlCommandBuilder.DeriveParameters(sqlCmd);           
        }

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
        public override DbParameter CreateNewParameter(string paramName
            , DbType paramType
            , string nativeDataType
            , Int32 maxLength
            , ParameterDirection paramDirection
            , object paramValue)
        {
            if (!paramName.Contains(SqlServer.Constants.ParameterPrefix))
                paramName = SqlServer.Constants.ParameterPrefix + paramName;
            SqlParameter newParam = new SqlParameter(paramName, paramType);
            newParam.Value = paramValue;
            newParam.Direction = paramDirection;
            newParam.DbType = paramType;
            return ValidateParam(newParam, maxLength, paramType);
        }
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
        public override DbParameterCollection CreateNewParameterAndCollection(string paramName
            , DbType paramType
            , string nativeDataType
            , Int32 maxLength
            , ParameterDirection paramDirection
            , object paramValue)
        {
            DbParameterCollection dbParams = BuildNoOpDbCommand().Parameters;
            CopyParameterToCollection(dbParams
                , CreateNewParameter(paramName
                    , paramType
                    , nativeDataType
                    , maxLength
                    , paramDirection
                    , paramValue));
            return dbParams;
        }

        private static SqlParameter ValidateParam(SqlParameter dbParam, Int32 size, DbType paramType)
        {
            // Only set the size if parameter is an output param
            if (dbParam.Direction == ParameterDirection.InputOutput
                || dbParam.Direction == ParameterDirection.Output)
                dbParam.Size = size;

            // if we need to reset the value to DbNull.value
            if (dbParam.Value == null)
            {
                dbParam.Value = DBNull.Value;
                // make sure we reset the dbType
                dbParam.DbType = paramType;
            }
            return dbParam;
        }


        /// <summary>
        /// Returns a boolean indicating if the two parameters are equivalent
        /// (same direction, type, and value);  Out params are always false.
        /// </summary>
        /// <param name="param1">DbParameter1</param>
        /// <param name="param2">DbParameter2</param>
        /// <returns>true or false</returns>
        public override bool CompareParamEquality(DbParameter dbParam1, DbParameter dbParam2)
        {
            SqlParameter sqlParam1 = (SqlParameter)dbParam1;
            SqlParameter sqlParam2 = (SqlParameter)dbParam2;
            switch (sqlParam1.SqlDbType)
            {
                case SqlDbType.Money:
                case SqlDbType.Decimal:
                    return Convert.ToDecimal(sqlParam1.Value) == Convert.ToDecimal(sqlParam2.Value);
                case SqlDbType.Int:
                    return Convert.ToInt32(sqlParam1.Value) == Convert.ToInt32(sqlParam2.Value);
                case SqlDbType.SmallInt:
                    return Convert.ToInt16(sqlParam1.Value) == Convert.ToInt16(sqlParam2.Value);
                case SqlDbType.TinyInt:
                    return Convert.ToByte(sqlParam1.Value) == Convert.ToByte(sqlParam2.Value);
                case SqlDbType.BigInt:
                    return Convert.ToInt64(sqlParam1.Value) == Convert.ToInt64(sqlParam2.Value);
                case SqlDbType.Real:
                    return Convert.ToSingle(sqlParam1.Value) == Convert.ToSingle(sqlParam2.Value);
                case SqlDbType.Float:
                    return Convert.ToDouble(sqlParam1.Value) == Convert.ToDouble(sqlParam2.Value);
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.VarChar:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                    return sqlParam1.Value.ToString().ToLower() == sqlParam2.Value.ToString().ToLower();
                case SqlDbType.DateTime:
                    return Convert.ToDateTime(sqlParam1.Value) == Convert.ToDateTime(sqlParam2.Value);
                default:
                    return sqlParam1.Value == sqlParam2.Value;
            }
        }


        /// <summary>
        /// Returns a clone of the given parameter
        /// </summary>
        /// <param name="dbParam">The DbParameter to clone</param>
        /// <returns>A copy of the DbParameter</returns>
        public override DbParameter CloneParameter(DbParameter dbParam)
        {
            SqlParameter cloneParam = new SqlParameter(dbParam.ParameterName, dbParam.DbType);
            cloneParam.DbType = dbParam.DbType;
            cloneParam.Direction = dbParam.Direction;
            cloneParam.Value = dbParam.Value;
            cloneParam.SourceColumn = dbParam.SourceColumn;
            cloneParam.SourceColumnNullMapping = dbParam.SourceColumnNullMapping;
            cloneParam.SourceVersion = dbParam.SourceVersion;
            cloneParam.ParameterName = dbParam.ParameterName;
            cloneParam.IsNullable = dbParam.IsNullable;

            return ValidateParam(cloneParam, dbParam.Size, dbParam.DbType);
        }

        /// <summary>
        /// Returns a copy of the given DbParameter that was added to the given collection.
        /// </summary>
        /// <param name="dbParameters">A DbParameter collection to add the parameter clone to</param>
        /// <param name="dbParam">A DbParameter to clone</param>
        /// <returns>The DbParameter clone</returns>
        public override DbParameter CopyParameterToCollection(DbParameterCollection dbParameters
            , DbParameter dbParam)
        {
            SqlParameterCollection sqlParameters = (SqlParameterCollection)dbParameters;
            SqlParameter sqlParam = (SqlParameter)dbParam;
            if (sqlParameters.Contains(sqlParam.ParameterName))
                throw new ExceptionMgr(this.ToString()
                        , new ArgumentException(string.Format("Parameter {0} already belongs to this collection; use Set to change value."
                                , sqlParam.ParameterName)));

            sqlParameters.Add(CloneParameter(sqlParam));
            return sqlParameters[sqlParam.ParameterName];
        }

        public override void CopyParameters(DbParameterCollection dbSourceParameters, DbParameterCollection dbDestinationParameters)
        {
            foreach (DbParameter dbParam in dbSourceParameters)
                CopyParameterToCollection(dbDestinationParameters, dbParam);
        }

        /// <summary>
        /// Returns a clone of the given DbParameter collection.
        /// </summary>
        /// <param name="dbParameters">The collection to clone</param>
        /// <returns>A copy of the DbParameter collection</returns>
        public override DbParameterCollection CloneParameterCollection(DbParameterCollection dbParameters)
        {
            SqlParameterCollection srcSqlCollection = (SqlParameterCollection)dbParameters;
            SqlParameterCollection tgtSqlCollection = (SqlParameterCollection)
                    SqlClientFactory.Instance.CreateCommand().Parameters;
            foreach (SqlParameter dbParam in srcSqlCollection)
                CopyParameterToCollection(tgtSqlCollection, dbParam);
            return tgtSqlCollection;
        }


        /// <summary>
        /// Returns the Data Access Application Block's dataType for the given
        /// database native dataType.
        /// </summary>
        /// <param name="nativeDataType">Database specific dataType</param>
        /// <returns>Data Access Application Block DataType equivalent</returns>
        public override DbType GetGenericDbTypeFromNativeDataType(string nativeDataType)
        {
            nativeDataType = nativeDataType.ToLower();
            if (nativeDataType == SqlServer.Constants.DataTypeBigInt)
                return DbType.Int64;
            else if (nativeDataType == SqlServer.Constants.DataTypeTinyint)
                return DbType.Byte;
            else if (nativeDataType == SqlServer.Constants.DataTypeBit)
                return DbType.Boolean;
            else if (nativeDataType == SqlServer.Constants.DataTypeChar
                || nativeDataType == SqlServer.Constants.DataTypeChar
                || nativeDataType == SqlServer.Constants.DataTypeVarChar
                || nativeDataType == SqlServer.Constants.DataTypeNVarChar
                || nativeDataType == SqlServer.Constants.DataTypeText
                || nativeDataType == SqlServer.Constants.DataTypeNText)
                return DbType.String;
            else if (nativeDataType == SqlServer.Constants.DataTypeSmallDateTime
                || nativeDataType == SqlServer.Constants.DataTypeDate
                || nativeDataType == SqlServer.Constants.DataTypeDateTime
                || nativeDataType == SqlServer.Constants.DataTypeDateTime2)
                return DbType.DateTime;
            else if (nativeDataType == SqlServer.Constants.DataTypeMoney
                || nativeDataType == SqlServer.Constants.DataTypeSmallMoney
                || nativeDataType == SqlServer.Constants.DataTypeDecimal)
                return DbType.Decimal;
            else if (nativeDataType == SqlServer.Constants.DataTypeInt)
                return DbType.Int32;
            else if (nativeDataType == SqlServer.Constants.DataTypeReal)
                return DbType.Double;
            else if (nativeDataType == SqlServer.Constants.DataTypeSmallInt)
                return DbType.Int16;
            else if (nativeDataType == SqlServer.Constants.DataTypeUniqueId)
                return DbType.Guid;
            else throw new ExceptionMgr(this.ToString()
                , new ArgumentOutOfRangeException(
                    string.Format("nativeDataType; {0} was not defined as a DotNetType."
                    , nativeDataType)));
        }



        /// <summary>
        /// Returns the .Net dataType for the given
        /// database native dataType.
        /// </summary>
        /// <param name="nativeDataType">String representation of the database native data type</param>
        /// <returns>String representation of the .Net data type</returns>
        public override string GetDotNetDataTypeFromNativeDataType(string nativeDataType)
        {
            nativeDataType = nativeDataType.ToLower();
            if (nativeDataType == SqlServer.Constants.DataTypeBigInt)
                return typeof(System.Int64).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeTinyint)
                return typeof(System.Byte).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeBit)
                return typeof(System.Boolean).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeChar
                || nativeDataType == SqlServer.Constants.DataTypeChar
                || nativeDataType == SqlServer.Constants.DataTypeVarChar
                || nativeDataType == SqlServer.Constants.DataTypeNVarChar)
                return typeof(System.String).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeText
                || nativeDataType == SqlServer.Constants.DataTypeNText)
                return typeof(System.Object).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeSmallDateTime
                || nativeDataType == SqlServer.Constants.DataTypeDate
                || nativeDataType == SqlServer.Constants.DataTypeDateTime
                || nativeDataType == SqlServer.Constants.DataTypeDateTime2)
                return typeof(System.DateTime).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeMoney
                || nativeDataType == SqlServer.Constants.DataTypeSmallMoney
                || nativeDataType == SqlServer.Constants.DataTypeDecimal)
                return typeof(System.Decimal).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeInt)
                return typeof(System.Int32).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeReal)
                return typeof(System.Double).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeSmallInt)
                return typeof(System.Int16).ToString();
            else if (nativeDataType == SqlServer.Constants.DataTypeUniqueId)
                return typeof(System.Guid).ToString();
            else throw new ExceptionMgr(this.ToString()
                        , new ArgumentOutOfRangeException(
                            string.Format("nativeDataType; {0} was not defined as a DotNetType."
                            , nativeDataType)));
        }


        /// <summary>
        /// Returns a back-end compliant script that can be executed in an interactive editor
        /// such as Management Studio or SQLDeveloper for the given DbCommand.
        /// Since the DbCommands are parameterized, the command text will only contain bind variables
        /// This function will provide variable declarations and initalizations so that the results
        /// can be tested.
        /// </summary>
        /// <param name="dbCmd">DbCommand object</param>
        /// Returns a back-end compliant script that can be executed in an interactive editor
        public override String GetCommandDebugScript(DbCommand dbCmd)
        {
            try
            {
                SqlCommand sqlCmd = (SqlCommand)dbCmd;
                StringBuilder sb = new StringBuilder();
                StringBuilder sbParamList = new StringBuilder();
                SortedDictionary<string, SqlParameter> cmdParams = new SortedDictionary<string
                        , SqlParameter>(StringComparer.CurrentCultureIgnoreCase);
                foreach (SqlParameter param in sqlCmd.Parameters)
                    cmdParams.Add(param.ParameterName, param);
                foreach (string paramName in cmdParams.Keys)
                {
                    SqlParameter param = cmdParams[paramName];
                    sb.AppendFormat("declare {0} {1} {2}"
                        , param.ParameterName
                        , GetParamTypeDecl(param)
                        , Environment.NewLine);
                    if (param.Direction != ParameterDirection.Output
                        && param.Value != null
                        && param.Value != DBNull.Value)
                        sb.AppendFormat("set {0} = {1} {2}"
                            , param.ParameterName
                            , param.SqlDbType == SqlDbType.Char
                                || param.SqlDbType == SqlDbType.DateTime
                                || param.SqlDbType == SqlDbType.NChar
                                || param.SqlDbType == SqlDbType.NVarChar
                                || param.SqlDbType == SqlDbType.UniqueIdentifier
                                || param.SqlDbType == SqlDbType.VarChar
                                || param.SqlDbType == SqlDbType.Xml
                                ? string.Format("'{0}'"
                                    , param.SqlDbType == SqlDbType.DateTime
                                    ? Convert.ToDateTime(param.Value).ToString("yyyy-MM-dd HH:mm:ss.fff")
                                    : param.Value.ToString().Replace("'", "''"))
                                : param.Value
                            , Environment.NewLine);
                    if (sqlCmd.CommandType == CommandType.StoredProcedure)
                        sbParamList.AppendFormat("{0}{1}{2}"
                            , param.ParameterName
                            , sbParamList.Length > 0 ? ", " : ""
                            , param.Direction == ParameterDirection.Output ? " out" : "");
                }
                if (sqlCmd.CommandType == CommandType.StoredProcedure)
                {
                    sb.AppendFormat("execute {0} {1} {2}"
                        , sqlCmd.CommandText
                        , sbParamList.ToString()
                        , Environment.NewLine);
                }
                else sb.AppendFormat("{0} {1}", sqlCmd.CommandText, Environment.NewLine);
                return sb.ToString();
            }
            catch (Exception e)
            {
                return string.Format("-- Error while trying to convert command text to debug string; return commandText {0}"
                        + "--Error: {1}{0}{2}{0}"
                        , Environment.NewLine
                        , e.Message
                        , dbCmd.CommandText);
            }
        }

        private String GetParamTypeDecl(SqlParameter sqlParam)
        {
            if (sqlParam.SqlDbType == SqlDbType.VarChar
                || sqlParam.SqlDbType == SqlDbType.NVarChar)
                return string.Format("{0}({1})"
                    , sqlParam.SqlDbType
                    , sqlParam.Size <= 0 ? "max" : sqlParam.Size.ToString());
            if (sqlParam.SqlDbType == SqlDbType.Decimal
                && sqlParam.Precision != 0
                || sqlParam.Scale != 0)
                return string.Format("{0}({1}, {2})"
                    , sqlParam.SqlDbType
                    , sqlParam.Precision
                    , sqlParam.Scale);
            return string.Format("{0}", sqlParam.SqlDbType);
        }

        /// <summary>
        /// Returns a boolean indicating whether or not the given dbException is for a primary key constraint
        /// </summary>
        /// <param name="dbe">DbException object</param>
        /// <returns>True if dbException is a primary key violation</returns>
        public override bool IsPrimaryKeyViolation(DbException dbException)
        {
            SqlException sqlException = (SqlException)dbException;
            if (sqlException.Number == SqlServer.Constants.DBError_UniqueKeyViolation)
                return true;
            return false;
        }




        /// <summary>
        /// Builds a Select DbCommand for the given
        /// Select Statement and parameter collection.
        /// </summary>
        /// <param name="SelectStatement"></param>
        /// <param name="DbParams"></param>
        /// <returns></returns>
        public override DbCommand BuildSelectDbCommand(string SelectStatement
                                    , DbParameterCollection DbParams)
        {
            return BuildNonQueryDbCommand(SelectStatement, DbParams);
        }


        public override DbCommand BuildNoOpDbCommand()
        {
            return BuildNonQueryDbCommand(NoOpDbCommandText, null);
        }

        /// <summary>
        /// Builds a NonQueryStatement DbCommand for the given
        /// NonQueryStatement Statement and parameter collection.
        /// </summary>
        /// <param name="nonQueryStatement"></param>
        /// <param name="dbParams"></param>
        /// <returns></returns>
        public override DbCommand BuildNonQueryDbCommand(string nonQueryStatement
                                    , DbParameterCollection dbParams)
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                DbCommand dbCmd = conn.CreateCommand();
                dbCmd.CommandText = nonQueryStatement;
                if (dbParams != null)
                    foreach (DbParameter dbParam in dbParams)
                        CopyParameterToCollection(dbParams, dbParam);
                return dbCmd;
            }
        }

        public override DbCommand BuildStoredProcedureDbCommand(string storedProcedure)
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                DbCommand dbCmd = conn.CreateCommand();
                dbCmd.CommandText = storedProcedure;
                dbCmd.CommandType = CommandType.StoredProcedure;
                DeriveParameters(dbCmd);
                return dbCmd;
            }
        }

        /// <summary>
        /// Returns a DataSet object from the database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <param name="dbTran">Transaction or null</param>
        /// <returns>DataSet</returns>
        public override DataSet ExecuteDataSet(DbCommand dbCommand
                , DbTransaction dbTran)
        {
            SqlCommand sqlCommand = (SqlCommand)dbCommand;
            using (SqlConnection con = new SqlConnection(_connectionString))
            {
                con.Open();
                sqlCommand.Connection = con;
                if (dbTran != null)
                    sqlCommand.Transaction = (SqlTransaction)dbTran;
                DataSet results = new DataSet();
                using (SqlDataAdapter data = new SqlDataAdapter(sqlCommand))
                {
                    data.Fill(results);
                }
                return results;
            }
        }

        /// <summary>
        /// Returns a DataSet object from the database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <returns>DataSet</returns>
        public override DataSet ExecuteDataSet(DbCommand dbCommand)
        {
            return ExecuteDataSet(dbCommand, null);
        }

        /// <summary>
        /// Returns an object from the database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <param name="dbTran">Transaction or null</param>
        /// <returns>object</returns>
        public override object ExecuteScalar(DbCommand dbCommand
                , DbTransaction dbTran)
        {
            SqlCommand sqlCommand = (SqlCommand)dbCommand;
            using (SqlConnection con = new SqlConnection(_connectionString))
            {
                con.Open();
                sqlCommand.Connection = con;
                if (dbTran != null)
                    sqlCommand.Transaction = (SqlTransaction)dbTran;
                return sqlCommand.ExecuteScalar();
            }
        }

        /// <summary>
        /// Returns an object from the database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <returns>object</returns>
        public override object ExecuteScalar(DbCommand dbCommand)
        {
            return ExecuteScalar(dbCommand, null);
        }

        /// <summary>
        /// Executes database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <param name="dbTran">Transaction or null</param>
        /// <returns>Rows Affected</returns>
        public override int ExecuteNonQuery(DbCommand dbCommand
                , DbTransaction dbTran)
        {
            SqlCommand sqlCommand = (SqlCommand)dbCommand;
            using (SqlConnection con = new SqlConnection(_connectionString))
            {
                con.Open();
                sqlCommand.Connection = con;
                if (dbTran != null)
                    sqlCommand.Transaction = (SqlTransaction)dbTran;
                return sqlCommand.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Executes database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <returns>RowsAffected</returns>
        public override int ExecuteNonQuery(DbCommand dbCommand)
        {
            return ExecuteNonQuery(dbCommand, null);
        }

        /// <summary>
        /// Returns an IDataReader object from the database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <param name="dbTran">Transaction or null</param>
        /// <returns>IDataReader</returns>
        public override IDataReader ExecuteReader(DbCommand dbCommand
                , DbTransaction dbTran)
        {
            SqlCommand sqlCommand = (SqlCommand)dbCommand;
            using (SqlConnection con = new SqlConnection(_connectionString))
            {
                con.Open();
                sqlCommand.Connection = con;
                if (dbTran != null)
                    sqlCommand.Transaction = (SqlTransaction)dbTran;
                return sqlCommand.ExecuteReader();
            }
        }

        /// <summary>
        /// Returns an IDataReader object from the database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <returns>IDataReader</returns>
        public override IDataReader ExecuteReader(DbCommand dbCommand)
        {
            return ExecuteReader(dbCommand, null);
        }

        /// <summary>
        /// Returns an XmlReader object from the database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <param name="dbTran">Transaction or null</param>
        /// <returns>XmlReader</returns>
        public override XmlReader ExecuteXmlReader(DbCommand dbCommand
                , DbTransaction dbTran)
        {
            SqlCommand sqlCommand = (SqlCommand)dbCommand;
            using (SqlConnection con = new SqlConnection(_connectionString))
            {
                con.Open();
                sqlCommand.Connection = con;
                if (dbTran != null)
                    sqlCommand.Transaction = (SqlTransaction)dbTran;
                return sqlCommand.ExecuteXmlReader();
            }
        }

        /// <summary>
        /// Returns an XmlReader object from the database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <returns>XmlReader</returns>
        public override XmlReader ExecuteXmlReader(DbCommand dbCommand)
        {
            return ExecuteXmlReader(dbCommand, null);
        }


    }
}
