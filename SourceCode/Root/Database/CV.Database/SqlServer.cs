using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data.Common;
using System.Data;
using System.Xml;

using ThomsonReuters.Global;

namespace ThomsonReuters.Database.Provider.Microsoft
{
    public class SqlServer : DatabaseProvider
    {
        public SqlServer(string connectionString)
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
            string startDateParam = FormatServerTimeCommandText(DateTimeKind.Unspecified, null);
            if (startDate is DateTimeKind)
                startDateParam = FormatServerTimeCommandText((DateTimeKind)startDate, null);
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
        /// Function takes any select statement and will turn it into a select statement
        /// that will return only the number of rows defined by parameter BufferSize.
        /// If BufferSize is a string, then it will be assumed be a bind variable.
        /// If it is an Int, then the constant will be used.
        /// NOTE: If for some executions you want a full result set without rewriting query
        ///         set BufferSize Param Value = 0;
        ///         Value CANNOT BE SET TO NULL
        /// </summary>
        /// <param name="SelectStatement"></param>
        /// <param name="BufferSize"></param>
        /// <returns></returns>
        public override string FormatSelectWithMaxRowsStatement(string SelectStatement, object BufferSize)
        {
            if (BufferSize == null
                || BufferSize.ToString() == string.Empty)
                return SelectStatement;

            if (!(BufferSize is string || BufferSize is int))
                throw new ArgumentException("BufferSize"
                            , string.Format("Must be non null string or int data type only: BufferSize:{0}", BufferSize));

            return string.Format("set rowcount {0}{2}{1}{2}"
                , BufferSize is string ? BuildBindVariableName(BufferSize.ToString()) : BufferSize.ToString()
                            , SelectStatement
                            , Environment.NewLine);
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
        public override string FormatServerTimeCommandText(DateTimeKind dbDateType, string returnAsAlias)
        {
            string dbTime = dbDateType == DateTimeKind.Local ? "getdate()" : "getutcdate()";
            return string.Format("select {0}"
                , dbTime + (string.IsNullOrEmpty(returnAsAlias) ? "" : " as " + returnAsAlias));
        }

        public override DbCommand BuildGetColumnsCommand()
        {
            DbParameter paramSchemaName = CreateNewParameter(Provider.Constants.SchemaName
                     , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            DbParameter paramTableName = CreateNewParameter(Provider.Constants.TableName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);
            StringBuilder sql = new StringBuilder();

            sql.AppendFormat("select s.name as {0}{1}", Provider.Constants.SchemaName, Environment.NewLine);
            sql.AppendFormat(", t.name as {0}{1}", Provider.Constants.TableName, Environment.NewLine);
            sql.AppendFormat(", c.name as {0}{1}", Provider.Constants.FLD_COLUMN_NAME, Environment.NewLine);
            sql.AppendFormat(", t.object_id as {0}{1}", Provider.Constants.FLD_TABLE_ID, Environment.NewLine);
            sql.AppendFormat(", c.object_id as {0}{1}", Provider.Constants.FLD_COLUMN_ID, Environment.NewLine);
            sql.AppendFormat(", c2.DATA_TYPE  as {0}{1}", Provider.Constants.DataType, Environment.NewLine);
            sql.AppendFormat(", c2.NUMERIC_PRECISION as {0}{1}", Provider.Constants.NumericPrecision, Environment.NewLine);
            sql.AppendFormat(", c2.NUMERIC_PRECISION_RADIX as {0}{1}", Provider.Constants.NumericPrecisionRadix, Environment.NewLine);
            sql.AppendFormat(", c2.NUMERIC_SCALE as {0}{1}", Provider.Constants.NumericScale, Environment.NewLine);
            sql.AppendFormat(", c2.DATETIME_PRECISION as {0}{1}", Provider.Constants.DateTimePrecision, Environment.NewLine);
            sql.AppendFormat(", case when c2.IS_NULLABLE = 'NO' then 0 else 1 end as {0}{1}", Provider.Constants.IsNullable, Environment.NewLine);
            sql.AppendFormat(", c.is_identity as {0}{1}", Provider.Constants.IsIdentity, Environment.NewLine);
            sql.AppendFormat(", c.is_computed as {0}{1}", Provider.Constants.IsComputed, Environment.NewLine);
            sql.AppendFormat(", c.is_rowguidcol as {0}{1}", Provider.Constants.IsRowGuid, Environment.NewLine);
            sql.AppendFormat(", c2.COLUMN_DEFAULT as {0}{1}", Provider.Constants.ColumnDefault, Environment.NewLine);
            sql.AppendFormat(", c2.ORDINAL_POSITION as {0}{1}", Provider.Constants.OrdinalPosition, Environment.NewLine);
            sql.AppendFormat(", c2.CHARACTER_MAXIMUM_LENGTH as {0}{1}", Provider.Constants.CharacterMaximumLength, Environment.NewLine);
            sql.AppendFormat("from sys.tables t{0}", Environment.NewLine);
            sql.AppendFormat("inner join sys.schemas s{0}", Environment.NewLine);
            sql.AppendFormat("on t.schema_id = s.schema_id{0}", Environment.NewLine);
            sql.AppendFormat("inner join sys.columns c{0}", Environment.NewLine);
            sql.AppendFormat("on t.object_id = c.object_id{0}", Environment.NewLine);
            sql.AppendFormat("inner join INFORMATION_SCHEMA.COLUMNS c2{0}", Environment.NewLine);
            sql.AppendFormat("on s.name = c2.TABLE_SCHEMA{0}", Environment.NewLine);
            sql.AppendFormat("and t.name = c2.TABLE_NAME{0}", Environment.NewLine);
            sql.AppendFormat("and c.name = c2.COLUMN_NAME{0}", Environment.NewLine);
            sql.AppendFormat("where ({0} IS NULL OR s.name = {0}){1}", paramSchemaName, Environment.NewLine);
            sql.AppendFormat("and ({0} IS NULL OR t.name = {0}){1}", paramTableName, Environment.NewLine);
            sql.AppendFormat("order by s.name, t.name, c.name{0}", Environment.NewLine);

            DbCommand dbCmd = BuildSelectDbCommand(sql.ToString(), null);
            CopyParameterToCollection(dbCmd.Parameters, paramSchemaName);
            CopyParameterToCollection(dbCmd.Parameters, paramTableName);
            return dbCmd;
        }

        public override DbCommand BuildGetPrimaryKeysCommand()
        {
            DbParameter paramSchemaName = CreateNewParameter(Provider.Constants.SchemaName
                     , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            DbParameter paramTableName = CreateNewParameter(Provider.Constants.TableName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);
            StringBuilder sql = new StringBuilder();

            sql.AppendFormat("select i.name as {0}{1}", Provider.Constants.IndexName, Environment.NewLine);
            sql.AppendFormat(", i.is_primary_key as {0}{1}", Provider.Constants.IsPrimaryKey, Environment.NewLine);
            sql.AppendFormat(", ic.is_descending_key as {0}{1}", Provider.Constants.IsDescending, Environment.NewLine);
            sql.AppendFormat(", ic.key_ordinal as {0}{1}", Provider.Constants.OrdinalPosition, Environment.NewLine);
            sql.AppendFormat(", sc.name as {0}{1}", Provider.Constants.FLD_COLUMN_NAME, Environment.NewLine);
            sql.AppendFormat(", o.name as {0}{1}", Provider.Constants.FLD_TABLE_NAME, Environment.NewLine);
            sql.AppendFormat(", s.name as {0}{1}", Provider.Constants.FLD_SCHEMA_NAME, Environment.NewLine);
            sql.AppendFormat("from sys.indexes i {0}", Environment.NewLine);
            sql.AppendFormat("inner join sys.index_columns ic{0}", Environment.NewLine);
            sql.AppendFormat("on i.object_id = ic.object_id{0}", Environment.NewLine);
            sql.AppendFormat("and i.index_id = ic.index_id{0}", Environment.NewLine);
            sql.AppendFormat("inner join sys.syscolumns sc{0}", Environment.NewLine);
            sql.AppendFormat("on sc.id = i.object_id{0}", Environment.NewLine);
            sql.AppendFormat("and ic.column_id = sc.colid{0}", Environment.NewLine);
            sql.AppendFormat("inner join sys.objects o{0}", Environment.NewLine);
            sql.AppendFormat("on o.object_id = i.object_id{0}", Environment.NewLine);
            sql.AppendFormat("and o.type = 'U'{0}", Environment.NewLine);
            sql.AppendFormat("inner join sys.schemas s{0}", Environment.NewLine);
            sql.AppendFormat("on o.schema_id = s.schema_id{0}", Environment.NewLine);
            sql.AppendFormat("where ({0} IS NULL OR s.name = {0}){1}", paramSchemaName, Environment.NewLine);
            sql.AppendFormat("and ({0} IS NULL OR o.name = {0}){1}", paramTableName, Environment.NewLine);
            sql.AppendFormat("order by s.name, o.name, i.name, ic.key_ordinal{0}", Environment.NewLine);

            DbCommand dbCmd = BuildSelectDbCommand(sql.ToString(), null);
            CopyParameterToCollection(dbCmd.Parameters, paramSchemaName);
            CopyParameterToCollection(dbCmd.Parameters, paramTableName);
            return dbCmd;
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

        /// <summary>
        /// Builds a NonQueryStatement DbCommand for the given
        /// NonQueryStatement Statement and parameter collection.
        /// </summary>
        /// <param name="NonQueryStatement"></param>
        /// <param name="DbParams"></param>
        /// <returns></returns>
        public override DbCommand BuildNonQueryDbCommand(string NonQueryStatement
                                    , DbParameterCollection DbParams)
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                DbCommand dbCmd = conn.CreateCommand();
                dbCmd.CommandText = NonQueryStatement;
                if (DbParams != null)
                    foreach (DbParameter dbParam in DbParams)
                        CopyParameterToCollection(dbCmd.Parameters, dbParam);
                return dbCmd;
            }
        }

        public override DbCommand BuildStoredProcDbCommand(string StoredProcedure, bool RemoveReturnValue)
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                conn.Open();
                SqlCommand sqlCmd = conn.CreateCommand();
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.CommandText = StoredProcedure;
                sqlCmd.Connection = conn;
                SqlCommandBuilder.DeriveParameters(sqlCmd);
                return sqlCmd;
            }
        }


        public override DbCommand BuildNullDbCommand()
        {
            return BuildNonQueryDbCommand(NoOpDbCommandText, null);
        }

        public override DbParameterCollection CreateParameterCollection()
        {
            return BuildNullDbCommand().Parameters;
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
            StringBuilder commandText = new StringBuilder();
            StringBuilder parameters = new StringBuilder();
            string returnParam  = null;
            if (sqlParameters != null && sqlParameters.Count > 0)
            {
                foreach (SqlParameter param in sqlParameters)
                {
                    if (param.Direction == ParameterDirection.ReturnValue)
                    {
                        commandText.AppendFormat("declare {0} int{1}", param.ParameterName, Environment.NewLine);
                        returnParam = param.ParameterName;
                        continue;
                    }

                    parameters.AppendFormat("{0} {1}{2}"
                            , parameters.Length == 0 ? "" : ", "
                            , param.ParameterName
                            , param.Direction == ParameterDirection.Output
                                || param.Direction == ParameterDirection.InputOutput ? " out" : "");
                }
            }
            commandText.AppendFormat("execute{0}{1} {2}"
                , string.IsNullOrEmpty(returnParam) ? " " : string.Format(" {0} = ", returnParam)
                , storedProcedure
                , parameters);
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
        public override DbCommand DeriveParameters(DbCommand dbCmd, bool RemoveReturnValue = true)
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                conn.Open();
                SqlCommand sqlCmd = (SqlCommand)dbCmd;
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.Connection = conn;
                SqlCommandBuilder.DeriveParameters(sqlCmd);
                conn.Close();
                sqlCmd.Connection = null;
                if (RemoveReturnValue)
                    for (int i = 0; i < sqlCmd.Parameters.Count; i++)
                        if (sqlCmd.Parameters[i].Direction == ParameterDirection.ReturnValue)
                            sqlCmd.Parameters.RemoveAt(i);
                return sqlCmd;
            }
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
            if (!paramName.Contains(Constants.ParameterPrefix))
                paramName = Constants.ParameterPrefix + paramName;
            SqlParameter newParam = new SqlParameter(paramName, paramType);
            newParam.Value = paramValue;
            newParam.Direction = paramDirection;
            newParam.DbType = paramType;
            return ValidateParam(newParam, maxLength, paramType);
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
                throw new ArgumentException(string.Format("Parameter {0} already belongs to this collection; use Set to change value."
                                , sqlParam.ParameterName));

            sqlParameters.Add(CloneParameter(sqlParam));
            return sqlParameters[sqlParam.ParameterName];
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
            if (nativeDataType == Constants.DataTypeBigInt)
                return DbType.Int64;
            else if (nativeDataType == Constants.DataTypeTinyint)
                return DbType.Byte;
            else if (nativeDataType == Constants.DataTypeBit)
                return DbType.Boolean;
            else if (nativeDataType == Constants.DataTypeChar
                || nativeDataType == Constants.DataTypeChar
                || nativeDataType == Constants.DataTypeVarChar
                || nativeDataType == Constants.DataTypeNVarChar
                || nativeDataType == Constants.DataTypeText
                || nativeDataType == Constants.DataTypeNText)
                return DbType.String;
            else if (nativeDataType == Constants.DataTypeSmallDateTime
                || nativeDataType == Constants.DataTypeDate
                || nativeDataType == Constants.DataTypeDateTime
                || nativeDataType == Constants.DataTypeDateTime2)
                return DbType.DateTime;
            else if (nativeDataType == Constants.DataTypeMoney
                || nativeDataType == Constants.DataTypeSmallMoney
                || nativeDataType == Constants.DataTypeDecimal
                || nativeDataType == Constants.DataTypeNumeric)
                return DbType.Decimal;
            else if (nativeDataType == Constants.DataTypeInt)
                return DbType.Int32;
            else if (nativeDataType == Constants.DataTypeReal)
                return DbType.Double;
            else if (nativeDataType == Constants.DataTypeSmallInt)
                return DbType.Int16;
            else if (nativeDataType == Constants.DataTypeUniqueId)
                return DbType.Guid;
            else if (nativeDataType == Constants.DataTypeVarBinary)
                return DbType.Binary;
            else throw new ArgumentOutOfRangeException(
                    string.Format("nativeDataType; {0} was not defined as a DotNetType."
                    , nativeDataType));
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
            if (nativeDataType == Constants.DataTypeBigInt)
                return typeof(System.Int64).ToString();
            else if (nativeDataType == Constants.DataTypeTinyint)
                return typeof(System.Byte).ToString();
            else if (nativeDataType == Constants.DataTypeBit)
                return typeof(System.Boolean).ToString();
            else if (nativeDataType == Constants.DataTypeChar
                || nativeDataType == Constants.DataTypeChar
                || nativeDataType == Constants.DataTypeVarChar
                || nativeDataType == Constants.DataTypeNVarChar)
                return typeof(System.String).ToString();
            else if (nativeDataType == Constants.DataTypeText
                || nativeDataType == Constants.DataTypeNText)
                return typeof(System.Object).ToString();
            else if (nativeDataType == Constants.DataTypeSmallDateTime
                || nativeDataType == Constants.DataTypeDate
                || nativeDataType == Constants.DataTypeDateTime
                || nativeDataType == Constants.DataTypeDateTime2)
                return typeof(System.DateTime).ToString();
            else if (nativeDataType == Constants.DataTypeMoney
                || nativeDataType == Constants.DataTypeSmallMoney
                || nativeDataType == Constants.DataTypeDecimal)
                return typeof(System.Decimal).ToString();
            else if (nativeDataType == Constants.DataTypeInt)
                return typeof(System.Int32).ToString();
            else if (nativeDataType == Constants.DataTypeReal)
                return typeof(System.Double).ToString();
            else if (nativeDataType == Constants.DataTypeSmallInt)
                return typeof(System.Int16).ToString();
            else if (nativeDataType == Constants.DataTypeUniqueId)
                return typeof(System.Guid).ToString();
            else if (nativeDataType == Constants.DataTypeNumeric)
                return typeof(System.Decimal).ToString();
            else if (nativeDataType == Constants.DataTypeVarBinary)
                return typeof(System.Byte[]).ToString();
            else throw new ArgumentOutOfRangeException(
                            string.Format("nativeDataType; {0} was not defined as a DotNetType."
                            , nativeDataType));
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

        public override DbDataReader ExecuteReader(DbCommand dbCommand, DbTransaction dbTran)
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
        /// Executes database command object
        /// </summary>
        /// <param name="dbCommand">Database Command Object</param>
        /// <param name="dbTran">Transaction or null</param>
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

        public override void ExecuteBulkCopy(DataTable datatable)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_connectionString))
            {
                bulkCopy.DestinationTableName = datatable.TableName;
                bulkCopy.WriteToServer(datatable);
                bulkCopy.Close();
            }
        }

        /// <summary>
        /// Returns a boolean indicating whether or not the given dbException is for a primary key constraint
        /// </summary>
        /// <param name="dbe">DbException object</param>
        /// <returns>True if dbException is a primary key violation</returns>
        public override bool IsPrimaryKeyViolation(DbException dbException)
        {
            SqlException sqlException = (SqlException)dbException;
            if (sqlException.Number == Constants.DBError_UniqueKeyViolation)
                return true;
            return false;
        }


        /// <summary>
        /// Returns a boolean indicating whether or not the given dbException is for a lock timeout exception
        /// </summary>
        /// <param name="dbe">DbException object</param>
        /// <returns>True if dbException is a locktimeout exception</returns>
        public override bool IsLockTimeOutException(DbException dbException)
        {
            SqlException sqlException = (SqlException)dbException;
            if (sqlException.Number == Constants.DBError_QueryTimeOut)
                return true;
            return false;
        }

        public override bool IsForeignKeyException(DbException dbException)
        {
            SqlException sqlException = (SqlException)dbException;
            if (sqlException.Number == Constants.DBError_ForeignKeyViolation)
                return true;
            return false;
        }

        public override bool IsUniqueConstraintException(DbException dbException)
        {
            SqlException sqlException = (SqlException)dbException;
            if (sqlException.Number == Constants.DBError_UniqueKeyViolation)
                return true;
            return false;
        }

        public override bool IsNullConstraintException(DbException dbException)
        {
            SqlException sqlException = (SqlException)dbException;
            if (sqlException.Number == Constants.DBError_NullConstraintViolation)
                return true;
            return false;
        }

    
    }

}
