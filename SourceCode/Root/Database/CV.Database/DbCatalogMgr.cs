using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Data;
using System.Data.Common;

using CV.Cache;
using CV.Global;
using CV.Database.Provider;

namespace CV.Database
{

    /// <summary>
    /// Maintains a cache of database catalog information about tables
    /// which can be used for generating dynamic sql.
    /// </summary>
    internal class DbCatalogMgr
    {
        private DatabaseMgr _dbMgr = null;
        CacheMgr<string, DbColumn> _columnCache = new CacheMgr<string, DbColumn>(StringComparer.CurrentCultureIgnoreCase);
        CacheMgr<string, DbTable> _tableCache = new CacheMgr<string, DbTable>(StringComparer.CurrentCultureIgnoreCase);
        CacheMgr<string, DbCommand> _dbCmdCache = new CacheMgr<string, DbCommand>(StringComparer.CurrentCultureIgnoreCase);

        enum TableAttributes
        {
            SchemaName, SchemaId, TableName, TableId, Hierarchy, ReconciledDate,
            Description
        };

        enum ColumnAttributes
        {
            SchemaName, TableName, TableId, ColumnId, ColumnName, DataType, ReconciledDate,
            Description, OrdinalPosition, ColumnDefault, IsNullable, IsIdentity,
            IsComputed, CharacterMaximumLength, NumericPrecision, NumericPrecisionRadix,
            NumericScale, DateTimePrecision
        };

        /// <summary>
        /// Database Catalog Cache of Database Objects (Tables, Columns, PrimaryKeys)
        /// Cache will the lazily initialized as requests for objects are made
        /// </summary>
        /// <param name="dbMgr"></param>
        internal DbCatalogMgr(DatabaseMgr dbMgr)
        {
            _dbMgr = dbMgr;
            // Build the DbCommandCache
            LoadDbCommandCache();
        }

        internal void ClearCache()
        {
            _columnCache.Clear();
            _tableCache.Clear();
        }


        /// <summary>
        /// Build the dbCommands and add them to the cache.
        /// </summary>
        void LoadDbCommandCache()
        {
            DbCommand dbCmd = GetCatalogColumnsCmd();
            _dbCmdCache.Add(Constants.CatalogGetColumns, dbCmd);

            dbCmd = GetCatalogPrimaryKeysCmd();
            _dbCmdCache.Add(Constants.CatalogGetPrimaryKeys, dbCmd);

            dbCmd = GetCatalogIndexesCmd();
            _dbCmdCache.Add(Constants.CatalogGetIndexes, dbCmd);

            dbCmd = GetCatalogForeignKeysCmd();
            _dbCmdCache.Add(Constants.CatalogGetForeignKeys, dbCmd);
        }



        /// <summary>
        /// Returns a structure containing metadata about columns used in an index
        /// </summary>
        /// <param name="columnName">The name of the column</param>
        /// <param name="IsAscending">Whether or not the column is sorted ascending (false = descending)</param>
        /// <returns>DbIndexColumnStructure</returns>
        public DbIndexColumn BuildIndexColumn(string columnName, bool IsAscending)
        {
            DbIndexColumn indexColumn = new DbIndexColumn();
            indexColumn.ColumnName = columnName;
            indexColumn.IsDescending = !IsAscending;
            return indexColumn;
        }

        /// <summary>
        /// Returns a structure containing metadata describing an index column that is sorted ascending
        /// </summary>
        /// <param name="columnName">Name of column</param>
        /// <returns>DbIndexColumnStructure for an ascending index</returns>
        public DbIndexColumn BuildIndexColumnAscending(string columnName)
        {
            return BuildIndexColumn(columnName, true);
        }

        /// <summary>
        /// Returns a structure containing metadata describing an index column that is sorted descending
        /// </summary>
        /// <param name="columnName">Name of column</param>
        /// <returns>DbIndexColumnStructure for an descending index</returns>
        public DbIndexColumn BuildIndexColumnDescending(string columnName)
        {
            return BuildIndexColumn(columnName, false);
        }

        /// <summary>
        /// Returns the DbCommand object to retrieve the database's catalog metadata about columns
        /// </summary>
        /// <returns>DbCommand object</returns>
        DbCommand GetCatalogColumnsCmd()
        {
            DbParameter paramSchemaName = _dbMgr.CreateParameter(Constants.SchemaName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            DbParameter paramTableName = _dbMgr.CreateParameter(Constants.TableName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            // each databaase has a different catalog so the query is different
            switch (_dbMgr.DatabaseType)
            {
                case DatabaseTypeName.SqlServer:
                    {
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.Sys
                                , Constants.Tables
                                , DmlMgr.SelectColumnsAs(Constants.Name, Constants.TableName));

                        joinSelect.AddJoin(Constants.Sys, Constants.Schemas, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Schemas, Constants.Schema_Id)
                                    == j.Column(Constants.Tables, Constants.Schema_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.SchemaName));

                        joinSelect.AddJoin(Constants.Sys, Constants.Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Sys, Constants.Columns, Constants.Object_Id)
                                    == j.Column(Constants.Sys, Constants.Tables, Constants.Object_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.ColumnName)
                                    , joinSelect.ColumnsAs(Constants.Column_Id, Constants.OrdinalPosition)
                                    , joinSelect.ColumnsAs(Constants.Is_RowGuidCol, Constants.IsRowGuidCol)
                                    , joinSelect.ColumnsAs(Constants.Is_Computed, Constants.IsComputed)
                                    , joinSelect.ColumnsAs(Constants.Is_Identity, Constants.IsIdentity));

                        string tAlias = joinSelect.AddJoin(Constants.Information_Schema, Constants.Columns, DbTableJoinType.Inner,
                                (j) => j.Column(Constants.Information_Schema, Constants.Columns, Constants.Table_Schema)
                                    == j.Column(Constants.Sys, Constants.Schemas, Constants.Name)
                                && j.Column(Constants.Information_Schema, Constants.Columns, Constants.Table_Name)
                                    == j.Column(Constants.Sys, Constants.Tables, Constants.Name)
                                && j.Column(Constants.Information_Schema, Constants.Columns, Constants.Column_Name)
                                    == j.Column(Constants.Sys, Constants.Columns, Constants.Name)
                                    , joinSelect.ColumnsAs(Constants.Data_Type, Constants.DataType)
                                    , joinSelect.ColumnsAs(Constants.Column_Default, Constants.ColumnDefault)
                                    , joinSelect.ColumnsAs(Constants.Character_Mximum_Length, Constants.CharacterMaximumLength)
                                    , joinSelect.ColumnsAs(Constants.Numeric_Precision, Constants.NumericPrecision)
                                    , joinSelect.ColumnsAs(Constants.Numeric_Precision_Radix, Constants.NumericPrecisionRadix)
                                    , joinSelect.ColumnsAs(Constants.Numeric_Scale, Constants.NumericScale)
                                    , joinSelect.ColumnsAs(Constants.DataTime_Precision, Constants.DataTimePrecision));

                        joinSelect.AddCaseColumn("0", Constants.IsNullable,
                                joinSelect.When(t => t.AliasedColumn(tAlias, Constants.Is_Nullable) == "yes", "1"));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.Sys, Constants.Schemas, Constants.Name)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.Sys, Constants.Tables, Constants.Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.Schemas
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.Tables
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.Columns
                                , BuildIndexColumnAscending(Constants.Column_Id)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                case DatabaseTypeName.Oracle:
                    {
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.Sys
                                , Constants.All_Tab_Columns
                                , DmlMgr.SelectColumnsAs(Constants.Owner, Constants.SchemaName)
                                , DmlMgr.SelectColumnsAs(Constants.Data_Type, Constants.DataType)
                                , DmlMgr.SelectColumnsAs(Constants.Data_Default, Constants.ColumnDefault)
                                , DmlMgr.SelectColumnsAs(Constants.Char_Length, Constants.CharacterMaximumLength)
                                , DmlMgr.SelectColumnsAs(Constants.Data_Precision, Constants.NumericPrecision)
                                , new DbConstValue(DmlMgr.SelectColumnsAs("null", Constants.NumericPrecisionRadix))
                                , DmlMgr.SelectColumnsAs(Constants.Data_Scale, Constants.NumericScale)
                                , DmlMgr.SelectColumnsAs(Constants.Data_Length, Constants.DataTimePrecision)
                                , DmlMgr.SelectColumnsAs(Constants.Data_Length, Constants.Data_Length)
                                , DmlMgr.SelectColumnsAs(Constants.Column_Name, Constants.ColumnName)
                                , DmlMgr.SelectColumnsAs(Constants.Column_Id, Constants.OrdinalPosition)
                                , new DbConstValue(DmlMgr.SelectColumnsAs("0", Constants.IsRowGuidCol))
                                , new DbConstValue(DmlMgr.SelectColumnsAs("0", Constants.IsComputed))
                                , new DbConstValue(DmlMgr.SelectColumnsAs("0", Constants.IsIdentity))
                                , new DbConstValue(DmlMgr.SelectColumnsAs(Constants.Table_Name, Constants.TableName)));


                        joinSelect.AddCaseColumn("0", Constants.IsNullable,
                                joinSelect.When(t => t.Column(Constants.All_Tab_Columns, Constants.Nullable) == "Y", "1"));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.Sys, Constants.All_Tab_Columns, Constants.Owner)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.Sys, Constants.All_Tab_Columns, Constants.Table_Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Tab_Columns
                                , BuildIndexColumnAscending(Constants.Owner)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Tab_Columns
                                , BuildIndexColumnAscending(Constants.Table_Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Tab_Columns
                                , BuildIndexColumnAscending(Constants.Column_Id)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                case DatabaseTypeName.Db2:
                    {
                        // base table
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.SysIbm
                                , Constants.Columns
                                , DmlMgr.SelectColumnsAs(Constants.Table_Name, Constants.TableName)
                                , DmlMgr.SelectColumnsAs(Constants.Column_Name, Constants.ColumnName)
                                , DmlMgr.SelectColumnsAs(Constants.Ordinal_Position, Constants.OrdinalPosition)
                                , DmlMgr.SelectColumnsAs(Constants.Column_Default, Constants.ColumnDefault)
                                , DmlMgr.SelectColumnsAs(Constants.Character_Mximum_Length
                                    , Constants.CharacterMaximumLength)
                                , DmlMgr.SelectColumnsAs(Constants.Numeric_Precision, Constants.NumericPrecision)
                                , DmlMgr.SelectColumnsAs(Constants.Numeric_Precision_Radix
                                    , Constants.NumericPrecisionRadix)
                                , DmlMgr.SelectColumnsAs(Constants.Numeric_Scale, Constants.NumericScale)
                                , DmlMgr.SelectColumnsAs(Constants.DataTime_Precision
                                    , Constants.DataTimePrecision)
                                , new DbConstValue(DmlMgr.SelectColumnsAs("0", Constants.IsRowGuidCol))
                                , new DbConstValue(DmlMgr.SelectColumnsAs("0", Constants.IsComputed)));

                        string tAlias = joinSelect.AddJoin(Constants.SysIbm, Constants.SQLColumns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.SQLColumns, Constants.Column_Name)
                                    == j.Column(Constants.Columns, Constants.Column_Name)
                                    && j.Column(Constants.SQLColumns, Constants.Table_Name)
                                    == j.Column(Constants.Columns, Constants.Table_Name)
                                    && j.Column(Constants.SQLColumns, Constants.Table_Schem)
                                    == j.Column(Constants.Columns, Constants.Table_Schema)
                                        , joinSelect.ColumnsAs(Constants.Table_Schem, Constants.SchemaName)
                                        , joinSelect.ColumnsAs(Constants.Type_Name, Constants.DataType)
                                        , joinSelect.ColumnsAs(Constants.Nullable, Constants.IsNullable));

                        joinSelect.AddCaseColumn("0", Constants.IsIdentity,
                                joinSelect.When(t => t.AliasedColumn(tAlias, Constants.Pseudo_Column) == 2, "1"));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.SysIbm, Constants.SQLColumns
                                    , Constants.Table_Schem)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.SysIbm, Constants.Columns, Constants.Table_Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.SysIbm
                                , Constants.SQLColumns
                                , BuildIndexColumnAscending(Constants.Table_Schem)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.SysIbm
                                , Constants.Columns
                                , BuildIndexColumnAscending(Constants.Table_Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.SysIbm
                                , Constants.Columns
                                , BuildIndexColumnAscending(Constants.Ordinal_Position)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                default:
                    throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                                    string.Format(Global.Constants.FormatError_FunctionNotImpleted
                                    ,   _dbMgr.DbProvider.TypeName.ToString())));
            }
        }

        DbCommand GetCatalogPrimaryKeysCmd()
        {
            DbParameter paramSchemaName = _dbMgr.CreateParameter(Constants.SchemaName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            DbParameter paramTableName = _dbMgr.CreateParameter(Constants.TableName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            switch (_dbMgr.DatabaseType)
            {
                case DatabaseTypeName.SqlServer:
                    {
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.Sys
                                , Constants.Indexes
                                , DmlMgr.SelectColumnsAs(Constants.Name, Constants.PrimaryKeyName));

                        joinSelect.AddJoin(Constants.Sys, Constants.Index_Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Index_Columns, Constants.Object_Id)
                                    == j.Column(Constants.Indexes, Constants.Object_Id)
                                    && j.Column(Constants.Index_Columns, Constants.Index_Id)
                                    == j.Column(Constants.Indexes, Constants.Index_Id)
                                    && j.Column(Constants.Indexes, Constants.Is_Primary_Key)
                                    == 1
                                    , joinSelect.ColumnsAs(Constants.Is_Descending_Key, Constants.IsDescend)
                                    , joinSelect.ColumnsAs(Constants.Key_Ordinal, Constants.Ordinal));

                        string c = joinSelect.AddJoin(Constants.Sys, Constants.SysColumns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.SysColumns, Constants.Id)
                                    == j.Column(Constants.Indexes, Constants.Object_Id)
                                    && j.Column(Constants.Index_Columns, Constants.Column_Id)
                                    == j.Column(Constants.SysColumns, Constants.ColId)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.ColumnName));

                        string t = joinSelect.AddJoin(Constants.Sys, Constants.Objects, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Objects, Constants.Object_Id)
                                    == j.Column(Constants.Indexes, Constants.Object_Id)
                                    && j.Column(Constants.Objects, Constants.Type)
                                    == "U"
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.TableName));

                        string s = joinSelect.AddJoin(Constants.Sys, Constants.Schemas, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Objects, Constants.Schema_Id)
                                    == j.Column(Constants.Schemas, Constants.Schema_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.SchemaName));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.Sys, Constants.Schemas
                                    , Constants.Name)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.Sys
                                    , Constants.Objects, Constants.Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Sys
                                , Constants.Schemas
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Sys
                                , Constants.Objects
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Sys
                                , Constants.Indexes
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Sys
                                , Constants.Index_Columns
                                , BuildIndexColumnAscending(Constants.Key_Ordinal)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                case DatabaseTypeName.Oracle:
                    {
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.Sys
                                , Constants.All_Indexes
                                , DmlMgr.SelectColumnsAs(Constants.Owner, Constants.SchemaName)
                                , DmlMgr.SelectColumnsAs(Constants.Index_Name, Constants.PrimaryKeyName));

                        joinSelect.AddJoin(Constants.Sys, Constants.All_Ind_Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.All_Indexes, Constants.Owner)
                                    == j.Column(Constants.All_Ind_Columns, Constants.Index_Owner)
                                    && j.Column(Constants.All_Indexes, Constants.Index_Name)
                                    == j.Column(Constants.All_Ind_Columns, Constants.Index_Name)
                                    , joinSelect.ColumnsAs(Constants.Table_Name, Constants.TableName)
                                    , joinSelect.ColumnsAs(Constants.Column_Name, Constants.ColumnName)
                                    , joinSelect.ColumnsAs(Constants.Column_Position, Constants.Ordinal));

                        joinSelect.AddCaseColumn("0", Constants.IsDescend,
                                joinSelect.When(t => t.Column(Constants.All_Ind_Columns, Constants.Descend) == "DESC", "1"));

                        joinSelect.AddJoin(Constants.Sys, Constants.All_Constraints, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.All_Indexes, Constants.Owner)
                                    == j.Column(Constants.All_Constraints, Constants.Owner)
                                    && j.Column(Constants.All_Indexes, Constants.Index_Name)
                                    == j.Column(Constants.All_Constraints, Constants.Constraint_Name));

                        joinSelect.AddCaseColumn("0", Constants.IsPrimaryKey,
                                joinSelect.When(t => t.Column(Constants.All_Constraints, Constants.Constraint_Type) == "P", "1"));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.Sys, Constants.All_Indexes, Constants.Owner)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.Sys, Constants.All_Ind_Columns, Constants.Table_Name)
                                    == paramTableName)
                                && j.Column(Constants.Sys, Constants.All_Constraints, Constants.Constraint_Type)
                                    == "P");

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Indexes
                                , BuildIndexColumnAscending(Constants.Owner)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Ind_Columns
                                , BuildIndexColumnAscending(Constants.Table_Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Indexes
                                , BuildIndexColumnAscending(Constants.Index_Name)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Ind_Columns
                                , BuildIndexColumnAscending(Constants.Column_Position)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                case DatabaseTypeName.Db2:
                    {
                        // base table
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.SysCat
                                , Constants.Indexes
                                , DmlMgr.SelectColumnsAs(Constants.IndSchema, Constants.SchemaName)
                                , DmlMgr.SelectColumnsAs(Constants.TabName, Constants.TableName)
                                , DmlMgr.SelectColumnsAs(Constants.IndName, Constants.PrimaryKeyName));

                        string tAlias = joinSelect.AddJoin(Constants.SysCat, Constants.IndexColUse, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Indexes, Constants.IndSchema)
                                    == j.Column(Constants.IndexColUse, Constants.IndSchema)
                                    && j.Column(Constants.Indexes, Constants.IndName)
                                    == j.Column(Constants.IndexColUse, Constants.IndName)
                                        , joinSelect.ColumnsAs(Constants.ColName, Constants.ColumnName)
                                        , joinSelect.ColumnsAs(Constants.ColSeq, Constants.Ordinal));

                        joinSelect.AddCaseColumn("1", Constants.IsDescend,
                                joinSelect.When(t => t.AliasedColumn(tAlias
                                        , Constants.ColOrder) == "A", "0"));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.SysCat, Constants.Indexes
                                , Constants.IndSchema)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.SysCat, Constants.Indexes
                                , Constants.TabName)
                                    == paramTableName)
                                && j.Column(Constants.Indexes, Constants.UniqueRule) == "P");

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.SysCat
                                , Constants.Indexes
                                , BuildIndexColumnAscending(Constants.IndSchema)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.SysCat
                                , Constants.Indexes
                                , BuildIndexColumnAscending(Constants.TabName)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.SysCat
                                , Constants.Indexes
                                , BuildIndexColumnAscending(Constants.IndName)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(Constants.SysCat
                                , Constants.IndexColUse
                                , BuildIndexColumnAscending(Constants.ColSeq)));
                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                default:
                    throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                                    string.Format("Unknown DatabaseType: {0}", _dbMgr.DatabaseType.ToString())));
            }
        }

        /// <summary>
        /// Returns the DbCommand object to retrieve the database's catalog metadata about indexes
        /// </summary>
        /// <returns>DbCommand object</returns>
        DbCommand GetCatalogIndexesCmd()
        {
            DbParameter paramSchemaName = _dbMgr.CreateParameter(Constants.SchemaName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            DbParameter paramTableName = _dbMgr.CreateParameter(Constants.TableName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            switch (_dbMgr.DatabaseType)
            {
                case DatabaseTypeName.SqlServer:
                    {
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.Sys
                                , Constants.Indexes
                                , DmlMgr.SelectColumnsAs(Constants.Name, Constants.IndexName)
                                , DmlMgr.SelectColumnsAs(Constants.Is_Unique, Constants.IsUnique)
                                , DmlMgr.SelectColumnsAs(Constants.Is_Primary_Key, Constants.IsPrimaryKey)
                                , DmlMgr.SelectColumnsAs(Constants.Type_Desc, Constants.TypeDescription)
                                , new DbConstValue(DmlMgr.SelectColumnsAs("null", Constants.ColumnFunction)));

                        joinSelect.AddCaseColumn("0", Constants.IsClustered,
                            joinSelect.When(j => j.Column(Constants.Indexes, Constants.Index_Id) == "1", "1"));

                        joinSelect.AddJoin(Constants.Sys, Constants.Index_Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Index_Columns, Constants.Object_Id)
                                    == j.Column(Constants.Indexes, Constants.Object_Id)
                                    && j.Column(Constants.Index_Columns, Constants.Index_Id)
                                    == j.Column(Constants.Indexes, Constants.Index_Id)
                                    , joinSelect.ColumnsAs(Constants.Is_Descending_Key, Constants.IsDescend)
                                    , joinSelect.ColumnsAs(Constants.Key_Ordinal, Constants.Ordinal));

                        string c = joinSelect.AddJoin(Constants.Sys, Constants.SysColumns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.SysColumns, Constants.Id)
                                    == j.Column(Constants.Indexes, Constants.Object_Id)
                                    && j.Column(Constants.Index_Columns, Constants.Column_Id)
                                    == j.Column(Constants.SysColumns, Constants.ColId)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.ColumnName));

                        string t = joinSelect.AddJoin(Constants.Sys, Constants.Objects, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Objects, Constants.Object_Id)
                                    == j.Column(Constants.Indexes, Constants.Object_Id)
                                    && j.Column(Constants.Objects, Constants.Type)
                                    == "U"
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.TableName));

                        string s = joinSelect.AddJoin(Constants.Sys, Constants.Schemas, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Objects, Constants.Schema_Id)
                                    == j.Column(Constants.Schemas, Constants.Schema_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.SchemaName));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.Sys, Constants.Schemas
                                    , Constants.Name)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.Sys
                                    , Constants.Objects, Constants.Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Sys
                                , Constants.Schemas
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Sys
                                , Constants.Objects
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Sys
                                , Constants.Indexes
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Sys
                                , Constants.Index_Columns
                                , BuildIndexColumnAscending(Constants.Key_Ordinal)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                case DatabaseTypeName.Oracle:
                    {
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.Sys
                                , Constants.All_Indexes
                                , DmlMgr.SelectColumnsAs(Constants.Owner, Constants.SchemaName)
                                , DmlMgr.SelectColumnsAs(Constants.Index_Name, Constants.IndexName));

                        joinSelect.AddCaseColumn("0", Constants.IsUnique,
                                joinSelect.When(t => t.AliasedColumn(joinSelect.MainTable.TableAlias
                                        , Constants.Uniqueness) == "UNIQUE", "1"));

                        joinSelect.AddJoin(Constants.Sys, Constants.All_Ind_Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.All_Indexes, Constants.Owner)
                                    == j.Column(Constants.All_Ind_Columns, Constants.Index_Owner)
                                    && j.Column(Constants.All_Indexes, Constants.Index_Name)
                                    == j.Column(Constants.All_Ind_Columns, Constants.Index_Name)
                                    , joinSelect.ColumnsAs(Constants.Table_Name, Constants.TableName)
                                    , joinSelect.ColumnsAs(Constants.Column_Name, Constants.ColumnName)
                                    , joinSelect.ColumnsAs(Constants.Column_Position, Constants.Ordinal));

                        joinSelect.AddCaseColumn("0", Constants.IsDescend,
                                joinSelect.When(t => t.Column(Constants.All_Ind_Columns, Constants.Descend) == "DESC", "1"));

                        joinSelect.AddJoin(Constants.Sys, Constants.All_Ind_Expressions, DbTableJoinType.LeftOuter
                                , (j) => j.Column(Constants.All_Indexes, Constants.Owner)
                                    == j.Column(Constants.All_Ind_Expressions, Constants.Index_Owner)
                                    && j.Column(Constants.All_Indexes, Constants.Index_Name)
                                    == j.Column(Constants.All_Ind_Expressions, Constants.Index_Name)
                                    && j.Column(Constants.All_Ind_Columns, Constants.Column_Position)
                                    == j.Column(Constants.All_Ind_Expressions, Constants.Column_Position)
                                    , joinSelect.ColumnsAs(Constants.Column_Expression, Constants.ColumnFunction));

                        joinSelect.AddJoin(Constants.Sys, Constants.All_Constraints, DbTableJoinType.LeftOuter
                                , (j) => j.Column(Constants.All_Indexes, Constants.Owner)
                                    == j.Column(Constants.All_Constraints, Constants.Owner)
                                    && j.Column(Constants.All_Indexes, Constants.Index_Name)
                                    == j.Column(Constants.All_Constraints, Constants.Constraint_Name));

                        joinSelect.AddCaseColumn("0", Constants.IsPrimaryKey,
                                joinSelect.When(t => t.Column(Constants.All_Constraints, Constants.Constraint_Type) == "P", "1"));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.Sys, Constants.All_Indexes, Constants.Owner)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.Sys, Constants.All_Ind_Columns, Constants.Table_Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Indexes
                                , BuildIndexColumnAscending(Constants.Owner)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Ind_Columns
                                , BuildIndexColumnAscending(Constants.Table_Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Indexes
                                , BuildIndexColumnAscending(Constants.Index_Name)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Ind_Columns
                                , BuildIndexColumnAscending(Constants.Column_Position)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                case DatabaseTypeName.Db2:
                    {
                        // base table
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.SysCat
                                , Constants.Indexes
                                , DmlMgr.SelectColumnsAs(Constants.IndSchema, Constants.SchemaName)
                                , DmlMgr.SelectColumnsAs(Constants.TabName, Constants.TableName)
                                , DmlMgr.SelectColumnsAs(Constants.IndName, Constants.IndexName)
                                , DmlMgr.SelectColumnsAs(Constants.IndexType, Constants.TypeDescription)
                                , new DbConstValue(DmlMgr.SelectColumnsAs("null", Constants.ColumnFunction)));

                        joinSelect.AddCaseColumn("0", Constants.IsUnique,
                                joinSelect.When(t => t.AliasedColumn(joinSelect.MainTable.TableAlias
                                        , Constants.UniqueRule) == "P", "1"),
                                joinSelect.When(t => t.AliasedColumn(joinSelect.MainTable.TableAlias
                                        , Constants.UniqueRule) == "U", "1"));

                        joinSelect.AddCaseColumn("0", Constants.IsPrimaryKey,
                                joinSelect.When(t => t.AliasedColumn(joinSelect.MainTable.TableAlias
                                        , Constants.UniqueRule) == "P", "1"));

                        string tAlias = joinSelect.AddJoin(Constants.SysCat, Constants.IndexColUse, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Indexes, Constants.IndSchema)
                                    == j.Column(Constants.IndexColUse, Constants.IndSchema)
                                    && j.Column(Constants.Indexes, Constants.IndName)
                                    == j.Column(Constants.IndexColUse, Constants.IndName)
                                        , joinSelect.ColumnsAs(Constants.ColName, Constants.ColumnName)
                                        , joinSelect.ColumnsAs(Constants.ColSeq, Constants.Ordinal));

                        joinSelect.AddCaseColumn("1", Constants.IsDescend,
                                joinSelect.When(t => t.AliasedColumn(tAlias
                                        , Constants.ColOrder) == "A", "0"));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.SysCat, Constants.Indexes
                                , Constants.IndSchema)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.SysCat, Constants.Indexes
                                , Constants.TabName)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.SysCat
                                , Constants.Indexes
                                , BuildIndexColumnAscending(Constants.IndSchema)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.SysCat
                                , Constants.Indexes
                                , BuildIndexColumnAscending(Constants.TabName)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.SysCat
                                , Constants.Indexes
                                , BuildIndexColumnAscending(Constants.IndName)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(Constants.SysCat
                                , Constants.IndexColUse
                                , BuildIndexColumnAscending(Constants.ColSeq)));
                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                default:
                    throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                            string.Format("Unknown DbType: {0}", _dbMgr.DatabaseType.ToString())));
            }
        }

        /// <summary>
        /// Returns the DbCommand object to retrieve the database's catalog metadata about Foreign Keys
        /// </summary>
        /// <returns>DbCommand object</returns>
        DbCommand GetCatalogForeignKeysCmd()
        {
            DbParameter paramSchemaName = _dbMgr.CreateParameter(Constants.SchemaName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            DbParameter paramTableName = _dbMgr.CreateParameter(Constants.TableName
                    , DbType.String, null, 0, ParameterDirection.Input, DBNull.Value);

            switch (_dbMgr.DatabaseType)
            {
                case DatabaseTypeName.SqlServer:
                    {
                        // base table
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                    , Constants.Sys
                                    , Constants.Tables
                                    , DmlMgr.SelectColumnsAs(Constants.Name, Constants.TableName));

                        joinSelect.AddJoin(Constants.Sys, Constants.Schemas, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Schemas, Constants.Schema_Id)
                                    == j.Column(Constants.Tables, Constants.Schema_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.SchemaName));

                        joinSelect.AddJoin(Constants.Sys, Constants.Foreign_Keys, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Foreign_Keys, Constants.Parent_Object_Id)
                                    == j.Column(Constants.Tables, Constants.Object_Id)
                                    , DmlMgr.SelectColumnsAs(Constants.Name, Constants.ForeignKey));

                        string fkc = joinSelect.AddJoin(Constants.Sys, Constants.Foreign_Key_Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Foreign_Key_Columns, Constants.Constraint_Object_Id)
                                    == j.Column(Constants.Foreign_Keys, Constants.Object_Id));

                        joinSelect.AddJoin(Constants.Sys, Constants.Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Columns, Constants.Object_Id)
                                    == j.Column(Constants.Tables, Constants.Object_Id)
                                    && j.Column(Constants.Foreign_Key_Columns, Constants.Parent_Column_Id)
                                    == j.Column(Constants.Columns, Constants.Column_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.ColumnName));

                        string refT = joinSelect.AddJoin(Constants.Sys, Constants.Tables, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Foreign_Keys, Constants.Referenced_Object_Id)
                                    == j.Column(Constants.Tables, Constants.Object_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.RefTable));

                        joinSelect.AddJoin(Constants.Sys, Constants.Schemas, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Schemas, Constants.Schema_Id)
                                    == j.AliasedColumn(refT, Constants.Schema_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.RefSchema));

                        joinSelect.AddJoin(Constants.Sys, Constants.Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Columns, Constants.Object_Id)
                                    == j.Column(Constants.Foreign_Keys, Constants.Referenced_Object_Id)
                                    && j.AliasedColumn(fkc, Constants.Referenced_Column_Id)
                                    == j.Column(Constants.Columns, Constants.Column_Id)
                                    , joinSelect.ColumnsAs(Constants.Name, Constants.ColumnName));

                        joinSelect.AddJoin(Constants.Information_Schema, Constants.Key_Column_Usage, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.Key_Column_Usage, Constants.Table_Schema)
                                    == j.Column(Constants.Schemas, Constants.Name)
                                    && j.Column(Constants.Key_Column_Usage, Constants.Column_Name)
                                    == j.Column(Constants.Columns, Constants.Name)
                                    && j.Column(Constants.Key_Column_Usage, Constants.Table_Name)
                                    == j.Column(Constants.Tables, Constants.Name)
                                    , joinSelect.ColumnsAs(Constants.Ordinal_Position, Constants.Ordinal));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.Sys, Constants.Schemas
                                , Constants.Name)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.Sys, Constants.Tables
                                , Constants.Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.Schemas
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.Tables
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.Foreign_Keys
                                , BuildIndexColumnAscending(Constants.Name)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(
                                Constants.Information_Schema
                                , Constants.Key_Column_Usage
                                , BuildIndexColumnAscending(Constants.Ordinal_Position)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                case DatabaseTypeName.Oracle:
                    {
                        // base table
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                    , Constants.Sys
                                    , Constants.All_Constraints
                                    , DmlMgr.SelectColumnsAs(Constants.Owner, Constants.SchemaName)
                                    , DmlMgr.SelectColumnsAs(Constants.Table_Name, Constants.TableName)
                                    , DmlMgr.SelectColumnsAs(Constants.Constraint_Name, Constants.ForeignKey));

                        string cc = joinSelect.AddJoin(Constants.Sys, Constants.All_Cons_Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.All_Cons_Columns, Constants.Constraint_Name)
                                    == j.Column(Constants.All_Constraints, Constants.Constraint_Name)
                                    && j.Column(Constants.All_Cons_Columns, Constants.Owner)
                                    == j.Column(Constants.All_Constraints, Constants.Owner)
                                    && j.Column(Constants.All_Cons_Columns, Constants.Table_Name)
                                    == j.Column(Constants.All_Constraints, Constants.Table_Name)
                                    , joinSelect.ColumnsAs(Constants.Column_Name, Constants.ColumnName)
                                    , joinSelect.ColumnsAs(Constants.Position, Constants.Ordinal));

                        joinSelect.AddJoin(Constants.Sys, Constants.All_Cons_Columns, DbTableJoinType.Inner
                                , (j) => j.Column(Constants.All_Cons_Columns, Constants.Constraint_Name)
                                    == j.Column(Constants.All_Constraints, Constants.R_Constraint_Name)
                                    && j.Column(Constants.All_Cons_Columns, Constants.Owner)
                                    == j.Column(Constants.All_Constraints, Constants.R_Owner)
                                    && j.Column(Constants.All_Cons_Columns, Constants.Position)
                                    == j.AliasedColumn(cc, Constants.Position)
                                    , joinSelect.ColumnsAs(Constants.Owner, Constants.RefSchema)
                                    , joinSelect.ColumnsAs(Constants.Table_Name, Constants.RefTable)
                                    , joinSelect.ColumnsAs(Constants.Column_Name, Constants.RefColumn));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.Sys, Constants.All_Constraints
                                , Constants.Owner)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.Sys, Constants.All_Constraints
                                , Constants.Table_Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Constraints
                                , BuildIndexColumnAscending(Constants.Owner)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Constraints
                                , BuildIndexColumnAscending(Constants.Table_Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Constraints
                                , BuildIndexColumnAscending(Constants.Constraint_Name)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(Constants.Sys
                                , Constants.All_Constraints
                                , cc
                                , BuildIndexColumnAscending(Constants.Position)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                case DatabaseTypeName.Db2:
                    {
                        // base table
                        DmlMgr joinSelect = new DmlMgr(_dbMgr
                                , Constants.SysIbm
                                , Constants.SQLForeignKeys
                                , DmlMgr.SelectColumnsAs(Constants.FKTable_Schem, Constants.SchemaName)
                                , DmlMgr.SelectColumnsAs(Constants.FKTable_Name, Constants.TableName)
                                , DmlMgr.SelectColumnsAs(Constants.FK_Name, Constants.ForeignKey)
                                , DmlMgr.SelectColumnsAs(Constants.FKColumn_Name, Constants.ColumnName)
                                , DmlMgr.SelectColumnsAs(Constants.PKTable_Schem, Constants.RefSchema)
                                , DmlMgr.SelectColumnsAs(Constants.PKTable_Name, Constants.RefTable)
                                , DmlMgr.SelectColumnsAs(Constants.PKColumn_Name, Constants.RefColumn)
                                , DmlMgr.SelectColumnsAs(Constants.Key_Seq, Constants.Ordinal));

                        joinSelect.SetWhereCondition((j) =>
                                (paramSchemaName == null || j.Column(Constants.SysIbm, Constants.SQLForeignKeys
                                , Constants.FKTable_Schem)
                                    == paramSchemaName)
                                && (paramTableName == null || j.Column(Constants.SysIbm, Constants.SQLForeignKeys
                                , Constants.FKTable_Name)
                                    == paramTableName));

                        joinSelect.OrderByColumns.Add(1, new DbQualifiedObject<DbIndexColumn>(Constants.SysIbm
                                , Constants.SQLForeignKeys
                                , BuildIndexColumnAscending(Constants.FKTable_Schem)));

                        joinSelect.OrderByColumns.Add(2, new DbQualifiedObject<DbIndexColumn>(Constants.SysIbm
                                , Constants.SQLForeignKeys
                                , BuildIndexColumnAscending(Constants.FKTable_Name)));

                        joinSelect.OrderByColumns.Add(3, new DbQualifiedObject<DbIndexColumn>(Constants.SysIbm
                                , Constants.SQLForeignKeys
                                , BuildIndexColumnAscending(Constants.FK_Name)));

                        joinSelect.OrderByColumns.Add(4, new DbQualifiedObject<DbIndexColumn>(Constants.SysIbm
                                , Constants.SQLForeignKeys
                                , BuildIndexColumnAscending(Constants.Key_Seq)));

                        // build the dbCommand
                        DbCommand dbCmd = _dbMgr.BuildSelectDbCommand(joinSelect, null);
                        // set the parameters
                        dbCmd.Parameters[paramSchemaName.ParameterName].Value = DBNull.Value;
                        dbCmd.Parameters[paramTableName.ParameterName].Value = DBNull.Value;
                        return dbCmd;
                    }
                default:
                    throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                            string.Format("Unknown DbType: {0}", _dbMgr.DatabaseType.ToString())));
            }
        }

        internal DbColumn GetDbColumn(string schemaName, string tableName, string columnName)
        {
            if (string.IsNullOrEmpty(schemaName))
                throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                              string.Format(Global.Constants.FormatError_NullOrEmptyParameter
                              , Constants.SchemaName)));

            if (string.IsNullOrEmpty(tableName))
                throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                              string.Format(Global.Constants.FormatError_NullOrEmptyParameter
                            , Constants.TableName)));

            if (string.IsNullOrEmpty(columnName))
                throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                              string.Format(Global.Constants.FormatError_NullOrEmptyParameter
                            , Constants.ColumnName)));

            // make sure the Table Cache is loaded
            GetDbTable(schemaName, tableName);
            string fullyQualifiedColumnName = schemaName.ToUpper() + "." + tableName.ToUpper()
                    + "." + columnName.ToUpper();

            // Now lookup the column in the cache
            if (!_columnCache.Exists(fullyQualifiedColumnName))
                // if the table cache was loaded, then the column was not found
                throw new ExceptionMgr(this.ToString(), new ArgumentOutOfRangeException(
                               string.Format(Global.Constants.FormatError_InvalidParameterValue
                                    , Constants.ColumnName, fullyQualifiedColumnName
                                    , "Column not found in database catalog")));
            // otherwise return it.
            return _columnCache.Get(fullyQualifiedColumnName);
        }


        internal DbTable GetDbTable(string schemaName, string tableName)
        {
            if (string.IsNullOrEmpty(schemaName))
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                              string.Format(Global.Constants.FormatError_NullOrEmptyParameter
                            , Constants.SchemaName)));

            if (string.IsNullOrEmpty(tableName))
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                              string.Format(Global.Constants.FormatError_NullOrEmptyParameter
                            , Constants.TableName)));

            string fullyQualifiedTableName = schemaName + "." + tableName;
            return GetDbTable(fullyQualifiedTableName);
        }

        internal DbTable GetDbTable(string fullyQualifiedTableName)
        {
            if (string.IsNullOrEmpty(fullyQualifiedTableName))
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                              string.Format(Global.Constants.FormatError_NullOrEmptyParameter
                            , "fullyQualifiedTableName")));

            DbTable cacheRef = _tableCache.GetOrAdd(fullyQualifiedTableName,
                () => GetTableMetaData(fullyQualifiedTableName));
            return new DbTable(cacheRef);
        }

        internal void RefreshCache(string SchemaName, string TableName)
        {
            RefreshCache(string.Format("{0}.{1}", SchemaName, TableName));
        }
        
        internal void RefreshCache(string FullyQualifiedTableName)
        {
            if (_tableCache.Exists(FullyQualifiedTableName))
            {
                DbTable tbl = _tableCache.Get(FullyQualifiedTableName);
                lock (tbl.Columns)
                {
                    foreach (string column in tbl.Columns.Keys)
                    {
                        string fullyQualifiedColumn = string.Format("{0}.{1}", tbl.FullyQualifiedName, column);
                        if (_columnCache.Exists(fullyQualifiedColumn))
                            _columnCache.Remove(fullyQualifiedColumn);
                    }
                    _tableCache.Remove(FullyQualifiedTableName);
                }
            }
            GetTableMetaData(FullyQualifiedTableName);
        }


        internal bool TableExists(string SchemaName, string TableName)
        {
            string fullyQualifiedTableName = SchemaName + "." + TableName;
            if (!_tableCache.Exists(fullyQualifiedTableName))
            {
                DataSet catalogDataSet = GetCatalogData(fullyQualifiedTableName.ToUpper());
                if (catalogDataSet.Tables[Database.Constants.Columns].Rows.Count > 0)
                    GetTableMetaData(fullyQualifiedTableName, catalogDataSet);
                else return false;
            }
            return true;
        }

        bool CheckTableMetaData(string fullyQualifiedTableName)
        {
            DataSet catalogDataSet = GetCatalogData(fullyQualifiedTableName.ToUpper());
            return (catalogDataSet.Tables[Database.Constants.Columns].Rows.Count > 0);
        }

        DbTable GetTableMetaData(string fullyQualifiedTableName)
        {
            return GetTableMetaData(fullyQualifiedTableName
                , GetCatalogData(fullyQualifiedTableName.ToUpper()));
        }

        /// <summary>
        /// Creates the table meta data structure for a given fully qualified table name.
        /// (Schema and table Name)
        /// </summary>
        /// <param name="fullyQualifiedTableName"></param>
        DbTable GetTableMetaData(string fullyQualifiedTableName, DataSet catalogDataSet)
        {
            // see if table was found
            if (catalogDataSet.Tables[Database.Constants.Columns].Rows.Count == 0)
                throw new ExceptionMgr(this.ToString(), new ArgumentNullException(
                              string.Format(Global.Constants.FormatError_InvalidParameterValue
                                    , "fullyQualifiedTableName", fullyQualifiedTableName, "Table MetaData Was Not Found")));

            // otherwise populate the cache
            return CreateTableMetaData(catalogDataSet.Tables[Constants.PrimaryKeys]
                        , catalogDataSet.Tables[Constants.Columns]
                        , catalogDataSet.Tables[Constants.Indexes]
                        , catalogDataSet.Tables[Constants.ForeignKeys]);
        }

        /// <summary>
        /// Get the Catalog Data from the Database by fully qualified table name
        /// (Schema and Table Name)
        /// </summary>
        /// <param name="fullyQualifiedTableName"></param>
        /// <returns>A dataset of catalog data</returns>
        DataSet GetCatalogData(string fullyQualifiedTableName)
        {
            string[] nameParts = fullyQualifiedTableName.Split(new char[] { '.' });
            string schemaName = nameParts[0];
            string tableName = nameParts[1];
            return GetCatalogData(schemaName, tableName);
        }


        /// <summary>
        /// Returns the Database Catalog MetaData for the given table and schema
        /// </summary>
        /// <param name="schemaName">Schema that table belongs to</param>
        /// <param name="tableName">Table name to retrieve meta data</param>
        /// <returns>Dataset of meta data (columns, indexes, primaryKey, foreignKey)</returns>
        DataSet GetCatalogData(string schemaName, string tableName)
        {
            string schemaNameUpper = !string.IsNullOrEmpty(schemaName) ? schemaName.ToUpper() : null;
            string tableNameUpper = !string.IsNullOrEmpty(tableName) ? tableName.ToUpper() : null;
            List<string> tableNames = new List<string>();
            DbCommandMgr dbCmdMgr = new DbCommandMgr(_dbMgr);
            DbCommand cmdPrimaryKey = _dbMgr.DbProvider.CloneDbCommand(_dbCmdCache.Get(Constants.CatalogGetPrimaryKeys));
            if (!string.IsNullOrEmpty(schemaNameUpper))
                cmdPrimaryKey.Parameters[dbCmdMgr.BuildParamName(Constants.SchemaName)].Value = schemaNameUpper;
            else cmdPrimaryKey.Parameters[dbCmdMgr.BuildParamName(Constants.SchemaName)].Value = DBNull.Value;

            if (!string.IsNullOrEmpty(tableNameUpper))
                cmdPrimaryKey.Parameters[dbCmdMgr.BuildParamName(Constants.TableName)].Value = tableNameUpper;
            else cmdPrimaryKey.Parameters[dbCmdMgr.BuildParamName(Constants.TableName)].Value = DBNull.Value;

            dbCmdMgr.AddDbCommand(cmdPrimaryKey);
            tableNames.Add(Constants.PrimaryKeys);

            DbCommand cmdIndex = _dbMgr.DbProvider.CloneDbCommand(_dbCmdCache.Get(Constants.CatalogGetIndexes));
            if (!string.IsNullOrEmpty(schemaNameUpper))
                cmdIndex.Parameters[dbCmdMgr.BuildParamName(Constants.SchemaName)].Value = schemaNameUpper;
            else cmdIndex.Parameters[dbCmdMgr.BuildParamName(Constants.SchemaName)].Value = DBNull.Value;

            if (!string.IsNullOrEmpty(tableNameUpper))
                cmdIndex.Parameters[dbCmdMgr.BuildParamName(Constants.TableName)].Value = tableNameUpper;
            else cmdIndex.Parameters[dbCmdMgr.BuildParamName(Constants.TableName)].Value = DBNull.Value;

            dbCmdMgr.AddDbCommand(cmdIndex);
            tableNames.Add(Constants.Indexes);

            DbCommand cmdForeignKeys = _dbMgr.DbProvider.CloneDbCommand(_dbCmdCache.Get(Constants.CatalogGetForeignKeys));
            if (!string.IsNullOrEmpty(schemaNameUpper))
                cmdForeignKeys.Parameters[dbCmdMgr.BuildParamName(Constants.SchemaName)].Value = schemaNameUpper;
            else cmdForeignKeys.Parameters[dbCmdMgr.BuildParamName(Constants.SchemaName)].Value = DBNull.Value;

            if (!string.IsNullOrEmpty(tableNameUpper))
                cmdForeignKeys.Parameters[dbCmdMgr.BuildParamName(Constants.TableName)].Value = tableNameUpper;
            else cmdForeignKeys.Parameters[dbCmdMgr.BuildParamName(Constants.TableName)].Value = DBNull.Value;

            dbCmdMgr.AddDbCommand(cmdForeignKeys);
            tableNames.Add(Constants.ForeignKeys);

            DbCommand cmdColumns = _dbMgr.DbProvider.CloneDbCommand(_dbCmdCache.Get(Constants.CatalogGetColumns));
            if (!string.IsNullOrEmpty(schemaNameUpper))
                cmdColumns.Parameters[dbCmdMgr.BuildParamName(Constants.SchemaName)].Value = schemaNameUpper;
            else cmdColumns.Parameters[dbCmdMgr.BuildParamName(Constants.SchemaName)].Value = DBNull.Value;

            if (!string.IsNullOrEmpty(tableNameUpper))
                cmdColumns.Parameters[dbCmdMgr.BuildParamName(Constants.TableName)].Value = tableNameUpper;
            else cmdColumns.Parameters[dbCmdMgr.BuildParamName(Constants.TableName)].Value = DBNull.Value;

            dbCmdMgr.AddDbCommand(cmdColumns);
            tableNames.Add(Constants.Columns);

            return dbCmdMgr.ExecuteDataSet(tableNames);
        }

        /// <summary>
        /// Returns the combined meta data of a table given the metadata of its different objects
        /// </summary>
        /// <param name="primaryKey">PrimaryKey meta data</param>
        /// <param name="columns">Column meta data</param>
        /// <param name="indexes">Index meta data</param>
        /// <param name="foreignKeys">ForeignKey meta data</param>
        /// <returns>Structure of table meta data</returns>
        DbTable CreateTableMetaData(DataTable primaryKey, DataTable columns, DataTable indexes, DataTable foreignKeys)
        {
            DbTable table = PopulateColumnCache(columns);
            table.PrimaryKeyColumns = GetPrimaryKeyColumns(table.SchemaName, table.TableName, primaryKey);
            table.PrimaryKey = GetPrimaryKey(table.SchemaName, table.TableName, primaryKey);
            table.Indexes = GetIndexes(table.SchemaName, table.TableName, table.Columns, indexes);
            table.ForeignKeys = GetForeignKeys(table.SchemaName, table.TableName, foreignKeys);
            return table;
        }

        /// <summary>
        /// Returns the index meta data for the given schema and table names and column and index data
        /// </summary>
        /// <param name="tableSchema">Schema index belongs to</param>
        /// <param name="tableName">Table index belongs to</param>
        /// <param name="tableColumns">the tables columns</param>
        /// <param name="indexes">DataTable of indexes for the table</param>
        /// <returns>Dictionary of index name and index meta data</returns>
        Dictionary<string, DbIndex> GetIndexes(string tableSchema
                , string tableName
                , Dictionary<string, Int16> tableColumns
                , DataTable Indexes)
        {
            Dictionary<string, DbIndex> indexes = new Dictionary<string, DbIndex>(StringComparer.CurrentCultureIgnoreCase);
            string prevIndexName = null;
            DataRow[] tableIndexes = Indexes.Select(string.Format("{0} = '{1}' and {2} = '{3}'"
                                                        , Constants.SchemaName
                                                        , tableSchema
                                                        , Constants.TableName
                                                        , tableName));
            DbIndex index = new DbIndex();
            index.IncludeColumns = new List<string>();
            index.Columns = new CacheMgr<string, DbIndexColumn>(StringComparer.CurrentCultureIgnoreCase);
            index.ColumnOrder = new SortedDictionary<short, DbIndexColumn>();
            for (int i = 0; i < tableIndexes.Length; i++)
            {
                string indexName = tableIndexes[i][Constants.IndexName].ToString().ToLower();
                if (prevIndexName != null && prevIndexName != indexName)
                {
                    indexes.Add(index.IndexName, index);
                    index = new DbIndex();
                    index.IncludeColumns = new List<string>();
                    index.Columns = new CacheMgr<string, DbIndexColumn>(StringComparer.CurrentCultureIgnoreCase);
                    index.ColumnOrder = new SortedDictionary<short, DbIndexColumn>();
                    index.SchemaName = tableSchema;
                    index.TableName = tableName;
                    index.IndexName = indexName;
                    prevIndexName = indexName;
                }
                else if (prevIndexName == null)
                {
                    prevIndexName = indexName;
                    index.SchemaName = tableSchema;
                    index.TableName = tableName;
                    index.IndexName = indexName;
                }
                DbIndexColumn indexColumn = new DbIndexColumn();
                indexColumn.ColumnName = tableIndexes[i][Constants.ColumnName].ToString();
                byte ordinal = Convert.ToByte(tableIndexes[i][Constants.Ordinal]);
                if (ordinal == 0)
                    index.IncludeColumns.Add(indexColumn.ColumnName);
                else
                {
                    indexColumn.ColumnFunction = tableIndexes[i][Constants.ColumnFunction].ToString();
                    indexColumn.IsDescending = Convert.ToBoolean(tableIndexes[i][Constants.IsDescend]);
                    index.ColumnOrder.Add(ordinal++, indexColumn);
                    index.Columns.Add(indexColumn.ColumnName, indexColumn);
                }
                index.IsClustered = Convert.ToBoolean(tableIndexes[i][Constants.IsClustered]);
                index.IsPrimaryKey = Convert.ToBoolean(tableIndexes[i][Constants.IsPrimaryKey]);
                index.IsUnique = Convert.ToBoolean(tableIndexes[i][Constants.IsUnique]);
            }
            if (!string.IsNullOrEmpty(prevIndexName))
                indexes.Add(index.IndexName, index);
            return indexes;
        }


        /// <summary>
        /// Returns the foreign key meta data for the given schema and table names and foreign key data
        /// </summary>
        /// <param name="tableSchema">Schema index belongs to</param>
        /// <param name="tableName">Table index belongs to</param>
        /// <param name="foreignKeys">Datatable of foreign key meta data</param>
        /// <returns>Dictionary of foreign key name and foreign key meta data</returns>
        Dictionary<string, DbForeignKey> GetForeignKeys(string tableSchema
                , string tableName
                , DataTable foreignKeys)
        {
            DataRow[] tableForeignKeys = foreignKeys.Select(string.Format("{0} = '{1}' and {2} = '{3}'"
                                                        , Constants.SchemaName
                                                        , tableSchema
                                                        , Constants.TableName
                                                        , tableName));

            Dictionary<string, DbForeignKey> tableFKeyCollection
                    = new Dictionary<string, DbForeignKey>(StringComparer.CurrentCultureIgnoreCase);

            DbForeignKey foreignKey = new DbForeignKey();
            foreignKey.KeyColumns = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            foreignKey.KeyOrder = new SortedDictionary<Int16, string>();
            for (byte i = 0; i < tableForeignKeys.Length; i++)
            {
                if (string.IsNullOrEmpty(foreignKey.ForeignKeyName)
                    || tableForeignKeys[i][Constants.ForeignKey].ToString().ToUpper() != foreignKey.ForeignKeyName.ToUpper())
                {
                    // we are on a new index
                    // save the previous one if there was one
                    if (!string.IsNullOrEmpty(foreignKey.ForeignKeyName))
                    {
                        tableFKeyCollection.Add(foreignKey.ForeignKeyName, foreignKey);
                        foreignKey = new DbForeignKey();
                        foreignKey.KeyColumns = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
                        foreignKey.KeyOrder = new SortedDictionary<Int16, string>();
                    }
                    foreignKey.ForeignKeyName = tableForeignKeys[i][Constants.ForeignKey].ToString();
                    foreignKey.SchemaName = tableForeignKeys[i][Constants.RefSchema].ToString();
                    foreignKey.TableName = tableForeignKeys[i][Constants.RefTable].ToString();
                }

                foreignKey.KeyColumns.Add(tableForeignKeys[i][Constants.ColumnName].ToString(), tableForeignKeys[i][Constants.RefTable].ToString());
                foreignKey.KeyOrder.Add(Convert.ToInt16(tableForeignKeys[i][Constants.Ordinal]), tableForeignKeys[i][Constants.ColumnName].ToString());
            }
            if (!string.IsNullOrEmpty(foreignKey.ForeignKeyName))
                tableFKeyCollection.Add(foreignKey.ForeignKeyName, foreignKey);
            return tableFKeyCollection;
        }

        /// <summary>
        /// Returns the primary key columns for the given schema and table names and primary key data
        /// </summary>
        /// <param name="tableSchema">Schema index belongs to</param>
        /// <param name="tableName">Table index belongs to</param>
        /// <param name="primaryKey">DataTable of primary key for the table</param>
        /// <returns>Dictionary of column name and index key number</returns>
        Dictionary<string, byte> GetPrimaryKeyColumns(string tableSchema, string tableName, DataTable primaryKey)
        {
            DataRow[] primaryKeyCols = primaryKey.Select(string.Format("{0} = '{1}' and {2} = '{3}'"
                                                        , Constants.SchemaName
                                                        , tableSchema
                                                        , Constants.TableName
                                                        , tableName));
            Dictionary<string, byte> primaryKeyColumnNames
                    = new Dictionary<string, byte>(StringComparer.CurrentCultureIgnoreCase);
            for (byte i = 0; i < primaryKeyCols.Length; i++)
                primaryKeyColumnNames.Add(primaryKeyCols[i][Constants.ColumnName].ToString()
                        , i);
            return primaryKeyColumnNames;
        }

        /// <summary>
        /// Returns the ordered columns of the given primary key
        /// </summary>
        /// <param name="tableSchema">Schema index belongs to</param>
        /// <param name="tableName">Table index belongs to</param>
        /// <param name="primaryKey">DataTable of primary key for the table</param>
        /// <returns>SortedDictionary of the primary key columns</returns>
        SortedDictionary<byte, string> GetPrimaryKey(string tableSchema, string tableName, DataTable primaryKey)
        {
            DataRow[] primaryKeyCols = primaryKey.Select(string.Format("{0} = '{1}' and {2} = '{3}'"
                                                        , Constants.SchemaName
                                                        , tableSchema
                                                        , Constants.TableName
                                                        , tableName));
            SortedDictionary<byte, string> primaryKeyColumnNames = new SortedDictionary<byte, string>();
            for (byte i = 0; i < primaryKeyCols.Length; i++)
                primaryKeyColumnNames.Add(i, primaryKeyCols[i][Constants.ColumnName].ToString());
            return primaryKeyColumnNames;
        }

        /// <summary>
        /// Returns table meta data structure for the given DataTable of column meta data.
        /// </summary>
        /// <param name="columns">DataTable of column meta data</param>
        /// <returns>Table metadata structure</returns>
        /// <exception cref="ExceptionEvent">Any exception during the processing of the cache.  
        /// Message will include Schema and Table Name</exception>
        DbTable PopulateColumnCache(DataTable columns)
        {
            DbCommandMgr dbCmdMgr = new DbCommandMgr(_dbMgr);
            DbTable table = new DbTable();
            table.SchemaName = columns.Rows[0][TableAttributes.SchemaName.ToString()].ToString();
            table.TableName = columns.Rows[0][TableAttributes.TableName.ToString()].ToString();
            table.Columns = new Dictionary<string, Int16>(StringComparer.CurrentCultureIgnoreCase);
            table.Row = new SortedDictionary<Int16, string>();
            foreach (DataRow columnRow in columns.Rows)
            {
                try
                {
                    DbColumn column = new DbColumn();
                    column.SchemaName = columnRow[ColumnAttributes.SchemaName.ToString()].ToString();
                    column.TableName = columnRow[ColumnAttributes.TableName.ToString()].ToString();
                    column.ColumnName = columnRow[ColumnAttributes.ColumnName.ToString()].ToString();
                    column.Ordinal = Convert.ToInt16(columnRow[ColumnAttributes.OrdinalPosition.ToString()]);

                    table.Columns.Add(column.ColumnName, column.Ordinal);
                    column.DataTypeNativeDb = columnRow[ColumnAttributes.DataType.ToString()].ToString();

                    if (columnRow[ColumnAttributes.NumericPrecision.ToString()] != DBNull.Value)
                        column.Precision = Convert.ToInt16(columnRow[ColumnAttributes.NumericPrecision.ToString()]);
                    if (columnRow[ColumnAttributes.NumericPrecisionRadix.ToString()] != DBNull.Value)
                        column.Radix = Convert.ToInt16(columnRow[ColumnAttributes.NumericPrecisionRadix.ToString()]);
                    if (columnRow[ColumnAttributes.NumericScale.ToString()] != DBNull.Value)
                        column.Scale = Convert.ToInt16(columnRow[ColumnAttributes.NumericScale.ToString()]);

                    if (_dbMgr.DatabaseType == DatabaseTypeName.Oracle
                        && column.DataTypeNativeDb.ToLower() == "number")
                    {
                        column.DataTypeDotNet = _dbMgr.DbProvider.GetDotNetDataTypeFromNativeDataType(
                                                                            column.DataTypeNativeDb
                                                                            , column.Precision
                                                                            , column.Scale);
                        column.DataTypeGenericDb = _dbMgr.DbProvider.GetGenericDbTypeFromNativeDataType(
                                                                            column.DataTypeNativeDb
                                                                            , column.Precision
                                                                            , column.Scale);
                        column.DataTypeDataColumn = "System.Decimal";
                    }
                    else
                    {
                        column.DataTypeDotNet = column.DataTypeDataColumn = _dbMgr.DbProvider.GetDotNetDataTypeFromNativeDataType(
                                                                                column.DataTypeNativeDb);
                        column.DataTypeGenericDb = _dbMgr.DbProvider.GetGenericDbTypeFromNativeDataType(
                                                                            column.DataTypeNativeDb);
                    }

                    column.IsNullable = Convert.ToBoolean(columnRow[ColumnAttributes.IsNullable.ToString()]);
                    column.IsAutoGenerated = Convert.ToBoolean(columnRow[ColumnAttributes.IsIdentity.ToString()]);
                    column.HasDefault = columnRow[ColumnAttributes.ColumnDefault.ToString()] == DBNull.Value ? false : true;
                    if (columnRow[ColumnAttributes.CharacterMaximumLength.ToString()] != DBNull.Value)
                        column.MaxLength = Convert.ToInt32(columnRow[ColumnAttributes.CharacterMaximumLength.ToString()]);

                    table.Row.Add(column.Ordinal, column.ColumnName);
                    // add to column cache
                    _columnCache.Set(column.FullyQualifiedName, column);
                }
                catch (Exception exc)
                {
                    string msg = string.Format("Schema: {0}; Table: {1}; Column:{2}"
                            , table.SchemaName
                            , table.TableName
                            , columnRow[ColumnAttributes.ColumnName.ToString()]);
                    throw new ExceptionMgr(this.ToString(), new ApplicationException(
                                    string.Format(Global.Constants.FormatError_UnhandledException
                                        , msg, exc)));
                }
            }
            return table;
        }
    }
}
