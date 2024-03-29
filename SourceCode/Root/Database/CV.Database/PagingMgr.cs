﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Dynamic;

using CV.Global;
using CV.Database.Provider;

namespace CV.Database
{
    /// <summary>
    /// Paging Manager - This class encapsulates the functionality to retreive result set data
    /// from the database in tunable (pageSize) buffers (pages) with consistent, efficient
    /// access times for both forward and backward directions as well as first and last pages.
    /// <para>
    /// The functionality requires that queries use indexes, specifically unique indexes.
    /// The indexes are necessary for efficient data access therefore the class will 
    /// verify their existence and throw an exception if they are not defined properly.
    /// </para>
    /// <para>
    /// The queries written by the class (or passed in by the caller), must be written so
    /// that they have an execution plan that utilizes the index.  Once the page size has
    /// been fetched the query is stopped and the resources released.
    /// </para>
    /// <para>
    /// In addition, the result set of the queries must include the columns of the index
    /// used in the query, so that the class will have the pointers (values of the keys)
    /// to use in the subsequent query (>= for next;  etc).  The first and last page
    /// queries do not require key values so they can be different commands.  If there were
    /// 'magic values' (such as 0) then the same query can be used for next and first as well 
    /// as previous and last.
    /// </para>
    /// <para>
    /// The page size can be defined once at construction and remain consistent for all 
    /// subsequent queries; or it can be overwritten on each subsequent call for getting a page.
    /// To do this, the class adds a parameter to the dbCommands. (Constants.PageSize)
    /// </para>
    /// </summary>
    /// <remarks>
    /// NOTE: This class does NOT perform a count of the result set because that would defeat
    /// the purpose of scalability.  Therefore it is not possible to know how many
    /// pages there are/will be in a result set.  In addition, the class does not provide
    /// an interface for returning the 'nth page as this would also not be efficient.
    /// You can however choose to calculate a rowcount and perform some heuristic to simulate
    /// the key of the n'th page.
    /// 
    /// <para>Sample Paging SQL for SqlServer</para>
    /// 
    /// <para> Get First Page </para>
    /// 
    /// <para> SET ROWCOUNT @PAGESIZE</para>
    /// <para> SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID, T1.APPSYNCHTIME, T1.APPLOCALTIME, T1.DBSERVERTIME, T1.APPSEQUENCENAME</para>
    /// <para> , T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM B1.TESTSEQUENCE T1</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME ASC ,T1.APPSEQUENCEID ASC</para>
    /// 
    /// <para> Get Last Page </para>
    /// 
    /// <para> SET ROWCOUNT @PAGESIZE</para>
    /// <para> SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID, T1.APPSYNCHTIME, T1.APPLOCALTIME, T1.DBSERVERTIME, T1.APPSEQUENCENAME</para>
    /// <para> , T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM B1.TESTSEQUENCE T1</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME DESC ,T1.APPSEQUENCEID DESC</para>
    /// 
    /// <para> Get Previous Page </para>
    /// 
    /// <para> SET ROWCOUNT @PAGESIZE</para>
    /// <para> SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID, T1.APPSYNCHTIME, T1.APPLOCALTIME, T1.DBSERVERTIME, T1.APPSEQUENCENAME</para>
    /// <para> , T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM B1.TESTSEQUENCE T1</para>
    /// <para> WHERE T1.APPSEQUENCENAME &lt; @APPSEQUENCENAME OR (T1.APPSEQUENCENAME = @APPSEQUENCENAME AND T1.APPSEQUENCEID &lt; @APPSEQUENCEID)</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME DESC ,T1.APPSEQUENCEID DESC</para>
    /// 
    /// <para> Get Next Page </para>
    /// 
    /// <para> SET ROWCOUNT @PAGESIZE</para>
    /// <para> SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID, T1.APPSYNCHTIME, T1.APPLOCALTIME, T1.DBSERVERTIME, T1.APPSEQUENCENAME</para>
    /// <para> , T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM B1.TESTSEQUENCE T1</para>
    /// <para> WHERE T1.APPSEQUENCENAME &lt; @APPSEQUENCENAME OR (T1.APPSEQUENCENAME = @APPSEQUENCENAME AND T1.APPSEQUENCEID &lt; @APPSEQUENCEID)</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME ASC ,T1.APPSEQUENCEID ASC</para>
    /// 
    /// <para>Sample Paging SQL for Oracle</para>
    /// 
    /// <para> Get First Page </para>
    /// 
    /// <para> OPEN :REFCURSOR FOR SELECT * FROM (SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID, T1.APPSYNCHTIME, T1.APPLOCALTIME</para>
    /// <para> , T1.DBSERVERTIME, T1.APPSEQUENCENAME, T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM   B1.TESTSEQUENCE T1</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME ASC ,T1.APPSEQUENCEID ASC )</para>
    /// <para> WHERE (:PAGESIZE = 0 OR (:PAGESIZE > 0 AND ROWNUM &lt;= :PAGESIZE)) ;</para>
    /// 
    /// <para> Get Last Page </para>
    /// 
    /// <para> OPEN :REFCURSOR FOR SELECT * FROM (SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID, T1.APPSYNCHTIME, T1.APPLOCALTIME</para>
    /// <para> , T1.DBSERVERTIME, T1.APPSEQUENCENAME, T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM   B1.TESTSEQUENCE T1</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME DESC ,T1.APPSEQUENCEID DESC )</para>
    /// <para> WHERE (:PAGESIZE = 0 OR (:PAGESIZE > 0 AND ROWNUM &lt;= :PAGESIZE)) ;</para>
    /// 
    /// <para> Get Previous Page </para>
    /// 
    /// <para> OPEN :REFCURSOR FOR SELECT * FROM (SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID, T1.APPSYNCHTIME, T1.APPLOCALTIME</para>
    /// <para> , T1.DBSERVERTIME, T1.APPSEQUENCENAME, T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM   B1.TESTSEQUENCE T1</para>
    /// <para> WHERE T1.APPSEQUENCENAME &lt; :APPSEQUENCENAME OR (T1.APPSEQUENCENAME = :APPSEQUENCENAME AND T1.APPSEQUENCEID &lt; :APPSEQUENCEID)</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME DESC ,T1.APPSEQUENCEID DESC )</para>
    /// <para> WHERE (:PAGESIZE = 0 OR (:PAGESIZE > 0 AND ROWNUM &lt;= :PAGESIZE)) ;</para>
    /// 
    /// <para> Get Next Page </para>
    /// 
    /// <para> OPEN :REFCURSOR FOR SELECT * FROM (SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID, T1.APPSYNCHTIME, T1.APPLOCALTIME</para>
    /// <para> , T1.DBSERVERTIME, T1.APPSEQUENCENAME, T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM   B1.TESTSEQUENCE T1</para>
    /// <para> WHERE T1.APPSEQUENCENAME &lt; :APPSEQUENCENAME OR (T1.APPSEQUENCENAME = :APPSEQUENCENAME AND T1.APPSEQUENCEID &lt; :APPSEQUENCEID)</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME ASC ,T1.APPSEQUENCEID ASC )</para>
    /// <para> WHERE (:PAGESIZE = 0 OR (:PAGESIZE > 0 AND ROWNUM &lt;= :PAGESIZE)) ;</para>
    /// 
    /// <para>Sample Paging SQL for DB2/UDB</para>
    /// 
    /// <para> Get First Page </para>
    /// 
    /// <para> SELECT APPSEQUENCEID, DBSEQUENCEID, APPSYNCHTIME, APPLOCALTIME, DBSERVERTIME, APPSEQUENCENAME, REMARKS</para>
    /// <para> , EXTRADATA  FROM (SELECT X.*, ROW_NUMBER() OVER () AS ROW_NUM FROM (SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID</para>
    /// <para> , T1.APPSYNCHTIME, T1.APPLOCALTIME, T1.DBSERVERTIME, T1.APPSEQUENCENAME, T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM B1.TESTSEQUENCE T1</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME ASC ,T1.APPSEQUENCEID ASC ) X ) Y </para>
    /// <para> WHERE (@PAGESIZE = 0 OR (@PAGESIZE > 0 AND Y.ROW_NUM &lt;= @PAGESIZE))</para>
    /// 
    /// <para> Get Last Page </para>
    /// 
    /// <para> SELECT APPSEQUENCEID, DBSEQUENCEID, APPSYNCHTIME, APPLOCALTIME, DBSERVERTIME, APPSEQUENCENAME, REMARKS</para>
    /// <para> , EXTRADATA  FROM (SELECT X.*, ROW_NUMBER() OVER () AS ROW_NUM FROM (SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID</para>
    /// <para> , T1.APPSYNCHTIME, T1.APPLOCALTIME, T1.DBSERVERTIME, T1.APPSEQUENCENAME, T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM B1.TESTSEQUENCE T1</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME DESC ,T1.APPSEQUENCEID DESC ) X ) Y </para>
    /// <para> WHERE (@PAGESIZE = 0 OR (@PAGESIZE > 0 AND Y.ROW_NUM &lt;= @PAGESIZE))</para>
    /// 
    /// <para> Get Previous Page </para>
    /// 
    /// <para> SELECT APPSEQUENCEID, DBSEQUENCEID, APPSYNCHTIME, APPLOCALTIME, DBSERVERTIME, APPSEQUENCENAME, REMARKS</para>
    /// <para> , EXTRADATA  FROM (SELECT X.*, ROW_NUMBER() OVER () AS ROW_NUM FROM (SELECT T1.APPSEQUENCEID, T1.DBSEQUENCEID</para>
    /// <para> , T1.APPSYNCHTIME, T1.APPLOCALTIME, T1.DBSERVERTIME, T1.APPSEQUENCENAME, T1.REMARKS, T1.EXTRADATA</para>
    /// <para> FROM B1.TESTSEQUENCE T1</para>
    /// <para> WHERE T1.APPSEQUENCENAME &lt; @APPSEQUENCENAME OR (T1.APPSEQUENCENAME = @APPSEQUENCENAME AND T1.APPSEQUENCEID &lt; @APPSEQUENCEID)</para>
    /// <para> ORDER BY T1.APPSEQUENCENAME DESC ,T1.APPSEQUENCEID DESC ) X ) Y </para>
    /// <para> WHERE (@PAGESIZE = 0 OR (@PAGESIZE > 0 AND Y.ROW_NUM &lt;= @PAGESIZE))</para>
    /// </remarks>
    public class PagingMgr
    {
        /// <summary>
        /// Default value for a page size if none is provided
        /// Page size corresponds to the number of rows returned in a query.
        /// </summary>
        public const Int16 CONST_DefaultPageSize = 500;

        DatabaseMgr _dbMgr = null;
        Dictionary<string, string> _pageKeys = null;
        Dictionary<string, object> _pageFirstItem = null;
        Dictionary<string, object> _pageLastItem = null;
        Int16 _pageSize = CONST_DefaultPageSize;
        string _pageSizeParam = null;
        int _paramOffset = 0;
        /// <summary>
        /// Paging Direction Enumeration
        /// </summary>
#pragma warning disable 1591 // disable the xmlComments warning
        public enum PagingDbCmdEnum { First, Last, Next, Previous };
#pragma warning restore 1591 // restore the xmlComments warning
        PagingDbCmdEnum _refreshDirection = PagingDbCmdEnum.First; // default
        bool _isRefresh = false;

        DbCommand _dbCmdFirstPage = null;
        DbCommand _dbCmdLastPage = null;
        DbCommand _dbCmdNextPage = null;
        DbCommand _dbCmdPreviousPage = null;
        List<string> _indexColumns = null;

        /// <summary>
        /// Baseline constructor used internally for setting member variables
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr class pointer</param>
        /// <param name="pageSizeParam"> Parameter name to be used to set page size for page requests</param>
        /// <param name="pageSize">The default page size or null; if pageSize is less than or equal to 0
        /// , then the class default will used (500)</param>
        /// <param name="pagingState">Optional paging state string generated by GetPagingState()</param>
        private PagingMgr(DatabaseMgr dbMgr
                , string pageSizeParam
                , Int16? pageSize
                , string pagingState = null)
        {
            _dbMgr = dbMgr;
            // if page size param is not provided, used default
            _pageSizeParam = !string.IsNullOrEmpty(pageSizeParam) ? pageSizeParam : Constants.PageSize;
            if (pageSize.HasValue && pageSize.Value > 0)
                _pageSize = pageSize.Value;
            // Oracle parameters do not start with a special character, so the name starts at positions 0
            _paramOffset = _dbMgr.DatabaseType == DatabaseTypeName.Oracle ? 0 : 1;
            // initialize the keys for the first/last item in a page
            _pageKeys = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            _pageFirstItem = new Dictionary<string, object>(StringComparer.CurrentCultureIgnoreCase);
            _pageLastItem = new Dictionary<string, object>(StringComparer.CurrentCultureIgnoreCase);
            _indexColumns = new List<string>();

            RestorePagingState(pagingState);
        }

        /// <summary>
        /// Constructor that allows programmer to provide DbCommands for paging forward/backward
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr class pointer</param>
        /// <param name="dbCmdFirstPage">DbCommandfor obtaining first page</param>
        /// <param name="dbCmdLastPage">DbCommand for obtaining last page</param>
        /// <param name="dbCmdNextPage">DbCommand for obtaining next page </param>
        /// <param name="dbCmdPreviousPage">DbCommand for obtaining first page</param>
        /// <param name="pageSizeParam"> Parameter name to be used to set page size for page requests</param>
        /// <param name="pageSize">The default page size or null; if pageSize is less than or equal to 0
        /// , then the class default will used (500)</param>
        /// <param name="pagingState">Optional paging state string generated by GetPagingState()</param>
        public PagingMgr(DatabaseMgr dbMgr
                , DbCommand dbCmdFirstPage
                , DbCommand dbCmdLastPage
                , DbCommand dbCmdNextPage
                , DbCommand dbCmdPreviousPage
                , string pageSizeParam
                , Int16? pageSize
                , string pagingState = null) : this (dbMgr, pageSizeParam, pageSize, pagingState)
        {
            Initialize(dbCmdFirstPage
                    , dbCmdLastPage
                    , dbCmdNextPage
                    , dbCmdPreviousPage);
        }

        /// <summary>
        /// Constructor that accepts a fully qualified table name and builds the paging commands
        /// for forward/backward paging using the PrimaryKey of the table.
        /// <para>
        /// NOTE:
        /// If the table does not have a primary key, this contructor will raise an exception
        /// </para>
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr class pointer</param>
        /// <param name="dbTable">catalog metadata about the table; including which columns to select</param>
        /// <param name="indexColumns">A string array of column names that need to be part of an index; 
        /// NULL if using Primary Key of table</param>
        /// <param name="pageSizeParam"> Parameter name to be used to set page size for page requests</param>
        /// <param name="pageSize">The default page size or null; if pageSize is less than or equal to 0
        /// , then the class default will used (500)</param>
        /// <param name="pagingState">Optional paging state string generated by GetPagingState()</param>
        public PagingMgr(DatabaseMgr dbMgr
                , DbTable dbTable
                , List<string> indexColumns
                , string pageSizeParam
                , Int16? pageSize
                , string pagingState = null)
            : this(dbMgr, pageSizeParam, pageSize, pagingState)
        {
            // Verify that the table has a PrimaryKey or an Index that matches the index column list
            indexColumns = VerifyIndexColumns(dbTable, indexColumns);

            DbCommand dbCmdFirstPage = GetPageDbCmd(PagingDbCmdEnum.First
                    , dbTable
                    , indexColumns);
            DbCommand dbCmdLastPage = GetPageDbCmd(PagingDbCmdEnum.Last
                    , dbTable
                    , indexColumns);
            DbCommand dbCmdNextPage = GetPageDbCmd(PagingDbCmdEnum.Next
                    , dbTable
                    , indexColumns);
            DbCommand dbCmdPreviousPage = GetPageDbCmd(PagingDbCmdEnum.Previous
                    , dbTable
                    , indexColumns);

            Initialize(dbCmdFirstPage
                    , dbCmdLastPage
                    , dbCmdNextPage
                    , dbCmdPreviousPage);
        }

        /// <summary>
        /// Constructor that accepts a fully qualified table name and builds the paging commands
        /// for forward/backward paging using the PrimaryKey of the table.
        /// <para>
        /// NOTE:
        /// If the table does not have a primary key, this contructor will raise an exception
        /// </para>
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr class pointer</param>
        /// <param name="fullyQualifiedTableName">Fully qualified table name to perform paging on</param>
        /// <param name="pageSizeParam"> Parameter name to be used to set page size for page requests</param>
        /// <param name="pageSize">The default page size or null; if pageSize is less than or equal to 0
        /// , then the class default will used (500)</param>
        /// <param name="pagingState">Optional paging state string generated by GetPagingState()</param>
        public PagingMgr(DatabaseMgr dbMgr
                , string fullyQualifiedTableName
                , string pageSizeParam
                , Int16? pageSize
                , string pagingState = null)
            : this(dbMgr
                , dbMgr.DbCatalogGetTable(fullyQualifiedTableName)
                , null
                , pageSizeParam
                , pageSize
                , pagingState)
        {           
        }


        /// <summary>
        /// Constructor that accepts catalog metadata structure for the table and builds the paging commands
        /// for forward/backward paging using the PrimaryKey of the table.
        /// <para>
        /// NOTE:
        /// If the table does not have a primary key, this contructor will raise an exception
        /// </para>
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr class pointer</param>
        /// <param name="dbTable">catalog metadata about the table; including which columns to select</param>
        /// <param name="pageSizeParam"> Parameter name to be used to set page size for page requests</param>
        /// <param name="pageSize">The default page size or null; if pageSize is less than or equal to 0
        /// , then the class default will used (500)</param>
        /// <param name="pagingState">Optional paging state string generated by GetPagingState()</param>
        public PagingMgr(DatabaseMgr dbMgr
                , DbTable dbTable
                , string pageSizeParam
                , Int16? pageSize
                , string pagingState = null)
            : this(dbMgr
                , dbTable
                , null
                , pageSizeParam
                , pageSize
                , pagingState)
        {
        }

        /// <summary>
        /// Constructor that accepts a fully qualified table name and builds the paging commands
        /// for forward/backward paging using the given column list.
        /// <para>
        /// NOTE:
        /// If the table does not have an index (including primary key), that covers the columns 
        /// given to this contructor, it will raise an exception.
        /// </para>
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr class pointer</param>
        /// <param name="fullyQualifiedTableName">Fully qualified table name to perform paging on</param>
        /// <param name="indexColumns">A string array of column names that need to be part of an index</param>
        /// <param name="pageSizeParam"> Parameter name to be used to set page size for page requests</param>
        /// <param name="pageSize">The default page size or null; if pageSize is less than or equal to 0
        /// , then the class default will used (500)</param>
        /// <param name="pagingState">Optional paging state string generated by GetPagingState()</param>
        public PagingMgr(DatabaseMgr dbMgr
                , string fullyQualifiedTableName
                , List<string> indexColumns
                , string pageSizeParam
                , Int16? pageSize
                , string pagingState = null)
            : this(dbMgr
                , dbMgr.DbCatalogGetTable(fullyQualifiedTableName)
                , indexColumns
                , pageSizeParam
                , pageSize
                , pagingState)
        {
        }

        /// <summary>
        /// Uses a DmlMgr to build a PagingMgr.
        /// DmlMgr can represent a select with one or more tables(join) and contain an existing
        /// where condition. The PagingMgr will append to the existing where condition to perform the 4 basic
        /// paging commands: first, last, prev, next.
        /// Order by columns will be ignored.
        /// </summary>
        /// <param name="dbMgr">DatabaseMgr class pointer</param>
        /// <param name="dmlSelect">DmlMgr with table(s) and where condition.</param>
        /// <param name="pageSizeParam"> Parameter name to be used to set page size for page requests</param>
        /// <param name="pageSize">The default page size or null; if pageSize is less than or equal to 0</param>
        /// <param name="pagingState">Optional paging state string generated by GetPagingState()</param>
        public PagingMgr(DatabaseMgr dbMgr
                , DmlMgr dmlSelect
                , string pageSizeParam
                , Int16? pageSize
                , string pagingState = null) : this (dbMgr, pageSizeParam, pageSize, pagingState)
        {
            List<string> indexColumns = new List<string>();
            foreach (DbQualifiedObject<DbIndexColumn> index in dmlSelect.OrderByColumns.Values)
                indexColumns.Add(index.DbObject.ColumnName);

            indexColumns = VerifyIndexColumns(dbMgr.DbCatalogGetTable(
                    dmlSelect.MainTable.SchemaName, dmlSelect.MainTable.TableName), indexColumns);

            DbCommand dbCmdFirstPage = GetPageDbCmd(PagingDbCmdEnum.First
                    , dmlSelect
                    , indexColumns);
            DbCommand dbCmdLastPage = GetPageDbCmd(PagingDbCmdEnum.Last
                    , dmlSelect
                    , indexColumns);
            DbCommand dbCmdNextPage = GetPageDbCmd(PagingDbCmdEnum.Next
                    , dmlSelect
                    , indexColumns);
            DbCommand dbCmdPreviousPage = GetPageDbCmd(PagingDbCmdEnum.Previous
                    , dmlSelect
                    , indexColumns);

            Initialize(dbCmdFirstPage
                    , dbCmdLastPage
                    , dbCmdNextPage
                    , dbCmdPreviousPage);
        }

        
        /// <summary>
        /// Returns the list of index columns used by this instance of the pagingMgr
        /// </summary>
        public List<string> IndexColumns
        {
            get { return _indexColumns; }
        }

        /// <summary>
        /// Function will verify that the given list of index columns is covered by an existing unique index
        /// of the table or the primary key and stores the list in memory.
        /// </summary>
        /// <param name="dbTable">DbTable object</param>
        /// <param name="indexColumns">optional list of column names</param>
        /// <returns>A list of column names of an index to use for the query</returns>
        /// <exception cref="ExceptionEvent">Exception with ExceptionEventCodes.DbTablePrimaryKeyUndefined code</exception>
        /// <exception cref="ExceptionEvent">Exception with ExceptionEventCodes.DbTableIndexNotFound code</exception>
        List<string> VerifyIndexColumns(DbTable dbTable
                , List<string> indexColumns)
        {
            // Verify that the table has a PrimaryKey or an Index that matches the index column list
            if (indexColumns == null
                || indexColumns.Count == 0)
            {
                if (dbTable.PrimaryKey == null
                    || dbTable.PrimaryKey.Count == 0)
                {
                    throw new ExceptionMgr(this.ToString(), new ArgumentNullException(string.Format("Table: {0} was not defined with a PrimaryKey and no alternative key "
                            + "was provided for PagingMgr."
                                , dbTable.FullyQualifiedName)));
                }
                else
                {
                    indexColumns = new List<string>();
                    foreach (byte columnIndex in dbTable.PrimaryKey.Keys)
                    {
                        indexColumns.Add(dbTable.PrimaryKey[columnIndex]);
                        _indexColumns.Add(dbTable.PrimaryKey[columnIndex]);
                    }
                }
            }
            else
            {
                if (!VerifyIndex(dbTable, indexColumns))
                    // we did not find an index that matches the given columns
                    throw new ExceptionMgr(this.ToString(), new ArgumentNullException(string.Format("The table: {0} did not have an index defined that contained the column list: "
                                + "{1} requested for paging."
                                , dbTable.FullyQualifiedName
                                , string.Join(", ", indexColumns))));
                foreach (string indexColumn in indexColumns)
                    _indexColumns.Add(indexColumn);
            }
            
            return indexColumns;
        }

        /// <summary>
        /// Determines if given column list is covered by an index of the given table
        /// </summary>
        /// <param name="dbTable">DbTable object to check</param>
        /// <param name="indexColumns">List of columns that require a covering index</param>
        /// <returns>boolean indicating if given column list is covered by an index of the given table</returns>
        bool VerifyIndex(DbTable dbTable
                , List<string> indexColumns)
        {
            if (dbTable.Indexes.Count > 0)
                foreach (string indexName in dbTable.Indexes.Keys)
                {
                    DbIndex index = dbTable.Indexes[indexName];
                    Int16 matchedColCount = 0;  // number of matched columns

                    for (short indexCol = 0; indexCol < index.Columns.Keys.Count(); indexCol++)
                    {
                        if (index.ColumnOrder[indexCol].ColumnName.ToLower()
                            != indexColumns[indexCol].ToLower())
                            break;
                        else
                        {
                            // once we have found a unique index that is covered
                            // by the requested index column list, we are done
                            // even if the requested index has more columns
                            // they wont affect the sort order because the left most
                            // portion of the index is unique already.
                            if (++matchedColCount == indexColumns.Count)
                                return true;
                        }
                    }
                }
            return false;
        }

        Expression GetPagingWhereClause(PagingDbCmdEnum pagingDbCmd,
            string tableName, List<string> indexColumns)
        {
            Int16 i;
            Int16 columnIndex;
            Expression expr;
            Expression exprAnd;
            Expression exprWhere = null;

            columnIndex = -1;
            foreach (string column in indexColumns)
            {
                columnIndex++;

                if (!_pageKeys.ContainsKey(column))
                    _pageKeys.Add(column, column);

                if (pagingDbCmd == PagingDbCmdEnum.Next || pagingDbCmd == PagingDbCmdEnum.Previous)
                {
                    expr = null;
                    exprAnd = null;

                    // Build AND clause for preceding columns
                    for (i = 0; i < columnIndex; i++)
                    {
                        expr = DbPredicate.CreatePredicatePart(tableName,
                            indexColumns[i], _dbMgr.BuildParameterName(indexColumns[i]), ComparisonOperators.Equals);

                        if (exprAnd == null)
                            exprAnd = expr;
                        else
                            exprAnd = Expression.AndAlso(exprAnd, expr);
                    }

                    // Build AND clause for current column
                    if (pagingDbCmd == PagingDbCmdEnum.Next)
                    {
                        expr = DbPredicate.CreatePredicatePart(tableName,
                                column, _dbMgr.BuildParameterName(column), ComparisonOperators.Greater);
                    }
                    else
                    {
                        expr = DbPredicate.CreatePredicatePart(tableName,
                                column, _dbMgr.BuildParameterName(column), ComparisonOperators.Less);
                    }

                    if (exprAnd == null)
                        exprAnd = expr;
                    else
                        exprAnd = Expression.AndAlso(exprAnd, expr);

                    // Append current expression group as OR clause into the WHERE condition
                    if (exprWhere == null)
                        exprWhere = exprAnd;
                    else
                        exprWhere = Expression.OrElse(exprWhere, exprAnd);
                }
            }

            return exprWhere;
        }

        DbCommand GetPageDbCmd(PagingDbCmdEnum pagingDbCmd
                , DbTable dbTable
                , List<string> indexColumns)
        {
            return GetPageDbCmd(pagingDbCmd,
                    _dbMgr.DbCatalogGetDmlMgr(dbTable.FullyQualifiedName, dbTable.Columns.Keys.ToArray()),
                    indexColumns);
        }

        DbCommand GetPageDbCmd( PagingDbCmdEnum pagingDbCmd
                , DmlMgr dmlSelect
                , List<string> indexColumns )
        {
            Int16 i;
            Int16 columnIndex;
            Expression expr;
            Expression exprAnd;
            Expression exprWhere = null;
            DmlMgr dbTableDml =  new DmlMgr(dmlSelect);
            dbTableDml.OrderByColumns.Clear();

            columnIndex = -1;
            foreach (string column in indexColumns)
            {
                columnIndex++;

                if (!_pageKeys.ContainsKey( column ))
                    _pageKeys.Add( column, column );

                if (pagingDbCmd == PagingDbCmdEnum.First
                        || pagingDbCmd == PagingDbCmdEnum.Next)
                    dbTableDml.AddOrderByColumnAscending(column);
                else
                    dbTableDml.AddOrderByColumnDescending(column);

                if (pagingDbCmd == PagingDbCmdEnum.Next || pagingDbCmd == PagingDbCmdEnum.Previous)
                {
                    expr = null;
                    exprAnd = null;

                    // Build AND clause for preceding columns
                    for (i = 0; i < columnIndex; i++)
                    {
                        expr = DbPredicate.CreatePredicatePart( dbTableDml.MainTable.TableName,
                            indexColumns[ i ], _dbMgr.BuildParameterName( indexColumns[ i ]), ComparisonOperators.Equals );

                        if (exprAnd == null)
                            exprAnd = expr;
                        else
                            exprAnd = Expression.AndAlso( exprAnd, expr );
                    }

                    // Build AND clause for current column
                    if (pagingDbCmd == PagingDbCmdEnum.Next)
                    {
                        expr = DbPredicate.CreatePredicatePart( dbTableDml.MainTable.TableName,
                                column, _dbMgr.BuildParameterName( column ), ComparisonOperators.Greater );
                    }
                    else
                    {
                        expr = DbPredicate.CreatePredicatePart( dbTableDml.MainTable.TableName,
                                column, _dbMgr.BuildParameterName( column ), ComparisonOperators.Less );
                    }

                    if (exprAnd == null)
                        exprAnd = expr;
                    else
                        exprAnd = Expression.AndAlso( exprAnd, expr );

                    // Append current expression group as OR clause into the WHERE condition
                    if (exprWhere == null)
                        exprWhere = exprAnd;
                    else
                        exprWhere = Expression.OrElse( exprWhere, exprAnd );
                }
            }

            if (exprWhere != null && dbTableDml._whereCondition == null)
                dbTableDml.SetWhereCondition( exprWhere );
            else if(exprWhere != null)
                dbTableDml.AddToWhereCondition(ExpressionType.AndAlso, exprWhere );

            return _dbMgr.BuildSelectDbCommand( dbTableDml, _pageSizeParam );
        }

        void Initialize(DbCommand dbCmdFirstPage
                , DbCommand dbCmdLastPage
                , DbCommand dbCmdNextPage
                , DbCommand dbCmdPreviousPage)
        {
            _dbCmdFirstPage = dbCmdFirstPage;
            _dbCmdLastPage = dbCmdLastPage;
            _dbCmdPreviousPage = dbCmdPreviousPage;
            _dbCmdNextPage = dbCmdNextPage;
        }

        /// <summary>
        /// Stores the key item values from the given data buffer
        /// </summary>
        /// <param name="newPage">A DataTable object of new data</param>
        /// <returns>The given data buffer</returns>
        DataTable ProcessNewPage(DataTable newPage)
        {
            // only if there was data in the buffer
            if (newPage != null
                && newPage.Rows.Count > 0)
            {
                // set the pageItem keys
                SetKeyItemValues(_pageFirstItem, newPage.Rows[0]);
                SetKeyItemValues(_pageLastItem, newPage.Rows[newPage.Rows.Count - 1]);
            }
            _isRefresh = false;
            return newPage;
        }

        /// <summary>
        /// Sets the key item values (column values) for the page based upon the given DataRow of data
        /// </summary>
        /// <param name="pageItem">The page's collection of key value pairs</param>
        /// <param name="itemRow">The new DataRow of data</param>
        void SetKeyItemValues(Dictionary<string, object> pageItem, DataRow itemRow)
        {
            foreach (string pageKey in _pageKeys.Keys)
                pageItem[pageKey] = itemRow[pageKey];
        }

        /// <summary>
        /// Sets the key item values (column values) for the page based upon the given entity object
        /// and property information from reflection
        /// </summary>
        /// <param name="pageItem">The page's collection of key value pairs</param>
        /// <param name="propertyDic">Dictionary of property name and reflection info</param>
        /// <param name="rowObject">Entity Framework entity object</param>
        void SetKeyItemValues(Dictionary<string, object> pageItem, Dictionary<string, PropertyInfo> propertyDic,
                object rowObject)
        {
            foreach (string pageKey in _pageKeys.Keys)
                pageItem[pageKey] = propertyDic[pageKey].GetValue(rowObject, null);
        }

        /// <summary>
        /// Sets the key item values (column values) for the page based upon the given 
        /// dynamic object or entity object (from Entity Framework)
        /// </summary>
        /// <param name="pageItem">The page's collection of key value pairs</param>
        /// <param name="rowObject">Dynamic object or Entity Framework entity object</param>
        void SetKeyItemValues(Dictionary<string, object> pageItem, object rowObject)
        {
            if (rowObject is IDynamicMetaObjectProvider)
            {
                IDictionary<string, object> dynObject = (IDictionary<string, object>)(dynamic)rowObject;
                foreach (string pageKey in _pageKeys.Keys)
                    pageItem[pageKey] = dynObject[pageKey];
            }
            else
            {
                var propertyDic = rowObject.GetType().GetProperties().ToDictionary(kv => kv.Name, kv => kv,
                        StringComparer.CurrentCultureIgnoreCase);

                SetKeyItemValues(pageItem, propertyDic, rowObject);
            }
        }

        /// <summary>
        /// Returns a new page of data based upon the given direction and page size
        /// </summary>
        /// <param name="pagingDirection">Enumeration: first, last, previous, next</param>
        /// <param name="pageSize">Optional page size; null indicates use default</param>
        /// <returns>New page of data as DataTable</returns>
        public DataTable GetPage(PagingDbCmdEnum pagingDirection, Int16? pageSize = null)
        {
            switch (pagingDirection)
            {
                case PagingDbCmdEnum.First:
                    return pageSize == null ? GetFirstPage() : GetFirstPage(pageSize.Value);
                case PagingDbCmdEnum.Next:
                    return pageSize == null ? GetNextPage() : GetNextPage(pageSize.Value);
                case PagingDbCmdEnum.Previous:
                    return pageSize == null ? GetPreviousPage() : GetPreviousPage(pageSize.Value);
                case PagingDbCmdEnum.Last:
                    return pageSize == null ? GetLastPage() : GetLastPage(pageSize.Value);
            }

            return null;
        }

        /// <summary>
        /// Returns updated buffer for the current direction, key settings, and optional pageSize parameter.
        /// </summary>
        /// <param name="pageSize">Optional override to default buffer page size</param>
        /// <returns>Updated page of data as DataTable</returns>
        public DataTable RefreshPage(short? pageSize = null)
        {
            _isRefresh = true;
            return GetPage(_refreshDirection, pageSize);
        }

        /// <summary>
        /// GetPage returns the buffer for the given paging direction. This function can be called with an entity or a specific
        /// concrete type. This function can also be called with "dynamic" for the type T in which case the dynamic
        /// Expando objects are returned. The newest ASP.NET MVC 3 has new HTML controls such as WebGrid which can not
        /// be used with DataTable. So sometimes instead of creating concrete classes for every resultset one can use
        /// the dynamic objects. e.g. GetPage&lt;Employee&gt; or GetPage&lt;dynamic&gt;
        /// </summary>
        /// <param name="pagingDirection">Enumeration: first, last, previous, next</param>
        /// <param name="pageSize">Optional override to default buffer page size</param>
        /// <returns>IEnumerable collection of type T</returns>
        public IEnumerable<T> GetPage<T>(PagingDbCmdEnum pagingDirection, Int16? pageSize = null) where T : new()
        {
            switch (pagingDirection)
            {
                case PagingDbCmdEnum.First:
                    return pageSize == null ? GetFirstPage<T>() : GetFirstPage<T>(pageSize.Value);
                case PagingDbCmdEnum.Next:
                    return pageSize == null ? GetNextPage<T>() : GetNextPage<T>(pageSize.Value);
                case PagingDbCmdEnum.Previous:
                    return pageSize == null ? GetPreviousPage<T>() : GetPreviousPage<T>(pageSize.Value);
                case PagingDbCmdEnum.Last:
                    return pageSize == null ? GetLastPage<T>() : GetLastPage<T>(pageSize.Value);
            }

            return null;
        }

        /// <summary>
        /// Returns the page size value for being used
        /// </summary>
        public Int16 PageSize
        {
            get { return _pageSize; }
        }

        /// <summary>
        /// Returns the first pagesize buffer of data
        /// </summary>
        /// <param name="pageSize">The size of the buffer to return. Must be greater than 0 or default will be used</param>
        /// <returns>First buffer of data</returns>
        public DataTable GetFirstPage(Int16 pageSize)
        {
            _refreshDirection = PagingDbCmdEnum.First;
            // set parameter values
            if (!_isRefresh)
                _dbCmdFirstPage.Parameters[_dbMgr.BuildParameterName(_pageSizeParam)].Value = pageSize > 0 ? pageSize : _pageSize;
            return ProcessNewPage(_dbMgr.ExecuteDataSet(_dbCmdFirstPage, null, null).Tables[0]);
        }

        /// <summary>
        /// Returns the first pagesize buffer of data. This function can be called with an entity or a specific
        /// concrete type. This function can also be called with "dynamic" for the type T in which case the dynamic
        /// Expando objects are returned. The newest ASP.NET MVC 3 has new HTML controls such as WebGrid which can not
        /// be used with DataTable. So sometimes instead of creating concrete classes for every resultset one can use
        /// the dynamic objects. e.g. GetFirstPage&lt;Employee&gt; or GetFirstPage&lt;dynamic&gt;
        /// </summary>
        /// <param name="pageSize">The size of the buffer to return. Must be greater than 0 or default will be used</param>
        /// <returns>First buffer of data</returns>
        public IEnumerable<T> GetFirstPage<T>(Int16 pageSize) where T : new()
        {
            _refreshDirection = PagingDbCmdEnum.First;
            // set parameter values
            if (!_isRefresh)
                _dbCmdFirstPage.Parameters[_dbMgr.BuildParameterName(_pageSizeParam)].Value = pageSize > 0 ? pageSize : _pageSize;

            // If called with GetFirstPage<dynamic>() OR GetFirstPage(Object) - returns the ExpandoObject
            // The BaseType is null when the type is Object or dynamic
            IEnumerable<T> ret = null;
            if (typeof(T).BaseType == null)
            {
                ret = (IEnumerable<T>)_dbMgr.ExecuteDynamic(_dbCmdFirstPage, null);
                if (ret.Count() > 0)
                {
                    SetKeyItemValues(_pageFirstItem, ret.First());
                    SetKeyItemValues(_pageLastItem, ret.Last());
                }
            }
            else
            {
                ret = _dbMgr.ExecuteCollection<T>(_dbCmdFirstPage, null, GetCollectionAndProcess<T>, null);
            }
            _isRefresh = false;
            return ret;
        }

        /// <summary>
        /// Returns the first pagesize buffer of data
        /// </summary>
        /// <returns>First buffer of data</returns>
        public DataTable GetFirstPage()
        {
            return GetFirstPage(_pageSize);
        }

        /// <summary>
        /// Returns the first pagesize buffer of data
        /// </summary>
        /// <returns>First buffer of data</returns>
        public IEnumerable<T> GetFirstPage<T>() where T : new()
        {
            return GetFirstPage<T>(_pageSize);
        }

        /// <summary>
        /// Returns the last pagesize buffer of data
        /// </summary>
        /// <param name="pageSize"></param>
        /// <returns>last buffer of data</returns>
        public DataTable GetLastPage(Int16 pageSize)
        {
            _refreshDirection = PagingDbCmdEnum.Last;
            // set parameter values
            if (!_isRefresh)
                _dbCmdLastPage.Parameters[_dbMgr.BuildParameterName(_pageSizeParam)].Value = pageSize > 0 ? pageSize : _pageSize;
            return ProcessNewPage(_dbMgr.ExecuteReader(_dbCmdLastPage, null, GetReverseOrderDataTable));
        }

        /// <summary>
        /// Returns the last pagesize buffer of data
        /// </summary>
        /// <param name="pageSize"></param>
        /// <returns>last buffer of data</returns>
        public IEnumerable<T> GetLastPage<T>(Int16 pageSize) where T : new()
        {
            _refreshDirection = PagingDbCmdEnum.Last;
            // set parameter values
            if (!_isRefresh)
                _dbCmdLastPage.Parameters[_dbMgr.BuildParameterName(_pageSizeParam)].Value = pageSize > 0 ? pageSize : _pageSize;

            // If called with GetLastPage<dynamic>() OR GetLastPage(Object) - returns the ExpandoObject
            // The BaseType is null when the type is Object or dynamic
            IEnumerable<T> ret = null;
            if (typeof(T).BaseType == null)
            {
                ret = (IEnumerable<T>)_dbMgr.ExecuteDynamic(_dbCmdLastPage, null);
                ret = ret.Reverse();
                if (ret.Count() > 0)
                {
                    SetKeyItemValues(_pageFirstItem, ret.First());
                    SetKeyItemValues(_pageLastItem, ret.Last());
                }
            }
            else
            {
                ret = _dbMgr.ExecuteCollection<T>(_dbCmdLastPage, null, ReverseCollectionAndProcess<T>, null);
            }
            _isRefresh = false;
            return ret;
        }

        /// <summary>
        /// Returns the last pagesize buffer of data
        /// </summary>
        /// <returns>Last buffer of data</returns>
        public DataTable GetLastPage()
        {
            return GetLastPage(_pageSize);
        }

        
        /// <summary>
        /// Returns the last pagesize buffer of data
        /// </summary>
        /// <returns>last buffer of data</returns>
        public IEnumerable<T> GetLastPage<T>() where T : new()
        {
            return GetLastPage<T>(_pageSize);
        }

        /// <summary>
        /// Returns the next pagesize buffer of data given the values found for the
        /// keys of the last row of the current buffer as maintained by caller.
        /// </summary>
        /// <param name="lastRow">The last datarow of the current buffer</param>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Next buffer of data</returns>
        public DataTable GetNextPage(DataRow lastRow, Int16 pageSize)
        {
            SetKeyItemValues(_pageLastItem, lastRow);
            return GetNextPage(_pageLastItem, pageSize);
        }

        /// <summary>
        /// Returns the next pagesize buffer of data given the values found for the
        /// keys of the last row of the current buffer as maintained by caller.
        /// </summary>
        /// <param name="lastObject">The last object in the current collection.</param>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Next buffer of data</returns>
        public IEnumerable<T> GetNextPage<T>(T lastObject, Int16 pageSize) where T : new()
        {
            // set parameter values
            SetKeyItemValues(_pageLastItem, lastObject);
            return GetNextPage<T>(_pageLastItem, pageSize);
        }

        /// <summary>
        /// Returns the next pagesize buffer of data given the values found for the
        /// keys of the last collection of the current buffer as maintained by caller.
        /// </summary>
        /// <param name="pageLastItem">A collection containing the keys and the values 
        /// for the last item of the most recent page retrieved.</param>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Next buffer of data</returns>
        public DataTable GetNextPage(Dictionary<string, object> pageLastItem, Int16 pageSize)
        {
            if (_pageLastItem != null && _pageLastItem.Count > 0)
            {
                _refreshDirection = PagingDbCmdEnum.Next;
                if (!_isRefresh)
                {
                    foreach (string pageKey in _pageKeys.Keys)
                        _dbCmdNextPage.Parameters[_dbMgr.BuildParameterName(pageKey)].Value = pageLastItem[pageKey];
                    _dbCmdNextPage.Parameters[_dbMgr.BuildParameterName(_pageSizeParam)].Value = pageSize > 0 ? pageSize : _pageSize;
                }
                return ProcessNewPage(_dbMgr.ExecuteDataSet(_dbCmdNextPage, null, null).Tables[0]);
            }
            else return GetFirstPage(pageSize);
        }

        /// <summary>
        /// Returns the next pagesize buffer of data given the values found for the
        /// keys of the last collection of the current buffer as maintained by caller.
        /// </summary>
        /// <param name="pageLastItem">A collection containing the keys and the values 
        /// for the last item of the most recent page retrieved.</param>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Next buffer of data</returns>
        public IEnumerable<T> GetNextPage<T>(Dictionary<string, object> pageLastItem, Int16 pageSize) where T : new()
        {
            _refreshDirection = PagingDbCmdEnum.Next;
            if (_pageLastItem != null && _pageLastItem.Count > 0)
            {
                if (!_isRefresh)
                {
                    foreach (string pageKey in _pageKeys.Keys)
                        _dbCmdNextPage.Parameters[_dbMgr.BuildParameterName(pageKey)].Value = pageLastItem[pageKey];
                    _dbCmdNextPage.Parameters[_dbMgr.BuildParameterName(_pageSizeParam)].Value = pageSize > 0 ? pageSize : _pageSize;
                }

                // If called with GetNextPage<dynamic>() OR GetNextPage(Object) - returns the ExpandoObject
                // The BaseType is null when the type is Object or dynamic
                IEnumerable<T> ret = null;
                if (typeof(T).BaseType == null)
                {
                    ret = (IEnumerable<T>)_dbMgr.ExecuteDynamic(_dbCmdNextPage, null);
                    if (ret.Count() > 0)
                    {
                        SetKeyItemValues(_pageFirstItem, ret.First());
                        SetKeyItemValues(_pageLastItem, ret.Last());
                    }
                }
                else
                {
                    ret = _dbMgr.ExecuteCollection<T>(_dbCmdNextPage, null, GetCollectionAndProcess<T>, null);
                }
                _isRefresh = false;
                return ret;
            }
            else return GetFirstPage<T>(pageSize);
        }

        /// <summary>
        /// Returns the next pagesize buffer of data given the values found for the
        /// keys of the last row of the current buffer.
        /// </summary>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Next buffer of data</returns>
        public DataTable GetNextPage(Int16 pageSize)
        {
            return GetNextPage(_pageLastItem, pageSize);
        }

        /// <summary>
        /// Returns the next pagesize buffer of data given the values found for the
        /// keys of the last row of the current buffer and the current page size.
        /// </summary>
        /// <returns>Next buffer of data</returns>
        public DataTable GetNextPage()
        {
            return GetNextPage(_pageLastItem, _pageSize);
        }

        /// <summary>
        /// Returns the next pagesize buffer of data given the values found for the
        /// keys of the last row of the current buffer.
        /// </summary>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Next buffer of data</returns>
        public IEnumerable<T> GetNextPage<T>(Int16 pageSize) where T : new()
        {
            return GetNextPage<T>(_pageLastItem, pageSize);
        }

        /// <summary>
        /// Returns the next pagesize buffer of data given the values found for the
        /// keys of the last row of the current buffer and the current page size.
        /// </summary>
        /// <returns>Next buffer of data</returns>
        public IEnumerable<T> GetNextPage<T>() where T : new()
        {
            return GetNextPage<T>(_pageLastItem, _pageSize);
        }

        /// <summary>
        /// Returns the previous pagesize buffer of data given the values found for the
        /// keys of the first row of the current buffer as maintained by caller.
        /// </summary>
        /// <param name="firstRow">The first datarow of the current buffer</param>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Previous buffer of data</returns>
        public DataTable GetPreviousPage(DataRow firstRow, Int16 pageSize)
        {
            SetKeyItemValues(_pageFirstItem, firstRow);
            return GetPreviousPage(_pageFirstItem, pageSize);
        }

        /// <summary>
        /// Returns the previous pagesize buffer of data given the values found for the
        /// keys of the first row of the current buffer as maintained by caller.
        /// </summary>
        /// <param name="firstObject">The first object in the current collection</param>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Previous buffer of data</returns>
        public IEnumerable<T> GetPreviousPage<T>(T firstObject, Int16 pageSize) where T : new()
        {
            SetKeyItemValues(_pageFirstItem, firstObject);
            return GetPreviousPage<T>(_pageFirstItem, pageSize);
        }

        /// <summary>
        /// Returns the previous pagesize buffer of data given the values found for the
        /// keys of the first collection of the current buffer as maintained by caller.
        /// </summary>
        /// <param name="pageFirstItem">A collection containing the keys and the values 
        /// for the first item of the most recent page retrieved.</param>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Previous buffer of data</returns>
        public DataTable GetPreviousPage(Dictionary<string, object> pageFirstItem, Int16 pageSize)
        {
            if (_pageFirstItem != null && _pageFirstItem.Count > 0)
            {
                _refreshDirection = PagingDbCmdEnum.Previous;
                if (!_isRefresh)
                {
                    foreach (string pageKey in _pageKeys.Keys)
                        _dbCmdPreviousPage.Parameters[_dbMgr.BuildParameterName(pageKey)].Value = pageFirstItem[pageKey];
                    _dbCmdPreviousPage.Parameters[_dbMgr.BuildParameterName(_pageSizeParam)].Value
                            = pageSize > 0 ? pageSize : _pageSize;
                }
                return ProcessNewPage(_dbMgr.ExecuteReader(_dbCmdPreviousPage, null, GetReverseOrderDataTable));
            }
            else return GetLastPage(pageSize);
        }

        /// <summary>
        /// Returns the previous pagesize buffer of data given the values found for the
        /// keys of the first collection of the current buffer as maintained by caller.
        /// </summary>
        /// <param name="pageFirstItem">A collection containing the keys and the values 
        /// for the first item of the most recent page retrieved.</param>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Previous buffer of data</returns>
        public IEnumerable<T> GetPreviousPage<T>(Dictionary<string, object> pageFirstItem, Int16 pageSize) where T : new()
        {
            _refreshDirection = PagingDbCmdEnum.Previous;
            if (_pageFirstItem != null && _pageFirstItem.Count > 0)
            {
                if (!_isRefresh)
                {
                    foreach (string pageKey in _pageKeys.Keys)
                        _dbCmdPreviousPage.Parameters[_dbMgr.BuildParameterName(pageKey)].Value = pageFirstItem[pageKey];
                    _dbCmdPreviousPage.Parameters[_dbMgr.BuildParameterName(_pageSizeParam)].Value
                            = pageSize > 0 ? pageSize : _pageSize;
                }

                // If called with GetPreviousPage<dynamic>() OR GetPreviousPage(Object) - returns the ExpandoObject
                // The BaseType is null when the type is Object or dynamic
                IEnumerable<T> ret = null;
                if (typeof(T).BaseType == null)
                {
                    ret = (IEnumerable<T>)_dbMgr.ExecuteDynamic(_dbCmdPreviousPage, null);
                    ret = ret.Reverse();
                    if (ret.Count() > 0)
                    {
                        SetKeyItemValues(_pageFirstItem, ret.First());
                        SetKeyItemValues(_pageLastItem, ret.Last());
                    }
                }
                else
                {
                    ret = _dbMgr.ExecuteCollection<T>(_dbCmdPreviousPage, null, ReverseCollectionAndProcess<T>, null);
                }
                _isRefresh = false;
                return ret;
            }
            else return GetLastPage<T>(pageSize);
        }

        /// <summary>
        /// Returns the previous pagesize buffer of data given the values found for the
        /// keys of the first row of the current buffer.
        /// </summary>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Previous buffer of data</returns>
        public DataTable GetPreviousPage(Int16 pageSize)
        {
            return GetPreviousPage(_pageFirstItem, pageSize);
        }

        /// <summary>
        /// Returns the previous pagesize buffer of data given the values found for the
        /// keys of the first row of the current buffer and the current page szie.
        /// </summary>
        /// <returns>Previous buffer of data</returns>
        public IEnumerable<T> GetPreviousPage<T>() where T : new()
        {
            return GetPreviousPage<T>(_pageFirstItem, _pageSize);
        }

        /// <summary>
        /// Returns the previous pagesize buffer of data given the values found for the
        /// keys of the first row of the current buffer.
        /// </summary>
        /// <param name="pageSize">The size of the buffer to return</param>
        /// <returns>Previous buffer of data</returns>
        public IEnumerable<T> GetPreviousPage<T>(Int16 pageSize) where T : new()
        {
            return GetPreviousPage<T>(_pageFirstItem, pageSize);
        }

        /// <summary>
        /// Returns the previous pagesize buffer of data given the values found for the
        /// keys of the first row of the current buffer and the current page szie.
        /// </summary>
        /// <returns>Previous buffer of data</returns>
        public DataTable GetPreviousPage()
        {
            return GetPreviousPage(_pageFirstItem, _pageSize);
        }

        /// <summary>
        /// Generates a string representing the paging state
        /// </summary>
        /// <returns></returns>
        public string GetPagingState()
        {
            var stateArray = new Dictionary<string,object>[] { _pageFirstItem, _pageLastItem };
            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(Dictionary<string,object>[]));
            
            using(MemoryStream ms = new MemoryStream())
            {
                serializer.WriteObject(ms, stateArray);
                return Convert.ToBase64String(ms.ToArray());
            }
        }

        /// <summary>
        /// Restores the paging state of this instance from the paging state string generated by
        /// calling GetPagingState()
        /// </summary>
        /// <param name="pagingState">paging state string generated by calling GetPagingState()</param>
        public void RestorePagingState(string pagingState)
        {
            if(string.IsNullOrWhiteSpace(pagingState))
                return;

            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(Dictionary<string,object>[]));
            
            using(MemoryStream ms = new MemoryStream(Convert.FromBase64String(pagingState)))
            {
                Dictionary<string,object>[] stateArray = (Dictionary<string,object>[])serializer.ReadObject(ms);
   
                _pageFirstItem = stateArray[0];
                _pageLastItem = stateArray[1];
            }
        }
        /// <summary>
        /// Populates a DataTable with the contents of a dataReader by inserting
        /// each new row at the top of the DataTable (position 0) so that the results
        /// will appear in the reverse order as they were returned.  This was necessary
        /// for backward paging where the result set is in descending order.
        /// </summary>
        /// <param name="dataReader">Open DataReader</param>
        /// <returns>DataTable with data and schema from dataReader</returns>
        DataTable GetReverseOrderDataTable(IDataReader dataReader)
        {
            DataTable dt = GetEmptyTableFromDataReader(dataReader);
            while (dataReader.Read())
            {
                object[] vals = new object[dt.Columns.Count];
                dataReader.GetValues(vals);
                DataRow dr = dt.NewRow();
                dr.ItemArray = vals;
                dt.Rows.InsertAt(dr, 0);
            }
            return dt;
        }

        /// Populates a List with the contents of a dataReader by inserting
        /// each new type T at the top of the collection (position 0) so that the results
        /// will appear in the reverse order as they were returned.  This was necessary
        /// for backward paging where the result set is in descending order.
        /// Last item and first item key values will be stored based on first and last object.
        IEnumerable<T> ReverseCollectionAndProcess<T>(IDataReader rdr, 
                List<KeyValuePair<int, PropertyInfo>> properties) where T : new()
        {
            List<T> items = new List<T>();
            
            while (rdr.Read())
            {
                T obj = new T();

                properties.ForEach(kv => kv.Value.SetValue(obj, DatabaseMgr.GetValueOrNull(
                        Convert.ChangeType(rdr.GetValue(kv.Key), kv.Value.PropertyType)), null));
                items.Insert(0, obj);
            }

            if(items.Count > 0)
            {
                Dictionary<string, PropertyInfo> propertyDic = properties.ToDictionary(kv => kv.Value.Name, kv => kv.Value,
                        StringComparer.CurrentCultureIgnoreCase);

                SetKeyItemValues(_pageFirstItem, propertyDic, items.First());
                SetKeyItemValues(_pageLastItem, propertyDic, items.Last());
            }

            return items;
        }

        /// Populates a List with the contents of a dataReader in the order read.
        /// Last item and first item key values will be stored based on first and last object.
        IEnumerable<T> GetCollectionAndProcess<T>(IDataReader rdr, 
                List<KeyValuePair<int, PropertyInfo>> properties) where T : new()
        {
            List<T> items = new List<T>();
            
            while (rdr.Read())
            {
                T obj = new T();

                properties.ForEach(kv => kv.Value.SetValue(obj, DatabaseMgr.GetValueOrNull(
                        Convert.ChangeType(rdr.GetValue(kv.Key), kv.Value.PropertyType)), null));
                items.Add(obj);
            }

            if(items.Count > 0)
            {
                Dictionary<string, PropertyInfo> propertyDic = properties.ToDictionary(kv => kv.Value.Name, kv => kv.Value,
                    StringComparer.CurrentCultureIgnoreCase);

                SetKeyItemValues(_pageFirstItem, propertyDic, items.First());
                SetKeyItemValues(_pageLastItem, propertyDic, items.Last());
            }

            return items;
        }

        /// <summary>
        /// Returns an empty data table with schema defined from the dataReader
        /// </summary>
        /// <param name="dataReader">Open data reader</param>
        /// <returns>Empty data table with schema defined</returns>
        DataTable GetEmptyTableFromDataReader(IDataReader dataReader)
		{	
			DataTable dt = new DataTable();
            if (dataReader != null)
                for (int i = 0; i < dataReader.FieldCount; ++i)
                    dt.Columns.Add(dataReader.GetName(i), dataReader.GetFieldType(i));
			return dt;
		}

    }

    /// <summary>
    /// This class helps to enumerator over rows in each page of the PagingMgr. It automatically fetch the next page
    /// when the enuemration is past the last row in the current page.
    /// </summary>
    public class PagingMgrEnumerator<T> : IEnumerator<T> where T : new()
    {
        PagingMgr _pagingMgr = null;
        IEnumerator<T> _currentPage = null;
        bool _endOfData = false;
        //?? bool _reverse = false; //?? NOT implemented yet

        /// <summary>
        /// Constructor needs the DbPagingManager which needs to be enumerated
        /// </summary>
        public PagingMgrEnumerator(PagingMgr pagingMgr)
        {
            _pagingMgr = pagingMgr;
        }

        /// <summary>
        /// GetEnumerator returns the this. This function is needed for using this class in "foreach"
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
            return this;
        }

        /// <summary>
        /// Returns the current row in the current page.
        /// </summary>
        public T Current
        {
            get
            {
                if (_endOfData)
                    throw new InvalidOperationException("End of Data reached.");
                if (_currentPage == null)
                    throw new InvalidOperationException("Enumeration has not started. Call MoveNext.");
                return _currentPage.Current;
            }
        }

        /// <summary>
        /// Disposes the current page.
        /// </summary>
        public void Dispose()
        {
            if (_currentPage != null)
                _currentPage.Dispose();
        }

        /// <summary>
        /// Current is returned as an object
        /// </summary>
        object System.Collections.IEnumerator.Current
        {
            get { return Current; }
        }

        /// <summary>
        /// Move the pointer to the next row in the current page. If end of data reached in the current page than
        /// fetches next page from the DbPagingManager and put the pointer to the first row of the next page.
        /// 
        /// It always starts with the First page in the paging manager to the next pages.
        /// </summary>
        /// <returns>False if end of the data is reached else return true.</returns>
        public bool MoveNext()
        {
            // Move the pointer in the current page - if End of Data reached or first time called
            if (_currentPage == null || !_currentPage.MoveNext())
            {
                // Get the next page from the paging manager
                IEnumerable<T> newPage = _currentPage == null ? _pagingMgr.GetFirstPage<T>() : _pagingMgr.GetNextPage<T>();

                // If the next page has data then make that as the current page
                if (newPage.Count() > 0)
                {
                    if (_currentPage != null) _currentPage.Dispose();
                    _currentPage = newPage.GetEnumerator();
                    return MoveNext();  // Move to next in the new enumerator
                }
                else
                {
                    // If the next page has no data than return false to signify end of data reached
                    _endOfData = true;
                    return false;
                }
            }

            // There is more data available in current
            return true;
        }

        /// <summary>
        /// Reset the pointer to the current page. It does not change any state in the DbPagingManager.
        /// </summary>
        public void Reset()
        {
            _currentPage = null;
            _endOfData = false;
        }
    }
}
