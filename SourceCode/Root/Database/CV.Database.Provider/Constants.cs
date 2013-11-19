using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CV.Database.Provider
{
    /// <summary>
    /// Metadata about columns used in indexes. 
    /// </summary>
    public struct DbIndexColumnMetaData
    {
        /// <summary>
        /// The column's name
        /// </summary>
        public string ColumnName;
        /// <summary>
        /// Used for functional based indexes
        /// </summary>
        public string ColumnFunction;
        /// <summary>
        /// Indicates if the column is sorted descending
        /// </summary>
        public bool IsDescending;
    }

    /// <summary>
    /// Contains the meta data regarding table indexes
    /// </summary>
    public struct DbIndexMetaData
    {
        /// <summary>
        /// Name of the index
        /// </summary>
        public string IndexName;
        /// <summary>
        /// Schema of the table that the index belongs to
        /// </summary>
        public string SchemaName;
        /// <summary>
        /// Name of table that the index belongs to
        /// </summary>
        public string TableName;
        /// <summary>
        /// SchemaName.TableName.IndexName
        /// </summary>
        public string FullyQualifiedName { get { return SchemaName + "." + TableName + "." + IndexName; } }
        /// <summary>
        /// Indicates if the index is a unqique index
        /// </summary>
        public bool IsUnique;
        /// <summary>
        /// Indicates if the index is the primary key
        /// </summary>
        public bool IsPrimaryKey;
        /// <summary>
        /// Indicates if the index is a clustered index
        /// </summary>
        public bool IsClustered;
        /// <summary>
        /// The ordered list of columns defining the index
        /// </summary>
        public SortedDictionary<Int16, DbIndexColumnMetaData> ColumnOrder;
        /// <summary>
        /// Collection of index columns
        /// </summary>
        public Dictionary<string, DbIndexColumnMetaData> Columns;
        /// <summary>
        /// A list of columns to be included with Index but not part of key directly(SQLServer 2008 >)
        /// </summary>
        public List<string> IncludeColumns;
        /// <summary>
        /// Constructor to provide a thread safe copy of the 
        /// given structure with all its contained collections.
        /// </summary>
        /// <param name="dbIndex">DbIndexStructure to copy</param>
        public DbIndexMetaData(DbIndexMetaData dbIndex)
        {
            IndexName = dbIndex.IndexName;
            SchemaName = dbIndex.SchemaName;
            TableName = dbIndex.TableName;
            IsUnique = dbIndex.IsUnique;
            IsPrimaryKey = dbIndex.IsPrimaryKey;
            IsClustered = dbIndex.IsClustered;
            ColumnOrder = new SortedDictionary<short, DbIndexColumnMetaData>(dbIndex.ColumnOrder);
            Columns = new Dictionary<string, DbIndexColumnMetaData>(dbIndex.Columns);
            IncludeColumns = new List<string>(dbIndex.IncludeColumns);
        }
    }


    public static class Constants
    {
        public const string BindValuePrefix = "@";    // used for SqlServer and Db2
        public const string DefaultTableAlias = "T";  // alias to use when joining tables
        public const string NoOpDbCommandText = "--"; // comment for most databases
        public const string ParameterPrefix = "@";    // used for SqlServer and Db2
        public const string RefCursor = "RefCursor";
        public const string ParamSetValueSuffix = "_sv";
    }
}
