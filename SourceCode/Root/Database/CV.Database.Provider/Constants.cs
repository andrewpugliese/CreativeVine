using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CV.Database
{
    public static class Constants
    {
        internal const string BindValuePrefix = "@";    // used for SqlServer and Db2
        internal const string DefaultTableAlias = "T";  // alias to use when joining tables
        internal const string NoOpDbCommandText = "--"; // comment for most databases
        internal const string ParameterPrefix = "@";    // used for SqlServer and Db2
        public const string RefCursor = "RefCursor";
        internal const string ParamSetValueSuffix = "_sv";
    }
}
