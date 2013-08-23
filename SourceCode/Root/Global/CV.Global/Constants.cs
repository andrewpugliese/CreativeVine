using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CV.Global
{
    // Summary:
    //     Enumerates the different options for calculating a datetime difference
    public enum DateTimeInterval
    {
        Day = 0,
        Hour = 1,
        Minute = 2,
        Second = 3,
        MilliSecond = 4,
    }

   public static class Constants
    {
        public const string FormatError_FunctionNotImpleted = "Function Not Implemented for Database Type: {0}";
        public const string FormatError_NullOrEmptyParameter = "Null or empty parameter: {0}";
        public const string FormatError_InvalidParameterValue = "Parameter: {0} had invalid value: {1}; hint: {2}";
        public const string FormatError_UnhandledException = "Hint: {0}; Exception: {1}";
    }
}
