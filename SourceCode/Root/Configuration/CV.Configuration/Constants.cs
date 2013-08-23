using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CV.Configuration
{
    class Constants
    {
        internal const string FormatError_SectionNotFound = "ConfigurationSection: {0} not found or not of type: {1}";
        internal const string FormatError_KeyNotFound = "The given configuration key: {0} was not found.";
        internal const string FormatError_RuntimeKeyNotFound = "The given runtime configuration key: {0} was not found.";
        internal const string FormatError_KeyEmptyValueFound = "The given configuration key: {0} was expected to have a non empty value.";

        internal const string AssemblyName = "AssemblyName";
        internal const string AssemblyPath = "AssemblyPath";
        internal const string ObjectFactories = "ObjectFactories";
        internal const string ObjectFactory = "ObjectFactory";
        internal const string ObjectKey = "ObjectKey";
        internal const string ObjectClass = "ObjectClass";
    }
}
