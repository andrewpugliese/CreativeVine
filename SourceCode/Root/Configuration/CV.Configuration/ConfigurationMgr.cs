﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

using CV.Global;

namespace CV.Configuration
{
    public static class ConfigurationMgr
    {
        static Dictionary<string, Func<Object>> _runtimeSettings = new Dictionary<string
                , Func<Object>>(StringComparer.CurrentCultureIgnoreCase);

        /// <summary>
        /// Get all the keys of the AppSettings section.
        /// </summary>
        public static IEnumerable<string> GetKeys()
        {
            return ConfigurationManager.AppSettings.AllKeys;
        }

        public static Tuple<string, string> GetConnectionInfo(string connectionKey)
        {
            return new Tuple<string, string>(GetConnectionString(connectionKey)
                , GetConnectionProvider(connectionKey));
        }

        public static string GetConnectionString(string connectionKey)
        {
            return ConfigurationManager.ConnectionStrings[connectionKey].ConnectionString;
        }

        public static string GetConnectionProvider(string connectionKey)
        {
            return ConfigurationManager.ConnectionStrings[connectionKey].ProviderName;
        }

        /// <summary>
        /// Retrieves the configuration section object from the configuration file
        /// and casts it to its specific type
        /// </summary>
        /// <typeparam name="T">The type that defines the configuration section</typeparam>
        /// <param name="configurationSectionKey">The configuration section name</param>
        /// <returns>T or throws exception</returns>
        public static T GetSection<T>(string configurationSectionKey)
        {
            object section = ConfigurationManager.GetSection(configurationSectionKey);
            if (section != null
                && section is T)
                return (T)section;
            else throw new ExceptionMgr(typeof(ConfigurationMgr).ToString()
                , new ApplicationException(string.Format(Constants.FormatError_SectionNotFound
                    , configurationSectionKey, typeof(T).ToString())));
        }

        /// <summary>
        /// Returns configuration value for given key. Value may be empty.
        /// </summary>
        /// <param name="configKeyName"></param>
        /// <returns>string value</returns>
        public static string GetValue(string configKeyName)
        {
            if (!ConfigurationManager.AppSettings.AllKeys.Contains(configKeyName))
                throw new ExceptionMgr(typeof(ConfigurationMgr).ToString()
                , new ArgumentNullException(
                    string.Format(Constants.FormatError_KeyNotFound, configKeyName)));
            return GetValueOrDefault(configKeyName, null);
        }

        /// <summary>
        /// GetValue return one value for a given key.
        /// </summary>
        public static string GetNonEmptyValue(string configKeyName)
        {
            string value = GetValue(configKeyName);
            if (string.IsNullOrEmpty(value))
                throw new ExceptionMgr(typeof(ConfigurationMgr).ToString()
                , new ArgumentNullException(
                    string.Format(Constants.FormatError_KeyEmptyValueFound, configKeyName)));
            return value;
        }

        /// <summary>
        /// Its a helper function which allows to pass a convertor function so that one can parse the value and get
        /// back the type of value one wants.
        /// </summary>
        public static T GetValue<T>(string configKeyName, Func<string, T> convertorFn)
        {
            return convertorFn(GetValue(configKeyName));
        }

        /// <summary>
        /// Its a helper function which converts the value to Int32 and returns Int32 value. This is also an example of
        /// how to take advantage of the generic GetValue function.
        /// </summary>
        public static int GetValueAsInt32(string configKeyName)
        {
            return GetValue(configKeyName, val => Convert.ToInt32(val));
        }

        /// <summary>
        /// GetValueOrDefault do NOT throws an exceptoon, instead returns the provided default value if no value is
        /// found for a given key.
        /// </summary>
        public static string GetValueOrDefault(string configKeyName, string defaultValue)
        {
            return GetValueOrDefault(configKeyName, defaultValue, val => val);
        }

        /// <summary>
        /// Generic GetValueOrDefault allows to convert the value into the desired type.
        /// </summary>
        public static T GetValueOrDefault<T>(string configKeyName, T defaultValue, Func<string, T> convertorFn)
        {
            string configValue = ConfigurationManager.AppSettings[configKeyName];
            return configValue == null ? defaultValue : convertorFn(configValue);
        }

        /// <summary>
        /// Set a function which will be called whenever a v alue will be seeked for a given config key.
        /// </summary>
        public static void SetRuntimeValue(string configKeyName, Func<Object> getValueHandler)
        {
            if (!_runtimeSettings.ContainsKey(configKeyName))
                _runtimeSettings.Add(configKeyName, getValueHandler);
            else _runtimeSettings[configKeyName] = getValueHandler;
        }

        /// <summary>
        /// Call the runtime function for a given key.
        /// </summary>
        public static T GetRuntimeValue<T>(string configKeyName)
        {
            Func<object> configValueFn = null;
            if (_runtimeSettings.TryGetValue(configKeyName, out configValueFn))
                return (T)_runtimeSettings[configKeyName]();
            else
                throw new ExceptionMgr(typeof(ConfigurationMgr).ToString()
                    , new ArgumentNullException(
                        string.Format(Constants.FormatError_RuntimeKeyNotFound, configKeyName)));
        }

    }

}
