using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CV.Global
{
    public static class Functions
    {
        public static IEnumerable<T> GetEnumValues<T>() where T : struct
        {
            return Enum.GetValues(typeof(T)).Cast<T>();
        }

        public static T EnumFromInt<T>(int value, string exceptionMsg)
            where T : struct
        {
            return EnumFromString<T>(value.ToString(), true, exceptionMsg);
        }

        public static T EnumFromString<T>(string value
            , bool ignoreCase
            , string exceptionMsg) where T : struct
        {
            T result = default(T);
            if (Enum.TryParse<T>(value, ignoreCase, out result))
                return (T)Enum.Parse(typeof(T), value);
            else throw new ArgumentOutOfRangeException(string.IsNullOrEmpty(exceptionMsg)
                ? string.Format("Value: {0} not found in enumeration type: {1}"
                    , value, typeof(T).ToString()) : exceptionMsg );
        }

        /// <summary>
        /// Returns a boolean indicating whether or not the last character of the string
        /// text is the given character.
        /// </summary>
        /// <param name="text">Input string to search</param>
        /// <param name="chr">The character to search for.</param>
        /// <returns>Boolean</returns>
        public static bool IsLastCharInText(string text, char chr)
        {
            int idx = text.LastIndexOf(chr);
            if (idx < 0)
                return false;
            int len = text.Length;
            for (int i = idx + 1; i < len; i++)
                if (char.IsLetterOrDigit(text[i]))
                    return false;
            return true;
        }

    }
}
