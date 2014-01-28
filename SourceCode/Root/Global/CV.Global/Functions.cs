using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;
using System.Xml;
using System.IO;

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

        /// <summary>
        /// Returns a serialized string version of the given type T
        /// </summary>
        /// <typeparam name="T">A valid .Net data type</typeparam>
        /// <param name="obj">An object of type T</param>
        /// <returns>A serialized string version of type T</returns>
        public static string Serialize<T>(T obj)
        {
            return Serialize<T>(obj, null);
        }

        /// <summary>
        /// Returns a serialized string version of the given type T
        /// </summary>
        /// <typeparam name="T">A valid .Net data type</typeparam>
        /// <param name="obj">An object of type T</param>
        /// <param name="knownTypes">An IEnumerable collection of non default data types which may be present in obj T </param>
        /// <returns>A serialized string version of type T</returns>
        public static string Serialize<T>(T obj, IEnumerable<Type> knownTypes)
        {
            var serializer = new DataContractSerializer(obj.GetType(), knownTypes);
            using (var writer = new StringWriter())
            using (var stm = new XmlTextWriter(writer))
            {
                serializer.WriteObject(stm, obj);
                return writer.ToString();
            }
        }

        /// <summary>
        /// Returns the object of type T which is in a serialized form in the given parameter.
        ///  </summary>
        /// <typeparam name="T">A valid .Net data type</typeparam>
        /// <param name="serialized">An object of type T that was perviously serialized</param>
        /// <returns>A fully deserialized object of type T</returns>
        public static T Deserialize<T>(string serialized)
        {
            return Deserialize<T>(serialized, null);
        }

        /// <summary>
        /// Returns the object of type T which is in a serialized form in the given parameter.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="serialized"></param>
        /// <param name="knownTypes"></param>
        /// <returns></returns>
        public static T Deserialize<T>(string serialized, IEnumerable<Type> knownTypes)
        {
            var serializer = new DataContractSerializer(typeof(T), knownTypes);
            using (var reader = new StringReader(serialized))
            using (var stm = new XmlTextReader(reader))
            {
                return (T)serializer.ReadObject(stm);
            }
        }

    }
}
