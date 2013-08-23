using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace CV.Files
{
    #pragma warning disable 1591 // disable the xmlComments warning
    public static class FileMgr
    {
        public static void WriteTextToFile(string fileName, string text, bool appendText, bool exclusive)
        {
            using(FileStream fs = File.Open(fileName, appendText ? FileMode.Append : FileMode.Create, FileAccess.Write,
                exclusive ? FileShare.Read : FileShare.ReadWrite ))
            using(StreamWriter sw = new StreamWriter(fs))
            {
                sw.Write(text);
                sw.Flush();
                sw.Close();
            }
        }

        public static string ReadTextFileIntoString(string fileName)
        {
            using (FileStream fs = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (StreamReader sr = new StreamReader(fs))
            {
                return sr.ReadToEnd();
            }
        }
    }
    #pragma warning restore 1591 // disable the xmlComments warning
}
