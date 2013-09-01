using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections;

using ICSharpCode.SharpZipLib.Zip;


namespace CV.Files
{
    public class ZipHelper
    {
        /// <summary>
        /// Extracts the specified zip file stream.
        /// </summary>
        /// <param name="fileStream">The zip file stream.</param>
        public static FileInfo[] Extract(string ZipFile, string TargetDirectory = null)
        {
            string targetDirectory = TargetDirectory;
            if (string.IsNullOrEmpty(targetDirectory))
                targetDirectory = Environment.CurrentDirectory;
            else if (!Directory.Exists(targetDirectory))
                Directory.CreateDirectory(targetDirectory);
            List<FileInfo> fileList = new List<FileInfo>();
            using (StreamReader sr = new StreamReader(ZipFile))
            {
                ZipFile zipFile = new ZipFile(sr.BaseStream);

                IEnumerator enumerator = zipFile.GetEnumerator();

                while (enumerator.MoveNext())
                {
                    ZipEntry entry = (ZipEntry)enumerator.Current;
                    fileList.Add(ExtractZipEntry(zipFile, entry, TargetDirectory));
                }
            }
            return fileList.ToArray();
        }

        private static FileInfo ExtractZipEntry(ZipFile zipFile, ZipEntry entry, string TargetDirectory)
        {
            byte[] buffer = new byte[0x1000];

            if (!entry.IsCompressionMethodSupported() 
                || string.IsNullOrEmpty(entry.Name)
                || !entry.IsFile) return null;

            string targetFileName;
            if (entry.Name.Contains('/'))
            {
                string[] fileparts = entry.Name.Split(new char[] { '/' });
                string subDir = TargetDirectory;
                for (int i = 0; i < fileparts.Length - 1; i++ )
                {
                    subDir += "/" + fileparts[i];
                    if (!Directory.Exists(subDir))
                        Directory.CreateDirectory(subDir);
                }
                targetFileName = string.Format("{0}/{1}", subDir, fileparts[fileparts.Length-1]);
            }
            else targetFileName = entry.Name;

            using (StreamWriter stream = new StreamWriter(targetFileName))
            {
                Stream inputStream = zipFile.GetInputStream(entry);

                FileHelper.TransferStream(inputStream
                    , stream.BaseStream
                    , FileHelper.StreamBufferSize._2K, null);
            }
            return new FileInfo(targetFileName);
        }
    }
}
