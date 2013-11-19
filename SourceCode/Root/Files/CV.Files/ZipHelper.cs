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
        static string _filenameExtension = "zip";

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
                for (int i = 0; i < fileparts.Length - 1; i++)
                {
                    subDir += "/" + fileparts[i];
                    if (!Directory.Exists(subDir))
                        Directory.CreateDirectory(subDir);
                }
                targetFileName = string.Format("{0}/{1}", subDir, fileparts[fileparts.Length - 1]);
            }
            else targetFileName = TargetDirectory + entry.Name;

            using (StreamWriter stream = new StreamWriter(targetFileName))
            {
                Stream inputStream = zipFile.GetInputStream(entry);

                FileHelper.TransferStream(inputStream
                    , stream.BaseStream
                    , FileHelper.StreamBufferSize._2K, null);
            }
            return new FileInfo(targetFileName);
        }

        public static List<string> DoZip(List<string> FilesList, FileInfo ZipFileName, int? BufferSize = null, bool IncludePath = false)
        {
            ZipFile zfile = null;
            List<string> zippedFiles = new List<string>();
            List<string> createdFiles = new List<string>();
            int bufferSize = BufferSize.HasValue && BufferSize.Value > 0 ? BufferSize.Value : FilesList.Count();
            int zipOutputFileCount = 0;

            int buffer = 0;
            for (int i = 0; i < FilesList.Count(); i++)
            {
                string file = FilesList[i];
                DateTime fileTime = DateTime.MinValue;
                if (buffer == 0 || buffer == bufferSize)
                {
                    //string pZipFile = GetZipFileName(Path.GetFileNameWithoutExtension(ZipFileName.Name), BufferSize.HasValue ? zipOutputFileCount++ : 0);
                    //string pZipFile = GetZipFileName(ZipFileName.Name, BufferSize.HasValue ? zipOutputFileCount++ : 0);
                    string pZipFile = GetZipFileName(ZipFileName.Name, zipOutputFileCount);
                    string pZipFilePath = string.Format("{0}\\{1}", ZipFileName.DirectoryName, pZipFile);
                    if (File.Exists(pZipFilePath) && !createdFiles.Contains(pZipFilePath))
                        File.Delete(pZipFilePath);
                    if (zfile == null && !createdFiles.Contains(pZipFilePath))
                    {
                        zfile = ZipFile.Create(pZipFilePath);
                        createdFiles.Add(pZipFilePath);
                    }
                    fileTime = File.GetCreationTime(pZipFilePath);
                    if (zfile == null || !zfile.TestArchive(true))
                        throw new ArgumentNullException(string.Format("Could not create zip file: {0}", pZipFilePath));
                    zippedFiles.Add(zfile.Name);
                    buffer = 0;
                }
                zfile.UseZip64 = UseZip64.On;
                zfile.BeginUpdate();
                if (IncludePath)
                    zfile.Add(file);
                else zfile.Add(file, Path.GetFileName(file));
                ++buffer;
                zfile.CommitUpdate();
                zfile.IsStreamOwner = true;
            }
            zfile.Close();
            return zippedFiles;
        }

        static string GetZipFileName(string FilenamePrefix, int ZipFileCount)
        {
            return string.Format("{0}{1}.{2}"
                , FilenamePrefix
                , ZipFileCount > 0 ? "." + ZipFileCount.ToString() : ""
                , _filenameExtension);
        }

    }
}
