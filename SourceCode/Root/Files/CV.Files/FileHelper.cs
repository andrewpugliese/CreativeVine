using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;

namespace CV.Files
{

    /// <summary>
    /// Helper class for File related functions
    /// </summary>
    public static class FileHelper
    {
        public enum StreamBufferSize { _1K = 1024, _2K = 2048, _4k = 4096, _1MB = 1048576, _5MB = 1048576 * 5 };
#pragma warning disable 1591 // disable the xmlComments warning
        public static void WriteTextToFile(string fileName, string text, bool appendText, bool exclusive)
        {
            using (FileStream fs = File.Open(fileName, appendText ? FileMode.Append : FileMode.Create, FileAccess.Write,
                exclusive ? FileShare.Read : FileShare.ReadWrite))
            using (StreamWriter sw = new StreamWriter(fs))
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
        /// <summary>
        /// Converts a list of filenames with paths to filename only
        /// </summary>
        /// <param name="fileList"></param>
        /// <returns></returns>
        public static IEnumerable<string> GetFileNameWithoutPath(List<string> fileList)
        {
            foreach (string file in fileList)
                if (file.Contains(":") || file.Contains("\\"))
                    yield return Path.GetFileName(file);
                else yield return file;
        }

        /// <summary>
        /// Converts an array of filenames with paths to list of filename only
        /// </summary>
        /// <param name="files"></param>
        /// <returns></returns>
        public static IEnumerable<string> GetFileNameWithoutPath(string[] files)
        {
            for(int i = 0; i < files.Length; i++)
            {
                string file = files[i];
                if (file.Contains(":") || file.Contains("\\"))
                    yield return Path.GetFileName(file);
                else yield return file;
            }
        }

        public static string GetFileName(string path, string file)
        {
            if (string.IsNullOrEmpty(path))
                return file;
            return string.Format("{0}\\{1}", path, file);
        }

        /// <summary>
        /// Deletes a file given the path and filename (without path)
        /// </summary>
        /// <param name="sourcePath"></param>
        /// <param name="file"></param>
        public static void DeleteFile(string sourcePath, string file)
        {
            string filename = GetFileName(sourcePath, file);
            File.Delete(file);
        }

        /// <summary>
        /// Renames a file given the path and filename and new filename
        /// </summary>
        /// <param name="sourcePath"></param>
        /// <param name="file"></param>
        /// <param name="newFileName"></param>
        public static void RenameFile(string sourcePath, string file, string newFileName)
        {
            string filename = GetFileName(sourcePath, file);
            File.Move(filename, newFileName);
        }

        /// <summary>
        /// Transfers data from the inputStream to the outputStream
        /// in the given buffer size number of bytes.
        /// <para>Function will flush outputStream but does NOT close either stream</para>
        /// </summary>
        /// <param name="inputStream"></param>
        /// <param name="outputStream"></param>
        /// <param name="bufferSize"></param>
        public static void TransferStream(Stream inputStream
            , Stream outputStream
            , StreamBufferSize bufferSize = StreamBufferSize._2K
            , Func<bool> stopTransfer = null)
        {
            int bufferLen = GetByteSize(bufferSize);
            byte[] buffer = new byte[bufferLen];
            int transferLen = inputStream.Read(buffer, 0, bufferLen);
            while (transferLen > 0)
            {
                if (stopTransfer != null    // have we been interrupted.
                    && stopTransfer())
                    break;
                outputStream.Write(buffer, 0, transferLen);
                transferLen = inputStream.Read(buffer, 0, bufferLen);
            }
            outputStream.Flush();
        }

        static int GetByteSize(StreamBufferSize bufferSize)
        {
            switch (bufferSize)
            {
                case StreamBufferSize._1K:
                    return 1024;
                case StreamBufferSize._2K:
                    return 2048;
                case StreamBufferSize._4k:
                    return 4096;
                case StreamBufferSize._1MB:
                    return 1048576;
                case StreamBufferSize._5MB:
                    return 1048576 * 5;
                default:
                    return 2048;
            }
        }

        public static List<string> GetDirectoryFiles(string sourcePath
            , string fileSpec
            , bool includePath = false)
        {
            string[] files = System.IO.Directory.GetFileSystemEntries(sourcePath, fileSpec);
            List<string> fileList = new List<string>(files.Length);
            foreach (string filename in includePath ? files : GetFileNameWithoutPath(files))
                fileList.Add(filename);
            return fileList;
        }

        /// <summary>
        /// Decompress .gz/.zip files to the specified folder
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="DecompressPath">if null, uses subdirectory of fileName</param>
        /// <exception>ArgumentOutOfRangeException</exception>
        public static FileInfo[] Decompress(string FileName
            , string DecompressPath = null)
        {
            DirectoryInfo di = new DirectoryInfo(FileName);

            if (di.Extension.ToLower() == ".gz")
            {
                List<FileInfo> fileList = new List<FileInfo>();
                string targetDir = DecompressPath;

                FileInfo fi = new FileInfo(FileName);
                // Get the stream of the source file.
                using (FileStream inFile = fi.OpenRead())
                {
                    //Removes the .gz extn on the files while Unzipping.
                    string outFileLocation = targetDir + fi.Name;
                    FileInfo fin = new FileInfo(outFileLocation);
                    int extLength = fin.Extension.Length;
                    string unCompressedFile = outFileLocation.Remove(outFileLocation.Length - extLength);

                    //Create the decompressed file.
                    using (FileStream outFile = File.Create(unCompressedFile))
                    {
                        using (GZipStream Decompress = new GZipStream(inFile, CompressionMode.Decompress))
                        {
                            // Copy the decompression stream into the output file.
                            Decompress.CopyTo(outFile);
                        }
                        fileList.Add(new FileInfo(unCompressedFile));
                    }
                }
                return fileList.ToArray();
            }

            else if (di.Extension.ToLower() == ".zip")
                return ZipHelper.Extract(FileName, DecompressPath);
            else throw new ArgumentOutOfRangeException(string.Format("Unsupported file extension for decompression: {0}", di.Extension));
        }
    }
#pragma warning restore 1591 // disable the xmlComments warning        public enum StreamBufferSize {_1K, _2K, _4k, _1MB, _5MB};
}
