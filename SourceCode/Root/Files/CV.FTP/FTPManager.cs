using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using System.Globalization;
using System.Threading;

using CV.Files;
using CV.Cache;

namespace CV.FTP
{
    public struct FtpFileAttributes
    {
        public string Filename;
        public DateTime TimeStamp;
        public int Size;
    }

    public struct FtpResults
    {
        public FtpFileAttributes File;
        public bool Success;
        public Exception Exc;
    }

    /// <summary>
    /// File Transfer Protocol (FTP) Manager
    /// The class provides the following functionality:
    /// <para>
    /// <list type="bullet">
    /// <item>Return listing of files on server</item>
    /// <item>Download files</item>
    /// <item>Upload files</item>
    /// <item>Delete source file after upload or download</item>
    /// <item>Rename source file after upload or download</item>
    /// <item>Can be used synchronously or asynchronously</item>
    /// </list>
    /// </para>Note: If using the asynchronous methods it is best practice to 
    /// use a using block: 
    /// <para>
    /// using(FTPManager ftp = new FTPManager()){}
    /// </para>
    /// </summary>
    public class FTPManager : IDisposable
    {
        public enum PostFtpOperation { None, Delete, Rename };
        CacheMgr<int, FTPWorker> _ftpWorkers;
        CacheMgr<int, Thread> _threadPool;
        string _ftpServer;
        string _ftpUser;
        string _ftpPassword;
        string _renameSuffix = ".completed";

        #region Constructors

        /// <summary>
        /// Main Constructor accepting server address and login credentials.
        /// </summary>
        /// <param name="ftpServer">Server url or ip address</param>
        /// <param name="ftpUser">UserId; null for anonymous</param>
        /// <param name="ftpPassword">Password; null for anonymous</param>
        /// <param name="renameSuffix">Suffix to be applied to filename when ftp operation is completed.
        /// This is only applied when PostOperation is set to rename.</param>
        /// <exception cref="ArgumentException">Thrown if Server cannot be accessed or credentials are invalid</exception>
        public FTPManager(string ftpServer
            , string ftpUser = null
            , string ftpPassword = null
            , string renameSuffix = ".completed")
        {
            _ftpServer = ftpServer;
            if (string.IsNullOrEmpty(ftpServer))
            {
                throw new ArgumentNullException("ftpServer", "FTP Server Address cannot be blank");
            }
            if (!_ftpServer.StartsWith("ftp://"))
                _ftpServer = "ftp://" + _ftpServer;
            if (!string.IsNullOrEmpty(ftpUser)
                && string.IsNullOrEmpty(ftpPassword))
            {
                throw new ArgumentNullException("ftpPassword", "Password cannot be blank for when userId is not null; if Anonymous, use null");
            }
            _ftpUser = ftpUser;
            _ftpPassword = ftpPassword;
            _renameSuffix = !string.IsNullOrEmpty(renameSuffix) ? renameSuffix.StartsWith(".") ? renameSuffix : "." + renameSuffix : _renameSuffix;
            VerifyCredentials();
            _threadPool = new CacheMgr<int, Thread>();
            _ftpWorkers = new CacheMgr<int, FTPWorker>();

        }

        void VerifyCredentials()
        {
            FtpWebRequest ftpClient = FTPWorker.CreateRequest(_ftpServer, _ftpUser, _ftpPassword, null);
            ftpClient.Method = WebRequestMethods.Ftp.PrintWorkingDirectory;
            /* Establish Return Communication with the FTP Server */
            using (FtpWebResponse ftpClientResponse = (FtpWebResponse)ftpClient.GetResponse())
            {
                ftpClientResponse.Close();
            }
        }

        /// <summary>
        /// Disposes any background threads
        /// </summary>
        public void Dispose()
        {
            Cancel();
        }

        /// <summary>
        /// Disposes any background threads
        /// </summary>
        public void Cancel()
        {
            foreach (int threaID in _ftpWorkers.Keys)
            {
                FTPWorker worker = _ftpWorkers.GetOrDefault(threaID, null);
                if (worker != null)
                    worker.Exit();
            }
            _ftpWorkers.Clear();
            foreach (int threadID in _threadPool.Keys)
            {
                Thread t = _threadPool.GetOrDefault(threadID, null);
                if (t != null && t.IsAlive && t.Join(2000))
                    t.Abort();
            }
            _threadPool.Clear();
        }

        public bool ThreadsCompleted
        {
            get { return _threadPool.IsEmpty; }
        }

        void WorkerCompleted(int i, List<FtpResults> results, Action<List<FtpResults>> asynchCallback)
        {
            _threadPool.Remove(i);
            _ftpWorkers.Remove(i);
            if (asynchCallback != null)
                asynchCallback(results);
        }

        #endregion

        #region ReadOnly Methods

        /// <summary>
        /// Retrieves list of files (including datetime, and size) matching filespec at filepath
        /// </summary>
        /// <param name="ftpPath">Path on ftp server to obtain listing</param>
        /// <param name="fileSpec">File search pattern</param>
        /// <returns>List<FileAttributes></returns>
        public List<FtpFileAttributes> GetFileListDetails(string ftpPath, string fileSpec)
        {
            if (ftpPath == null)
                ftpPath = string.Empty;
            FtpWebRequest ftpClient = FTPWorker.CreateRequest(_ftpServer
                , _ftpUser
                , _ftpPassword
                , FTPWorker.CreateURI(ftpPath, fileSpec));
            List<FtpFileAttributes> ftpFileList = new List<FtpFileAttributes>();
            ftpClient.Method = WebRequestMethods.Ftp.ListDirectoryDetails;
            using (FtpWebResponse ftpClientResponse = (FtpWebResponse)ftpClient.GetResponse())
            {
                using (StreamReader outputStream = new StreamReader(ftpClientResponse.GetResponseStream()))
                {
                    while (!outputStream.EndOfStream)
                    {
                        string line = outputStream.ReadLine();
                        var tokens = line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        if (tokens.Length > 3)
                        {
                            FtpFileAttributes ftpFile = new FtpFileAttributes();
                            string date = string.Format("{0}{1}", tokens[0], tokens[1]);
                            //ftpFile.TimeStamp = Convert.ToDateTime(date);
                            ftpFile.TimeStamp = DateTime.ParseExact(date, "MM-dd-yyhh:mmtt"
                                    , CultureInfo.InvariantCulture);
                            ftpFile.Size = int.Parse(tokens[2]);
                            ftpFile.Filename = tokens[3];
                            ftpFileList.Add(ftpFile);
                        }
                    }
                }
                ftpClientResponse.Close();
            }
            return ftpFileList;
        }


        /// <summary>
        /// Retrieves list of files (name only) matching filespec at filepath
        /// </summary>
        /// <param name="ftpPath">Path on ftp server to obtain listing</param>
        /// <param name="fileSpec">File search pattern</param>
        /// <returns>List<string></returns>
        public List<string> GetFileList(string ftpPath, string fileSpec)
        {
            FtpWebRequest ftpClient = FTPWorker.CreateRequest(_ftpServer
                , _ftpUser
                , _ftpPassword
                , FTPWorker.CreateURI(ftpPath, fileSpec));
            List<string> ftpFileList = new List<string>();
            ftpClient.Method = WebRequestMethods.Ftp.ListDirectory;
            using (FtpWebResponse ftpClientResponse = (FtpWebResponse)ftpClient.GetResponse())
            {
                using (StreamReader responseStream = new StreamReader(ftpClientResponse.GetResponseStream()))
                {
                    while (!responseStream.EndOfStream)
                    {
                        string line = responseStream.ReadLine();
                        ftpFileList.Add(line.Trim());
                    }
                }
                ftpClientResponse.Close();
            }
            return ftpFileList;
        }

        #endregion

        #region Download Methods

        /// <summary>
        /// Downloads all the files found that match the fileSpec from the given sourcePath
        /// to the given targetPath.  Depending on the postOperation, source file is either 
        /// deleted or renamed.
        /// </summary>
        /// <param name="ftpSourcePath">Path on ftp server to search for files</param>
        /// <param name="fileSpec">File search pattern</param>
        /// <param name="targetFilePath">Target path to download files</param>
        /// <param name="postFtpOperation">Delete or Rename source file after operation
        /// <para>
        /// Default is to do nothing
        /// </para>
        /// </param>
        /// <returns>List of FTPResult structures</returns>
        public List<FtpResults> DownloadFiles(string ftpSourcePath
            , string fileSpec
            , string targetFilePath
            , PostFtpOperation postFtpOperation = PostFtpOperation.None
            , FileHelper.StreamBufferSize bufferSize = FileHelper.StreamBufferSize._5MB)
        {
            return DownloadFiles(GetFileList(ftpSourcePath, fileSpec)
                , ftpSourcePath
                , targetFilePath
                , postFtpOperation
                , bufferSize);
        }

        /// <summary>
        /// Downloads all the files found in the given list of filenames from the given sourcePath
        /// to the given targetPath.  Depending on the postOperation, source file is either 
        /// deleted or renamed.
        /// </summary>
        /// <param name="fileList">List of files to download</param>
        /// <param name="ftpSourcePath">Path on ftp server to search for files</param>
        /// <param name="targetFilePath">Target path to download files</param>
        /// <param name="postFtpOperation">Delete or Rename source file after operation
        /// <param name="bufferSize">The buffer size of the data transfer
        /// <para>
        /// Default is 1MB
        /// </para>
        /// </param>
        /// <returns></returns>
        public List<FtpResults> DownloadFiles(List<string> fileList
            , string ftpSourcePath
            , string targetFilePath
            , PostFtpOperation postFtpOperation = PostFtpOperation.None
            , FileHelper.StreamBufferSize bufferSize = FileHelper.StreamBufferSize._5MB)
        {
            List<FtpResults> results = new List<FtpResults>();
            FTPWorker ftpWorker = new FTPWorker(_ftpServer
                , _ftpUser
                , _ftpPassword
                , null
                , null
                , _renameSuffix
                , fileList
                , ftpSourcePath
                , targetFilePath
                , postFtpOperation
                , bufferSize);
            return ftpWorker.DownloadFiles();
        }

        /// <summary>
        /// Downloads (ASYNCHRONOUSLY) all the files found that match the fileSpec from the given sourcePath
        /// to the given targetPath.  Depending on the postOperation, source file is either 
        /// deleted or renamed.
        /// </summary>
        /// <param name="ftpSourcePath">Path on ftp server to search for files</param>
        /// <param name="fileSpec">File search pattern</param>
        /// <param name="targetFilePath">Target path to download files</param>
        /// <param name="asyncCallback">This function will be called when the
        /// method is completed (asynchronously)</param>
        /// <param name="postFtpOperation">Delete or Rename source file after operation
        /// <param name="bufferSize">The buffer size of the data transfer
        /// <para>
        /// Default is 1MB
        /// </para>
        /// </param>
        public void BeginDownloadFiles(string ftpSourcePath
            , string fileSpec
            , string targetFilePath
            , Action<List<FtpResults>> asyncCallback
            , PostFtpOperation postFtpOperation = PostFtpOperation.None
            , FileHelper.StreamBufferSize bufferSize = FileHelper.StreamBufferSize._5MB)
        {
            BeginDownloadFiles(GetFileList(ftpSourcePath, fileSpec)
                , ftpSourcePath
                , targetFilePath
                , asyncCallback
                , postFtpOperation
                , bufferSize);
        }

        /// <summary>
        /// Downloads (ASYNCHRONOUSLY) all the files found in the given list of filenames from the given sourcePath
        /// to the given targetPath.  Depending on the postOperation, source file is either 
        /// deleted or renamed.
        /// </summary>
        /// <param name="fileList">List of files to download</param>
        /// <param name="ftpSourcePath">Path on ftp server to search for files</param>
        /// <param name="targetFilePath">Target path to download files</param>
        /// <param name="asyncCallback">This function will be called when the
        /// method is completed (asynchronously)</param>
        /// <param name="postFtpOperation">Delete or Rename source file after operation
        /// <param name="bufferSize">The buffer size of the data transfer
        /// <para>
        /// Default is 1MB
        /// </para>
        /// </param>
        public void BeginDownloadFiles(List<string> fileList
            , string sourcePath
            , string targetPath
            , Action<List<FtpResults>> asyncCallback
            , PostFtpOperation postFtpOperation = PostFtpOperation.None
            , FileHelper.StreamBufferSize bufferSize = FileHelper.StreamBufferSize._5MB)
        {
            FTPWorker ftpWorker = new FTPWorker(_ftpServer
                , _ftpUser
                , _ftpPassword
                , WorkerCompleted
                , asyncCallback
                , _renameSuffix
                , fileList
                , sourcePath
                , targetPath
                , postFtpOperation
                , bufferSize);
            Thread t = new Thread(ftpWorker.BeginDownloadFiles);
            t.IsBackground = true;
            _ftpWorkers.Add(t.ManagedThreadId, ftpWorker);
            _threadPool.Add(t.ManagedThreadId, t);
            t.Start();
        }

        #endregion

        #region Upload Methods

        /// <summary>
        /// Uploads all the files found that match the fileSpec from the given sourceFilePath
        /// to the given targetFtpPath.  Depending on the postOperation, source file is either 
        /// deleted or renamed.
        /// </summary>
        /// <param name="sourceFilePath">Directory path to search for files</param>
        /// <param name="fileSpec">File search pattern</param>
        /// <param name="targetFtpPath">Target directory on server to upload files</param>
        /// <param name="postFtpOperation">Delete or Rename source file after operation
        /// <param name="bufferSize">The buffer size of the data transfer
        /// <para>
        /// Default is 1MB
        /// </para>
        /// <returns></returns>
        public List<FtpResults> UploadFiles(string sourceFilePath
            , string fileSpec
            , string targetFtpPath
            , PostFtpOperation postFtpOperation = PostFtpOperation.None
            , FileHelper.StreamBufferSize bufferSize = FileHelper.StreamBufferSize._5MB)
        {

            return UploadFiles(FileHelper.GetDirectoryFiles(sourceFilePath, fileSpec)
               , sourceFilePath
               , targetFtpPath
               , postFtpOperation
               , bufferSize);
        }

        /// <summary>
        /// Uploads all the files found in the given list of filenames from the given sourceFilePath
        /// to the given targetFtpPath.  Depending on the postOperation, source file is either 
        /// deleted or renamed.
        /// </summary>
        /// <param name="fileList">List of files to Upload</param>
        /// <param name="sourceFilePath">Directory path to search for files</param>
        /// <param name="targetFtpPath">Target directory on server to upload files</param>
        /// <param name="postFtpOperation">Delete or Rename source file after operation
        /// <param name="bufferSize">The buffer size of the data transfer
        /// <para>
        /// Default is 1MB
        /// </para>
        /// <returns></returns>
        public List<FtpResults> UploadFiles(List<string> fileList
            , string sourceFilePath
            , string targetFtpPath
            , PostFtpOperation postFtpOperation = PostFtpOperation.None
            , FileHelper.StreamBufferSize bufferSize = FileHelper.StreamBufferSize._5MB)
        {
            List<FtpResults> results = new List<FtpResults>();
            FTPWorker ftpWorker = new FTPWorker(_ftpServer
                , _ftpUser
                , _ftpPassword
                , null
                , null
                , _renameSuffix
                , fileList
                , sourceFilePath
                , targetFtpPath
                , postFtpOperation
                , bufferSize);
            return ftpWorker.UploadFiles();
        }


        /// <summary>
        /// Uploads (ASYNCHRONOUSLY) all the files found in the given list of filenames from the given sourceFilePath
        /// to the given targetFtpPath.  Depending on the postOperation, source file is either 
        /// deleted or renamed.
        /// </summary>
        /// <param name="sourceFilePath">Directory path to search for files</param>
        /// <param name="fileSpec">File search pattern</param>
        /// <param name="targetFtpPath">Target directory on server to upload files</param>
        /// <param name="asyncCallback">This function will be called when the
        /// method is completed (asynchronously)</param>
        /// <param name="postFtpOperation">Delete or Rename source file after operation
        /// <param name="bufferSize">The buffer size of the data transfer
        /// <para>
        /// Default is 1MB
        /// </para>
        public void BeginUploadFiles(string sourceFilePath
            , string fileSpec
            , string targetFtpPath
            , Action<List<FtpResults>> asyncCallback
            , PostFtpOperation postFtpOperation = PostFtpOperation.None
            , FileHelper.StreamBufferSize bufferSize = FileHelper.StreamBufferSize._1MB)
        {
            BeginUploadFiles(FileHelper.GetDirectoryFiles(sourceFilePath, fileSpec)
               , sourceFilePath
               , targetFtpPath
                , asyncCallback
               , postFtpOperation
               , bufferSize);
        }

        /// <summary>
        /// Uploads (ASYNCHRONOUSLY) all the files found in the given list of filenames from the given sourceFilePath
        /// to the given targetFtpPath.  Depending on the postOperation, source file is either 
        /// deleted or renamed.
        /// </summary>
        /// <param name="fileList">List of files to Upload</param>
        /// <param name="sourceFilePath">Directory path to search for files</param>
        /// <param name="targetFtpPath">Target directory on server to upload files</param>
        /// <param name="asyncCallback">This function will be called when the
        /// method is completed (asynchronously)</param>
        /// <param name="postFtpOperation">Delete or Rename source file after operation
        /// <param name="bufferSize">The buffer size of the data transfer
        /// <para>
        /// Default is 1MB
        /// </para>
        public void BeginUploadFiles(List<string> fileList
            , string sourceFilePath
            , string targetFtpPath
            , Action<List<FtpResults>> asyncCallback
            , PostFtpOperation postFtpOperation = PostFtpOperation.None
            , FileHelper.StreamBufferSize bufferSize = FileHelper.StreamBufferSize._1MB)
        {
            FTPWorker ftpWorker = new FTPWorker(_ftpServer
                , _ftpUser
                , _ftpPassword
                , WorkerCompleted
                , asyncCallback
                , _renameSuffix
                , fileList
                , sourceFilePath
                , targetFtpPath
                , postFtpOperation
                , bufferSize);
            Thread t = new Thread(ftpWorker.BeginUploadFiles);
            t.IsBackground = true;
            _ftpWorkers.Add(t.ManagedThreadId, ftpWorker);
            _threadPool.Add(t.ManagedThreadId, t);
            t.Start();
        }


        #endregion

    }
}
