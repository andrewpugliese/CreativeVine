using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Net;
using System.Threading;

using CV.Files;

namespace CV.FTP
{
    /// <summary>
    /// The internal worker class for the FTPManager.
    /// <para>
    /// This class performs all the FTP operations and is only to be consumed by the FTPManager
    /// </para>
    /// <para>
    /// This class has entry point methods for asynchronous calls
    /// </para>
    /// </summary>
    internal class FTPWorker
    {
        Action<int, List<FtpResults>, Action<List<FtpResults>>> _asyncCallback;
        Action<List<FtpResults>> _asyncCallbackDelegate;
        string _ftpServer;
        string _ftpUser;
        string _ftpPassword;
        string _renameSuffix;
        string _sourcePath;
        string _targetPath;
        FTPManager.PostFtpOperation _postFtpOperation;
        bool _exit;
        List<string> _fileList;
        FileHelper.StreamBufferSize _bufferSize;

        #region Constructor

        internal FTPWorker(string ftpServer
            , string ftpUser
            , string ftpPassword
            , Action<int, List<FtpResults>, Action<List<FtpResults>>> asyncCallback
            , Action<List<FtpResults>> asyncCallbackDelegate
            , string renameSuffix
            , List<string> fileList
            , string sourcePath
            , string targetPath
            , FTPManager.PostFtpOperation postFtpOperation
            , FileHelper.StreamBufferSize bufferSize)
        {
            _ftpServer = ftpServer;
            _ftpUser = ftpUser;
            _ftpPassword = ftpPassword;
            _asyncCallback = asyncCallback;
            _asyncCallbackDelegate = asyncCallbackDelegate;
            _fileList = fileList;
            _renameSuffix = renameSuffix;
            _sourcePath = sourcePath;
            _targetPath = targetPath;
            _postFtpOperation = postFtpOperation;
            _exit = false;
            _bufferSize = bufferSize;
            if (fileList != null 
                && fileList.Count > 0
                && !Directory.Exists(targetPath))
                Directory.CreateDirectory(targetPath);
        }

        #endregion

        #region Helper Functions

        internal void Exit()
        {
            _exit = true;
        }

        internal static string CreateURI(string filePath, string fileSpec)
        {
            if (string.IsNullOrEmpty(filePath))
                return fileSpec;
            return string.Format("{0}{1}"
                , filePath.StartsWith("/") ? filePath : "/" + filePath
                , filePath.EndsWith("/") ? fileSpec : "/" + fileSpec);
        }

        internal FtpWebRequest CreateRequest(string uri)
        {
            return CreateRequest(_ftpServer, _ftpUser, _ftpPassword, uri);
        }

        internal static FtpWebRequest CreateRequest(string ftpServer
            , string ftpUser
            , string ftpPassword
            , string uri)
        {
            FtpWebRequest ftpClient = (FtpWebRequest)FtpWebRequest.Create(ftpServer
                + (string.IsNullOrEmpty(uri) ? string.Empty : uri));
            /* Log in to the FTP Server with the User Name and Password if Provided */
            if (!string.IsNullOrEmpty(ftpUser) && !string.IsNullOrEmpty(ftpPassword))
                ftpClient.Credentials = new NetworkCredential(ftpUser, ftpPassword);
            ftpClient.UseBinary = true;
            ftpClient.UsePassive = true;
            ftpClient.KeepAlive = false;
            return ftpClient;
        }


        void DeleteFtpFile(string sourcePath, string file)
        {
            FtpWebRequest ftpClient = CreateRequest(CreateURI(sourcePath, file));
            ftpClient.Method = WebRequestMethods.Ftp.DeleteFile;
            using (FtpWebResponse ftpClientResponse = (FtpWebResponse)ftpClient.GetResponse())
            {
                ftpClientResponse.Close();
            }
        }

        void RenameFtpFile(string sourcePath, string file, string newFileName)
        {
            FtpWebRequest ftpClient = CreateRequest(CreateURI(sourcePath, file));
            ftpClient.RenameTo = newFileName;
            ftpClient.Method = WebRequestMethods.Ftp.Rename;
            using (FtpWebResponse ftpClientResponse = (FtpWebResponse)ftpClient.GetResponse())
            {
                ftpClientResponse.Close();
            }
        }

        #endregion

        #region Download

        /// <summary>
        /// Used for asynchronous calls to download files
        /// </summary>
        internal void BeginDownloadFiles()
        {
            List<FtpResults> results = DownloadFiles();
            _asyncCallback(Thread.CurrentThread.ManagedThreadId, results, _asyncCallbackDelegate);
        }

        /// <summary>
        /// use for asynchronous transferring of large files
        /// </summary>
        /// <returns></returns>
        bool Stopped()
        {
            return _exit;
        }

        /// <summary>
        /// Used for synchronous calls
        /// </summary>
        /// <returns></returns>
        internal List<FtpResults> DownloadFiles()
        {
            List<FtpResults> results = new List<FtpResults>();
            foreach (string file in _fileList)
            {
                if (_exit)
                    break;
                try
                {
                    FtpWebRequest ftpClient = CreateRequest(CreateURI(_sourcePath, file));
                    ftpClient.Method = WebRequestMethods.Ftp.DownloadFile;
                    ftpClient.KeepAlive = false;
                    using (FtpWebResponse ftpClientResponse = (FtpWebResponse)ftpClient.GetResponse())
                    {
                        using (StreamReader responseStream = new StreamReader(ftpClientResponse.GetResponseStream()))
                        {
                            using (StreamWriter outputStream = new StreamWriter(FileHelper.GetFileName(_targetPath, file), false))
                            {
                                FileHelper.TransferStream(responseStream.BaseStream
                                    , outputStream.BaseStream
                                    , _bufferSize
                                    , Stopped);
                                outputStream.Close();
                            }
                            responseStream.Close();
                        }
                        ftpClientResponse.Close();
                        if (_postFtpOperation == FTPManager.PostFtpOperation.Delete)
                            DeleteFtpFile(_sourcePath, file);
                        else if (_postFtpOperation == FTPManager.PostFtpOperation.Rename)
                            RenameFtpFile(_sourcePath, file, string.Format("{0}{1}"
                                , file, _renameSuffix));
                        FtpResults result = new FtpResults();
                        FtpFileAttributes fileAttr = new FtpFileAttributes();
                        fileAttr.Filename = file;
                        result.File = fileAttr;
                        result.Success = true;
                        results.Add(result);
                    }
                }
                catch (Exception exc)
                {
                        FtpResults result = new FtpResults();
                        FtpFileAttributes fileAttr = new FtpFileAttributes();
                        fileAttr.Filename = file;
                        result.File = fileAttr;
                        result.Exc = new ApplicationException(string.Format("Ftp Download file: {0} from server: {1}"
                            , file, _ftpServer), exc);
                        results.Add(result);
                }
            }
            return results;
        }

        #endregion

        #region Upload
        /// <summary>
        /// used for asynchronous processing
        /// </summary>
        internal void BeginUploadFiles()
        {
            List<FtpResults> results = UploadFiles();
            _asyncCallback(Thread.CurrentThread.ManagedThreadId, results, _asyncCallbackDelegate);
        }

        /// <summary>
        /// used for synchronous processing
        /// </summary>
        /// <returns></returns>
        internal List<FtpResults> UploadFiles()
        {
            List<FtpResults> results = new List<FtpResults>();
            foreach (string file in FileHelper.GetFileNameWithoutPath(_fileList))
            {
                if (_exit)
                    break;
                try
                {
                    FtpWebRequest ftpClient = CreateRequest(CreateURI(_targetPath, file));
                    ftpClient.Method = WebRequestMethods.Ftp.UploadFile;
                    ftpClient.KeepAlive = false;
                    using (Stream ftpClientStream = ftpClient.GetRequestStream())
                    {
                        using (StreamReader inputStream = new StreamReader(FileHelper.GetFileName(_sourcePath, file), false))
                        {
                            FileHelper.TransferStream(inputStream.BaseStream
                                    , ftpClientStream
                                    , _bufferSize
                                    , Stopped);
                            inputStream.Close();
                        }
                        ftpClientStream.Close();
                        if (_postFtpOperation == FTPManager.PostFtpOperation.Delete)
                            FileHelper.DeleteFile(_sourcePath, file);
                        else if (_postFtpOperation == FTPManager.PostFtpOperation.Rename)
                            FileHelper.RenameFile(_sourcePath, file, string.Format("{0}{1}"
                                , file, _renameSuffix));
                        FtpResults result = new FtpResults();
                        FtpFileAttributes fileAttr = new FtpFileAttributes();
                        fileAttr.Filename = file;
                        result.File = fileAttr;
                        result.Success = true;
                        results.Add(result);
                    }
                }
                catch (Exception exc)
                {
                    FtpResults result = new FtpResults();
                    FtpFileAttributes fileAttr = new FtpFileAttributes();
                    fileAttr.Filename = file;
                    result.File = fileAttr;
                    result.Exc = new ApplicationException(string.Format("Ftp Upload file: {0} from server: {1}"
                        , file, _ftpServer), exc);
                    results.Add(result);
                }
            }
            return results;
        }

        #endregion

    }
}
