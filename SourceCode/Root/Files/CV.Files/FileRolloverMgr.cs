using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace CV.Files
{
    public class FileRolloverMgr : IDisposable
    {
        public enum RolloverMetric { ByteSize, RecordCount };
        int _maxMetric = 0;
        RolloverMetric _rolloverMetric;
        int _fileSizeBytes = 0;
        int _fileRecordCounter = 0;
        int _rolloverCount = 0;
        string _filenamePrefix = null;
        string _filenameExtension = null;
        string _fileName = null;
        string _header = null;
        string _footer = null;
        List<FileInfo> _fileList = new List<FileInfo>();
        bool _maintainFilePtr = true;
        StreamWriter _sw = null;

        public FileRolloverMgr(string filenamePrefix
            , RolloverMetric rolloverMetric = RolloverMetric.ByteSize
            , int maxMetric = 1024000
            , string filenameExtension = "txt"
            , string header = null
            , string footer = null
            , bool maintainFilePtr = true)
        {
            if (string.IsNullOrEmpty(filenamePrefix))
                throw new ArgumentNullException("FilenamePrefix cannot be empty");

            _header = header;
            _footer = footer;
            _filenamePrefix = filenamePrefix;
            _filenameExtension = filenameExtension;
            _rolloverMetric = rolloverMetric;
            _maintainFilePtr = maintainFilePtr;
            _maxMetric = (_rolloverMetric == RolloverMetric.ByteSize) ? Math.Max(maxMetric, 1024) : Math.Max(maxMetric, 1);
            _fileName = RolloverName();
            _fileList.Add(new FileInfo(_fileName));
        }

        ~FileRolloverMgr()
        {
            Dispose();
        }

        string RolloverName()
        {
            string fileName = string.Format("{0}.{1}.{2}"
                , _filenamePrefix
                , ++_rolloverCount
                , _filenameExtension);
            if (File.Exists(fileName))
                File.Delete(fileName);
            return fileName;
        }

        public bool AppendData(char[] buffer)
        {
            bool rollover = false;
            int bufferSize = buffer.Count();
            if (_rolloverMetric == RolloverMetric.ByteSize)
                if (bufferSize > _maxMetric)
                    throw new ArgumentOutOfRangeException(string.Format("The given buffer size: {0} was greater than the max sized defined: {1}"
                        , bufferSize, _maxMetric));

            if (((_rolloverMetric == RolloverMetric.ByteSize) &&
                    (bufferSize + _fileSizeBytes) > _maxMetric)
                || ((_rolloverMetric == RolloverMetric.RecordCount) &&
                    (_fileRecordCounter + 1) > _maxMetric))
            {
                Close();
                _fileName = RolloverName();
                _fileList.Add(new FileInfo(_fileName));
                rollover = true;
                _fileSizeBytes = 0;
                _fileRecordCounter = 0;
            }

            try
            {
                if (_maintainFilePtr)
                {
                    if (_sw == null)
                        _sw = new StreamWriter(_fileName);

                    if (!string.IsNullOrEmpty(_header)
                        && _fileSizeBytes == 0)
                        _sw.Write(_header);
                    _sw.Write(buffer);
                    _sw.Flush();
                }
                else
                {
                    using (StreamWriter sw = new StreamWriter(_fileName, true))
                    {
                        if (!string.IsNullOrEmpty(_header)
                            && _fileSizeBytes == 0)
                            sw.Write(_header);
                        sw.Write(buffer);
                    }
                }
                _fileSizeBytes += bufferSize;
                ++_fileRecordCounter;
            }
            catch
            {
                if (_maintainFilePtr && _sw != null)
                    _sw.Close();
                throw;
            }

            return rollover;
        }

        public void Dispose()
        {
            Close();
            if (_maintainFilePtr && _sw != null)
            {
                _sw.Flush();
                _sw.Close();
            }
        }

        public void Close()
        {
            try
            {
                if (_maintainFilePtr && _sw != null)
                {
                    if (!string.IsNullOrEmpty(_footer))
                        _sw.Write(_footer);
                    _sw.Flush();
                    _sw.Close();
                    _sw = null;
                }
                else
                {
                    using (StreamWriter sw = new StreamWriter(_fileName, true))
                    {
                        if (!string.IsNullOrEmpty(_footer))
                            sw.Write(_footer);
                        sw.Flush();
                    }
                }
            }
            catch
            {
                if (_maintainFilePtr && _sw != null)
                    _sw.Close();
                throw;
            }
        }

        public string CurrentFile
        {
            get { return _fileName; }
        }

        public FileInfo[] Files
        {
            get { return _fileList.ToArray(); }
        }
    }
}
