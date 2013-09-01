using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace CV.Files
{
    public class FileRolloverMgr
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
        List<FileInfo> _fileList = new List<FileInfo>();

        public FileRolloverMgr(string filenamePrefix
            , RolloverMetric rolloverMetric = RolloverMetric.ByteSize
            , int maxMetric = 1024000
            , string filenameExtension = "txt")
        {
            if (string.IsNullOrEmpty(filenamePrefix))
                throw new ArgumentNullException("FilenamePrefix cannot be empty");

            _filenamePrefix = filenamePrefix;
            _filenameExtension = filenameExtension;
            _rolloverMetric = rolloverMetric;
            _maxMetric = (_rolloverMetric == RolloverMetric.ByteSize) ? Math.Max(maxMetric, 1024) : Math.Max(maxMetric, 1);
            _fileName = RolloverName();
        }

        string RolloverName()
        {
            return string.Format("{0}.{1}.{2}"
                , _filenamePrefix
                , ++_rolloverCount
                , _filenameExtension);
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
                _fileName = RolloverName();
                _fileList.Add(new FileInfo(_fileName));
                rollover = true;
                _fileSizeBytes = 0;
                _fileRecordCounter = 0;
            }
            using (StreamWriter sw = new StreamWriter(_fileName, true))
            {
                sw.Write(buffer);
                _fileSizeBytes += bufferSize;
                ++_fileRecordCounter;
            }
            return rollover;
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
