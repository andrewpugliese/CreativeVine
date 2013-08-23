using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CV.Global
{
    public class ExceptionMgr : Exception
    {
        string _source = null;
        Exception _exception = null;
        long? _referenceNumber = null;

        public ExceptionMgr(string source, Exception exception)
        {
            _exception = exception;
            if (!string.IsNullOrEmpty(source))
            {
                _source = source;
                this.Source = _source;
            }
        }

        public override string ToString()
        {
            return _referenceNumber.HasValue 
                ? string.Format("Ref: {0}", _referenceNumber.Value) : string.Empty 
                + _exception != null ? ExceptionToString(_exception) : string.Empty;
        }

        public static string ExceptionToString(Exception exc)
        {
            return string.Format("Msg: {0}{3}; Src: {1}{3}; Trc: {2}{3}"
                , exc.Source
                , exc.Message
                , exc.StackTrace
                , Environment.NewLine)
                + exc.InnerException != null ? "    Inner:" 
                    + ExceptionToString(exc.InnerException)
                : string.Empty;
        }
    }
}
