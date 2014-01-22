using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.SessionState;
using System.Web.Routing;
using System.ServiceModel.Activation;
using System.ServiceModel.Web;

using CV.Database;
using CV.Database.Sequence;
using CV.Configuration;
using CV.Logging;
using CV.Rest.Sequences;

namespace CV.Wcf.Rest.Sequence.Svc
{
    public class Global : System.Web.HttpApplication
    {
        static DatabaseMgr _dbMgr = null;
        static LoggingMgr _logMgr = null;
        static SequenceMgr _seqMgr = null;

        internal static DatabaseMgr DbMgr
        {
            get { return _dbMgr; }
        }

        internal static LoggingMgr LogMgr
        {
            get { return _logMgr; }
        }

        internal static SequenceMgr SeqMgr
        {
            get { return _seqMgr; }
        }

        protected void Application_Start(object sender, EventArgs e)
        {
            
             _dbMgr = new DatabaseMgr(ConfigurationMgr.GetNonEmptyValue("ConnectionKey"));
            _seqMgr = new SequenceMgr(_dbMgr, 100);
            SequenceDispatcher sd = new SequenceDispatcher(_dbMgr, _seqMgr);
            _logMgr = new LoggingMgr("BasicLogging");
            _logMgr.WriteToLog("ApplicationStarted");
        }

        protected void Session_Start(object sender, EventArgs e)
        {

        }

        protected void Application_BeginRequest(object sender, EventArgs e)
        {

        }

        protected void Application_AuthenticateRequest(object sender, EventArgs e)
        {

        }

        protected void Application_Error(object sender, EventArgs e)
        {
            if (_logMgr != null)
                _logMgr.WriteToLog(Server.GetLastError());
        }

        protected void Session_End(object sender, EventArgs e)
        {

        }

        protected void Application_End(object sender, EventArgs e)
        {
            _seqMgr.Dispose();
            _logMgr.WriteToLog("ApplicationEnded");
        }
    }
}