using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

using CV.Configuration;
using CV.Logging;

namespace CV.Test.Tracing
{
    class Program
    {
        private static Random _random = new Random();
        static void Main(string[] args)
        {
            TestTrace();
        }

        public static void TestTrace()
        {
            FileLog backupFileLog = new FileLog("TestBackupLog", "", EventPriorities.All);

            LoggingMgr lm = new LoggingMgr(
                    new List<ILoggingTarget>() 
                    {
                        new WindowsEventLog( "Application", "LoggingTest", backupFileLog, EventPriorities.Critical ),
                    }, backupFileLog, TraceLevels.All);

            TraceLog log = new TraceLog();
            lm.TraceToWindow = true;

            int n = 0;
            DateTime start = DateTime.Now;
            while ((DateTime.Now - start).TotalSeconds < 600)
            {
                using (new LoggingContext("Running Trace Test"))
                {
                    lm.Trace(@"Number: " + n.ToString());
                    n++;

                    TraceDeep1(lm);

                    TraceLargeMessage(lm);

                    System.Threading.Thread.Sleep(1);
                }
            }
        }

        protected static void TraceDeep1(LoggingMgr lm)
        {
            using (new LoggingContext("TraceDeep1"))
            {
                System.Threading.Thread.Sleep(10);
                lm.Trace("Call To TraceDeep1");

                TraceDeep2(lm);
            }
        }

        protected static void TraceDeep2(LoggingMgr lm)
        {
            using (new LoggingContext("TraceDeep2"))
            {
                System.Threading.Thread.Sleep(10);
                lm.Trace("Call To TraceDeep2");
                TraceDeep3(lm);
            }
        }

        protected static void TraceDeep3(LoggingMgr lm)
        {
            using (new LoggingContext("TraceDeep3"))
            {
                System.Threading.Thread.Sleep(10);
                lm.Trace("Call To TraceDeep3.");
            }
        }

        protected static void TraceLargeMessage(LoggingMgr lm)
        {

            lm.Trace("RANDOM MESSAGE: ".PadRight(_random.Next(50, 200), (char)_random.Next(65, 90)));
        }

    }
}
