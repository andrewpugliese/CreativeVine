using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ServiceModel;
using System.ServiceModel.Web;

using CV.Configuration;
using CV.Database;
using CV.Database.Sequence;
using CV.Logging;
using CV.Rest.Sequences;

namespace CV.Test.Sequence.Svc
{
    class Program
    {
        static void Main(string[] args)
        {
          //  DatabaseMgr dbMgr = new DatabaseMgr(ConfigurationMgr.GetNonEmptyValue("ConnectionKey"));
            using (ServiceHost host = new ServiceHost(typeof(SequenceDispatcher)))
            {
                host.Open();
                Console.WriteLine("Service host running......");

                Console.ReadLine();

                host.Close();
            }
        }
    }
}
