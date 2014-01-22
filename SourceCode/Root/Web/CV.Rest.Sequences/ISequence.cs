using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;

namespace CV.Rest.Sequences
{
    [ServiceContract]
    public interface ISequence
    {
        #region "GET OPERATIONS"

        [OperationContract]
        [WebGet(UriTemplate = "")]
        List<SequenceItem> GetKeys();

        [OperationContract]
        [WebGet(UriTemplate = "{sequenceKey}")]
        SequenceItem GetKey(string SequenceKey);

        #endregion

        #region "PUT OPERATIONS"

        [OperationContract]
        [WebInvoke(UriTemplate = "{sequenceKey}", Method = "PUT")]
        bool SetKey(string SequenceKey);
        [OperationContract]
        [WebInvoke(UriTemplate = "uniqueid/{sequenceKey}", Method = "PUT")]
        long GenerateUniqueIdRange(string SequenceKey, int RangeSize = 1);
        [OperationContract]
        [WebInvoke(UriTemplate = "generate/{sequenceKey}", Method = "PUT")]
        long GenerateSequence(string SequenceKey);

        #endregion

        #region "DELETE OPERATIONS"

        [OperationContract]
        [WebInvoke(UriTemplate = "{sequenceKey}", Method = "DELETE")]
        bool DeleteKey(string SequenceKey);

        #endregion

        #region "POST OPERATIONS"

        [OperationContract]
        [WebInvoke(UriTemplate = "", Method = "POST")]
        SequenceItem CreateKey();

        #endregion
    }
}
