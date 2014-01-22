using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CV.Rest.Sequences
{
    public class SequenceItem
    {
        public string Key { get; set; }
        public long KeyValue { get; set; }
        public long? MaxValue { get; set; }
        public long? RolloverValue { get; set; }
    }
}
