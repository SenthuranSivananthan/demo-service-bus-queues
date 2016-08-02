using System;

namespace Demos.Queues.Model
{
    [Serializable]
    public class MessageControl
    {
        public string ReferenceID { get; set; }
        public string PayloadLocation { get; set; }
    }
}
