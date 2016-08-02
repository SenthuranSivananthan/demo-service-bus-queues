using System;

namespace Demos.Queues.Model
{
    [Serializable]
    public class MessagePayload
    {
        public string ReferenceID { get; set; }

        public string PayloadXML { get; set; }
    }
}
