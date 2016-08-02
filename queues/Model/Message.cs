using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Demos.Queues.Model
{
    public class Message
    {
        public MessageControl Control { get; set; }
        public MessagePayload Payload { get; set; }
    }
}
