using Microsoft.ServiceBus.Messaging;
using Demos.Queues.Model;
using System;
using System.Configuration;
using System.Threading;

namespace Demos.Queues
{
    class Program
    {
        static void Main(string[] args)
        {
            ListenForMessages();
            SendMessages();

            Console.ReadLine();
        }

        public static QueueClient CreateQueueClient()
        {
            return QueueClient.CreateFromConnectionString(ConfigurationManager.AppSettings["ServiceBusQueue.ConnectionString"]);
        }

        public static async void SendMessages()
        {
            var queueClient = CreateQueueClient();

            var random = new Random();
            var referenceIdCounter = 1;

            while (true)
            {
                var ctrlMessage = new MessageControl
                {
                    ReferenceID = string.Format("{0:#}", referenceIdCounter),
                    PayloadLocation = "https://blah.com"
                };

                var message = new BrokeredMessage(ctrlMessage);
                queueClient.Send(message);

                referenceIdCounter++;

                Thread.Sleep(random.Next(500, 5000));
            }
        }

        public static async void ListenForMessages()
        {
            var queueClient = CreateQueueClient();

            while (true)
            {
                Console.WriteLine("Waiting for next message");
                var message = await queueClient.ReceiveAsync();
                try
                {
                    var messageControl = message.GetBody<MessageControl>();

                    Console.WriteLine("Received Message: Reference ID: {0}", messageControl.ReferenceID);
                    message.Complete();
                }
                catch (Exception e)
                {
                    message.Abandon();
                    Console.WriteLine(e.StackTrace);
                }
            }
        }
    }
}
