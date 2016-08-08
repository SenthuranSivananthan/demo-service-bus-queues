using Microsoft.ServiceBus.Messaging;
using Demos.Queues.Model;
using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;

namespace Demos.Queues
{
    class Program
    {
        static void Main(string[] args)
        {
            var task = Task.Run(
                async () =>
                {
                    await (new Program()).Execute();
                });

            task.Wait();
        }
        
        public async Task Execute()
        {
            var cancellationToken = new CancellationTokenSource();

            var listenTask = Task.Run(async () => { await ListenForMessages(cancellationToken.Token); });
            var sendTask = Task.Run(async () => { await SendMessages(cancellationToken.Token); });

            Console.ReadLine();

            cancellationToken.Cancel();

            await listenTask;
            await sendTask;
        }

        public static async Task SendMessages(CancellationToken cancellationToken)
        {
            var messageFactory = MessagingFactory.Create(
                ConfigurationManager.AppSettings["ServiceBusQueue.Address"],
                new MessagingFactorySettings
                {
                    TransportType = TransportType.Amqp,
                    TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(ConfigurationManager.AppSettings["ServiceBusQueue.SAS.Send.PolicyName"], ConfigurationManager.AppSettings["ServiceBusQueue.SAS.Send.Key"])
                });

            var messageSender = await messageFactory.CreateMessageSenderAsync(ConfigurationManager.AppSettings["ServiceBusQueue.QueueName"]);

            cancellationToken.Register(
                    async () =>
                    {
                        Console.WriteLine("Cancelled:  SendMessages(...)");
                        await messageSender.CloseAsync();
                        await messageFactory.CloseAsync();
                    }
                );

            var random = new Random();
            var referenceIdCounter = 1;

            while (!cancellationToken.IsCancellationRequested)
            {
                var message = PrepareOutgoingMessage(referenceIdCounter);
                messageSender.Send(message);
                Console.WriteLine(string.Format("Message Sent: {0}", message.MessageId));

                referenceIdCounter++;

                Thread.Sleep(random.Next(500, 5000));
            }
        }

        public static async Task ListenForMessages(CancellationToken cancellationToken)
        {
            var stopListeningForNewMessages = new TaskCompletionSource<bool>();

            var messageFactory = MessagingFactory.Create(
                ConfigurationManager.AppSettings["ServiceBusQueue.Address"],
                new MessagingFactorySettings
                {
                    TransportType = TransportType.Amqp,
                    TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(ConfigurationManager.AppSettings["ServiceBusQueue.SAS.Listen.PolicyName"], ConfigurationManager.AppSettings["ServiceBusQueue.SAS.Listen.Key"])
                });

            var messageReceiver = await messageFactory.CreateMessageReceiverAsync(ConfigurationManager.AppSettings["ServiceBusQueue.QueueName"], ReceiveMode.PeekLock);

            // ensure that the messaging factory and message receiver are closed when 
            // the listening task is cancelled
            cancellationToken.Register(
                    async () =>
                    {
                        Console.WriteLine("Cancelled:  ListenForMessages(...)");
                        await messageReceiver.CloseAsync();
                        await messageFactory.CloseAsync();
                        stopListeningForNewMessages.SetResult(true);
                    }
                );

            messageReceiver.OnMessageAsync(
                    async message =>
                    {
                        var isMessageProcessed = ProcessIncomingMessage(message);

                        // If the cancellation token is received, then don't update the queue since the connection
                        // will be closed.  The timeout on the token will expire (configured on the Queue) and the
                        // message can be reprocessed.  Your application code will need to handle these messages
                        // as idempotent.
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            if (isMessageProcessed)
                            {
                                await message.CompleteAsync();
                            }
                            else
                            {
                                await message.AbandonAsync();
                            }
                        }
                    },
                    new OnMessageOptions {  AutoComplete = false, MaxConcurrentCalls = 1 }
                );

            await stopListeningForNewMessages.Task;
        }

        private static BrokeredMessage PrepareOutgoingMessage(int referenceID)
        {
            var ctrlMessage = new MessageControl
            {
                ReferenceID = string.Format("{0:#}", referenceID),
                PayloadLocation = "https://blah.com"
            };

            return new BrokeredMessage(ctrlMessage);
        }

        private static bool ProcessIncomingMessage(BrokeredMessage message)
        {
            var messageControl = message.GetBody<MessageControl>();
            Console.WriteLine("Received Message: {0}, Reference ID: {1}", message.MessageId, messageControl.ReferenceID);
            return true;
        }
    }
}
