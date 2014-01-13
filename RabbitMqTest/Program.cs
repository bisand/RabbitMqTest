using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Framing.Impl.v0_9_1;

namespace RabbitMqTest
{
    class Program
    {
        private static string QUEUE_NAME = "Test";
        private static int _numberOfMessages = 100000;
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();
            factory.HostName = "aragorn";
            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            channel.QueueDeclare(QUEUE_NAME, true, false, false, null);
            var prop = channel.CreateBasicProperties();
            prop.SetPersistent(true);
            var generator = new RandomBufferGenerator(64 * 1024);
            for (int i = 0; i < _numberOfMessages; i++)
            {
                var buffer = generator.GenerateBufferFromSeed(1024);
                channel.BasicPublish("", QUEUE_NAME, prop, buffer);
            }

            //BasicGetResult result;

            //QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
            //channel.BasicConsume(QUEUE_NAME, false, consumer);
            //while ((result = channel.BasicGet(QUEUE_NAME, false)) != null) 
            //{
            //    var data = Encoding.UTF8.GetString(result.Body);
            //    channel.BasicAck(result.DeliveryTag, false);
            //}

            QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
            bool autoAck = false;
            String consumerTag = channel.BasicConsume(QUEUE_NAME, autoAck, consumer);
            BasicDeliverEventArgs result;
            while (consumer.Queue.Dequeue(1000, out result))
            {
                try
                {
                    byte[] body = result.Body;
                    // ... process the message
                    //onMessageReceived(body);
                    channel.BasicAck(result.DeliveryTag, false);
                }
                catch (OperationInterruptedException ex)
                {
                    // The consumer was removed, either through
                    // channel or connection closure, or through the
                    // action of IModel.BasicCancel().
                    break;
                }
            }
 
            
            channel.Close();
            connection.Close();
        }
    }
}
