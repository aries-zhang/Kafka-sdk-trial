using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Beging sending messages..");

            var host = ConfigurationManager.AppSettings["kafka_host"];
            var topic = ConfigurationManager.AppSettings["kafka_topic"];

            var options = new KafkaOptions(new Uri(host));
            var router = new BrokerRouter(options);

            var client = new Producer(router);

            for(int i = 0; i < 1000; i++)
            {
                var message = "hello kafka " + i;

                client.SendMessageAsync(topic, new[] { new Message(message) });
                Console.WriteLine("message sent: " + message);
            }

            Console.WriteLine("Press any key to continue..");
            Console.Read();
        }
    }
}
