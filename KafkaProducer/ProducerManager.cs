using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer
{
    internal class ProducerManager
    {

        public async Task ProduceAsync(string clientId)
        {
            string topic = "demoWithPartition";
            var config = new ProducerConfig
            {
                BootstrapServers = "172.26.104.98:9092",
                ClientId = clientId,
                MessageSendMaxRetries = 3
            };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                for (var i = 0; i < 50; ++i)
                {
                    var value = $"Hello World {i}";

                    var message = new Message<string, string>()
                    {
                        Value = value,
                        Key = clientId //i <= 30 ? "1-30" : (i > 30 && i <= 60) ? "31-60" : (i > 60 && i <= 90) ? "61-90" : ""
                    };
                    var result = await producer.ProduceAsync(topic, message);
                    Console.WriteLine($"Value: {value}");
                    Console.WriteLine($"Partition: {result.Partition}");
                    Console.WriteLine($"Key: {result.Key}");
                    Console.WriteLine($"=========================");
                    Thread.Sleep(500);
                }
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }

        public async Task BatchProduceAsync(string clientId)
        {
            string topic = "Batching";
            var config = new ProducerConfig
            {
                BootstrapServers = "172.26.99.250:9092",
                ClientId = clientId,

                //linger.ms refers to the time to wait before sending messages out to Kafka. It defaults to 0, which the system interprets as 'send messages as soon as they are ready to be sent'
                LingerMs = TimeSpan.FromSeconds(5).TotalMilliseconds,

                // batch.size measures batch size in total bytes instead of the number of messages. It controls how many bytes of data to collect before sending messages to the Kafka broker.
                // Set this as high as possible, without exceeding available memory
                BatchSize = 10 * 1024

                // In batch either what ever occurs first either ms or batch size, kafka producer will send the message

            };

            //for (int i = 0; i < 10; i++)
            //{
            //    using (var producer = new ProducerBuilder<string, string>(config).Build())
            //    {
            //        var value = $"Hello World {i}";

            //        var message = new Message<string, string>()
            //        {
            //            Value = value,
            //            Key = clientId
            //        };
            //        var result = producer.ProduceAsync(topic, message);
            //        producer.Flush();
            //    }
            //}

            var producer = new ProducerBuilder<string, string>(config).Build();
            for (var i = 0; i < 50; ++i)
            {
                var value = $"Hello World {i}";
                var message = new Message<string, string>()
                {
                    Value = value,
                    Key = clientId
                };

                // adding await would be as same as calling a synchronous function.
                producer.ProduceAsync(topic, message);
            }

            // adding a thread sleep because calling producer.flush() will send the batch immediately.
            Thread.Sleep((int)config.LingerMs);
            producer.Flush();

        }
    }
}
