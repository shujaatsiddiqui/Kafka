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
        string topic = "demoWithPartition";
        public async Task ProduceAsync(string clientId)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "172.26.104.98:9092",
                ClientId = clientId
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
    }
}
