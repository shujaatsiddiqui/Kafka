using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    internal class Consumer
    {
        string topic = "demoWithPartition";
        public void Start()
        {
            var config = new ConsumerConfig
            {                
                BootstrapServers = "172.26.104.98:9092",
                ClientId = "Consumer" + DateTime.Now.ToString(),
                GroupId = "Group1",
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            using (var consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                consumer.Subscribe(topic);
                int count = 1;
                while (count <= 500)
                {
                    var result = consumer.Consume();
                    Console.WriteLine($"{config.ClientId}: connected");
                    Console.WriteLine($"Value: {result.Message.Value}");
                    Console.WriteLine($"Partition: {result.Partition}");
                    Console.WriteLine($"Key: {result.Message.Key}");
                    Console.WriteLine($"=========================");
                    count++;
                    Thread.Sleep(500);
                }
                consumer.Close();
            }
        }
    }
}
