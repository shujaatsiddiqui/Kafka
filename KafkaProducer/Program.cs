// See https://aka.ms/new-console-template for more information
using KafkaProducer;

Console.WriteLine("Enter Client Id");
string clientId = Console.ReadLine();
//await new ProducerManager().ProduceAsync(clientId);

await new ProducerManager().BatchProduceAsync(clientId);
Console.WriteLine("Hello, World!");
