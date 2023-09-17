﻿
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaTest;
using System.Collections.Concurrent;

public class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("Please Enter BootstrapServers? (separate them with ',')");
        var BootstrapServers = Console.ReadLine();
        if (string.IsNullOrEmpty(BootstrapServers))
            BootstrapServers = "localhost:9092";
        Console.WriteLine("Please Enter Number Of Topics?");
        var numberOfTopics = Console.ReadLine();
        Console.WriteLine("Please Enter Number Of Partitions For Each Topic? (separate them with ',')");
        var numberOfPartitions = Console.ReadLine();
        Console.WriteLine("Please Enter Replication Factor For Each Topic? (separate them with ',')");
        var replicationFactors = Console.ReadLine();
        Console.WriteLine("Please Enter App Run Mode ,Producer:1,ConsumerMode:2 ,Both:3?");
        string Mode = Console.ReadLine();
        Mode = string.IsNullOrWhiteSpace(Mode) ? "3" : Mode;
        var topicPrefixName = "Abed_Topic";
        Console.WriteLine("Please Enter Number of Workers");
        string Worker = Console.ReadLine();
        int worker = string.IsNullOrWhiteSpace(Worker) ? 8 : Convert.ToInt32(Worker);

        int number = string.IsNullOrWhiteSpace(numberOfTopics) ? 1 : Convert.ToInt32(numberOfTopics);

        string[] partitions = numberOfPartitions.Split(',');
        if (partitions.Length < number)
        {
            GenerateDefaultPartitions(ref numberOfPartitions, number, ref partitions);
        }

        string[] replicas = replicationFactors.Split(',');
        if (replicas.Length < number)
        {
            GenerateDefaultReplicas(ref replicationFactors, number, ref replicas);

        }

        for (int i = 1; i <= number; i++)
        {
            try
            {
                await CreateTopicIfNotExists(BootstrapServers, topicPrefixName + "_" + i.ToString(), Convert.ToInt32(partitions[i - 1]) < 1 ? 1 : Convert.ToInt32(partitions[i - 1]),
                Convert.ToInt32(replicas[i - 1]) < 1 ? 1 : Convert.ToInt32(replicas[i - 1]));
            }
            catch (IndexOutOfRangeException)
            {
                await CreateTopicIfNotExists(BootstrapServers, topicPrefixName + "_" + i.ToString(), 1, 1);
            }
        }


        var messageProducer = new MessageProducer(BootstrapServers);
        var messageConsumer = new MessageConsumer(BootstrapServers);

        if (Mode == "1") await messageProducer.load(topicPrefixName, number, worker);
        else if (Mode == "2") await messageConsumer.load(topicPrefixName, number, worker, partitions);
        else if (Mode == "3")
        {
            Task[] tasks = { messageProducer.load(topicPrefixName, number, worker), messageConsumer.load(topicPrefixName, number, worker, partitions) };
            Task.WaitAll(tasks);
        }



        Console.WriteLine("Press any key to exit...");
        Console.ReadLine();
    }

    private static void GenerateDefaultReplicas(ref string? replicationFactors, int number, ref string[] replicas)
    {
        if (string.IsNullOrEmpty(replicationFactors))
            replicationFactors = string.Join(",", Enumerable.Repeat("1", number - (replicas.Length - 1)));
        else if (replicationFactors.EndsWith(','))
            replicationFactors += string.Join(",", Enumerable.Repeat("1", number - (replicas.Length-1)));
        else
            replicationFactors = string.Concat(replicationFactors, ",", string.Join(",", Enumerable.Repeat("1", number - replicas.Length)));
        replicas = replicationFactors.Split(',');
    }

    private static void GenerateDefaultPartitions(ref string? numberOfPartitions, int number, ref string[] partitions)
    {
        if (string.IsNullOrEmpty(numberOfPartitions))
            numberOfPartitions = string.Join(",", Enumerable.Repeat("1", number - (partitions.Length - 1)));
        else if (numberOfPartitions.EndsWith(','))
            numberOfPartitions += string.Join(",", Enumerable.Repeat("1", number - (partitions.Length -1)));
        else
            numberOfPartitions = string.Concat(numberOfPartitions, ",", string.Join(",", Enumerable.Repeat("1", number - partitions.Length)));
        partitions = numberOfPartitions.Split(',');
    }

    private static async Task CreateTopicIfNotExists(string bootstrapServers, string topicName, int partitions, int replicationFactor)
    {
        var adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        };

        using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
        {
            try
            {
                await adminClient.DeleteTopicsAsync(new List<string>()
                {
                    topicName
                });
                Console.WriteLine("Deleting Existing topics ...");
                await Task.Delay(2000);
            }
            catch (Exception)
            {
                Console.WriteLine("Creating new topic ...");
            }


            var topicSpecification = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = partitions,
                ReplicationFactor = (short)replicationFactor
            };

            await adminClient.CreateTopicsAsync(new List<TopicSpecification> { topicSpecification });
            Console.WriteLine($"Topic '{topicName}' created with {partitions} partitions and replication factor {replicationFactor}.");
        }
    }
}

