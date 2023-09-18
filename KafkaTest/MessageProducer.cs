using Confluent.Kafka;
using System.Diagnostics.Metrics;

namespace KafkaTest;
public class MessageProducer
{
    private readonly string _bootstrapServers;

    public MessageProducer(string bootstrapServers)
    {
        _bootstrapServers = bootstrapServers;
    }

    public async Task load(string topicName, int number, int worker)
    {
        List<int> keys = Enumerable.Range(1, number).ToList();
        await Parallel.ForEachAsync(keys, new ParallelOptions() { MaxDegreeOfParallelism = worker },
        (item, cancelToken) =>
        {
            return BuildMessage(topicName + "_" + item.ToString());
        });

    }

    private async ValueTask BuildMessage(string topic)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            QueueBufferingMaxMessages = 1000_000_000
        };

        using (var producer = new ProducerBuilder<string, string>(
           config).Build())
        {
            try
            {
                int counter = 0;
                while (!Console.KeyAvailable)
                {
                    var message = $"test_kafka_{topic}_{Guid.NewGuid().ToString()}";
                    producer.Produce(topic, new Message<string, string> { Value = message },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Interlocked.Increment(ref counter);
                                if (counter % 1000 == 0)
                                    Console.WriteLine($"Produced event to topic {topic}: value = {message}");

                            }
                        });
                }
                producer.Flush(TimeSpan.FromSeconds(5));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in producer: {ex}");
            }
        }
    }
}
