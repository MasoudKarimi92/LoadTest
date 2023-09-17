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
            BootstrapServers = _bootstrapServers
        };

        using (var producer = new ProducerBuilder<string, string>(
           config).Build())
        {
            int counter = 0;
            while (!Console.KeyAvailable)
            {
                var message = $"test_kafka_{topic}_{Guid.NewGuid().ToString()}";
                producer.Produce(topic, new Message<string, string> { Key = null, Value = message },
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

            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}
