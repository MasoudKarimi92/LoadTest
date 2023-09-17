using Confluent.Kafka;

namespace KafkaTest;
public class MessageConsumer
{

    private readonly string _bootstrapServers;

    public MessageConsumer(string bootstrapServers)
    {
        _bootstrapServers = bootstrapServers;
    }

    public async Task load(string topicName, int number, int workers)
    {
        List<int> keys = Enumerable.Range(1, number).ToList();
        await Parallel.ForEachAsync(keys, new ParallelOptions() { MaxDegreeOfParallelism = workers },
        (item, cancelToken) =>
        {
            return Consume(topicName + "_" + item.ToString());
        });
    }


    private async ValueTask Consume(string topic)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = topic + "_Group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(
            config).SetLogHandler((_, message) => LogCallBack(message, config)).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                int counter = 0;
                while (true)
                {
                    Interlocked.Increment(ref counter);
                    var cr = consumer.Consume(cts.Token);
                    if (counter % 1000 == 0)
                        Console.WriteLine($"Consumed event from topic {topic} value {cr.Message.Value}");

                }
            }

            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed.
            }
            catch (Exception)
            {

            }
            finally
            {
                consumer.Close();
            }
        }
    }

    private static void LogCallBack(LogMessage message, ConsumerConfig config)
    {
        if (message.Level < SyslogLevel.Notice)
        {
            Console.WriteLine($"Error occurred | Level: {message.Level} | Message : {message.Message}");
        }
    }
}
