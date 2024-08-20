using Confluent.Kafka;

public class KafkaMessageBus : IMessageBus
{
    private readonly IProducer<string, string> _producer;
    private readonly IConsumer<string, string> _consumer;

    public KafkaMessageBus(string brokerList, string groupId)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = brokerList };
        var consumerConfig = new ConsumerConfig
        {
            GroupId = groupId,
            BootstrapServers = brokerList,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    }

    public async Task PublishAsync(string topic, string message)
    {
        await _producer.ProduceAsync(topic, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = message });
    }

    public async Task SubscribeAsync(string topic, Func<string, Task> handleMessage)
    {
        _consumer.Subscribe(topic);

        await Task.Run(() =>
        {
            while (true)
            {
                var consumeResult = _consumer.Consume();
                if (consumeResult != null)
                {
                    handleMessage(consumeResult.Message.Value);
                }
            }
        });
    }

    public async Task UnsubscribeAsync(string topic)
    {
        _consumer.Unsubscribe();
        await Task.CompletedTask;
    }
}
