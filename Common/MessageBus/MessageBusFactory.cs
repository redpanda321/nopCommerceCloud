public class MessageBusFactory
{
    public static IMessageBus Create(string busType, string connectionString, string groupId = null)
    {
        return busType.ToLower() switch
        {
            "kafka" => new KafkaMessageBus(connectionString, groupId),
            "rabbitmq" => new RabbitMQMessageBus(connectionString),
            _ => throw new ArgumentException("Invalid message bus type")
        };
    }
}
