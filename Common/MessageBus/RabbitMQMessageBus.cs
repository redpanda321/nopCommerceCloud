using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

public class RabbitMQMessageBus : IMessageBus
{
    private readonly IConnection _connection;
    private readonly IModel _channel;

    public RabbitMQMessageBus(string hostName)
    {
        var factory = new ConnectionFactory() { HostName = hostName };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
    }

    public async Task PublishAsync(string topic, string message)
    {
        _channel.ExchangeDeclare(exchange: topic, type: ExchangeType.Fanout);
        var body = Encoding.UTF8.GetBytes(message);

        _channel.BasicPublish(exchange: topic, routingKey: "", basicProperties: null, body: body);
        await Task.CompletedTask;
    }

    public async Task SubscribeAsync(string topic, Func<string, Task> handleMessage)
    {
        _channel.ExchangeDeclare(exchange: topic, type: ExchangeType.Fanout);
        var queueName = _channel.QueueDeclare().QueueName;
        _channel.QueueBind(queue: queueName, exchange: topic, routingKey: "");

        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            await handleMessage(message);
        };
        _channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

        await Task.CompletedTask;
    }

    public async Task UnsubscribeAsync(string topic)
    {
        _channel.ExchangeDelete(exchange: topic);
        await Task.CompletedTask;
    }
}
