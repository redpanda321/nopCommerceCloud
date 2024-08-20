public interface IMessageBus
{
    Task PublishAsync(string topic, string message);
    Task SubscribeAsync(string topic, Func<string, Task> handleMessage);
    Task UnsubscribeAsync(string topic);
}
