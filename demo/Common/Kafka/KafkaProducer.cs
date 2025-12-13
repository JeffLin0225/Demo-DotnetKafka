using Confluent.Kafka;

namespace demo.Common.Kafka;
using demo.Common.Kafka;

public interface IKafkaProducerService
{
    Task SendAsync(string topic , string message);
}
public class KafkaProducer : IKafkaProducerService
{
    private readonly ILogger<KafkaProducer> _logger;
    private readonly IKafkaFactory _kafkaFactory;

    public KafkaProducer(ILogger<KafkaProducer> logger , IKafkaFactory kafkaFactory)
    {
        _logger = logger;
        _kafkaFactory = kafkaFactory;
    }
    
    /// <summary>
    /// Producer
    /// </summary>
    public async Task SendAsync(string topic , string message)
    {
        var producer = _kafkaFactory.CreateProducer();
    
        // 發送等待
        await producer.ProduceAsync(topic, new Message<Null, string>{Value = message});
    }
}