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
    private readonly KafkaFactory _kafkaFactory;

    public KafkaProducer(ILogger<KafkaProducer> logger , KafkaFactory kafkaFactory)
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
        try
        {
            // 發送等待
            await producer.ProduceAsync(topic, new Message<Null, string>{Value = message}); 
        }
        catch (Exception e)
        {
            _logger.LogError("Kafka SendAsync Error: {e}" , e.Message);
        }
    }
}