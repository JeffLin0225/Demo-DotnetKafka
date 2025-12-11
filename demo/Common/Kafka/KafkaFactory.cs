namespace demo.Common.Kafka;
using Confluent.Kafka;

public interface IKafkaFactory
{
    IProducer<Null, string> CreateProducer();
    IConsumer<Null, string> CreateConsumer(string groupId);
}
public class KafkaFactory : IKafkaFactory , IDisposable
{
    private readonly ILogger<KafkaFactory> _logger;
    private readonly IConfiguration _configuration;
    private readonly Lazy<IProducer<Null, string>> _producer; // Lazy執行緒安全，只建立一次
    private bool _disposed =  false;
    
    public KafkaFactory(ILogger<KafkaFactory> logger,IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _producer = new Lazy<IProducer<Null, string>>(() =>
        {
            var config = new ProducerConfig
            {
                BootstrapServers =  _configuration["Kafka:BootstrapServers"],
                LingerMs = 20,
                BatchSize =  16384,
                Acks = Acks.All
            };
            return new ProducerBuilder<Null, string>(config).Build();
        });
    }

    public IProducer<Null, string> CreateProducer()
    {
        return _producer.Value;
    }

    public IConsumer<Null, string> CreateConsumer(string groupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _configuration["Kafka:BootstrapServers"],
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000,
        };
        return new ConsumerBuilder<Null, string>(config).Build();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            if (_producer.IsValueCreated)
            {
                _producer.Value.Dispose();
            }
        }
        _disposed = true;
    }
}