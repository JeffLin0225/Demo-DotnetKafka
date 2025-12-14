using demo.Common.Kafka;
using demo.Service;

namespace demo.DiCollection;

public static class KafkaCollection
{
    public static IServiceCollection AddKafkaCollection(this IServiceCollection services, IConfiguration config)
    {
        services.AddSingleton<IKafkaFactory, KafkaFactory>();
        services.AddSingleton<IKafkaProducerService, KafkaProducer>();
        
        // 應用層 (收發)
        // 注意 namespace 是 demo.Service.Kafka
        services.AddSingleton<DataReceiver>();
        services.AddSingleton<DataSender>();

        return services;
    }
}