using demo.Common.Kafka;

namespace demo.Service;

public class DataSender
{
    private readonly ILogger<DataSender> _logger;
    private readonly IKafkaProducerService _kafkaProducerService;
    private readonly string _topic;

    public DataSender(ILogger<DataSender> logger, IKafkaProducerService kafkaProducerService, IConfiguration configuration)
    {
        _logger = logger;
        _kafkaProducerService = kafkaProducerService;
        _topic = configuration["Kafka:OutputTopic"] ?? throw new ArgumentNullException("設定檔案找不到 Topic");
    }

    public async Task HighPerfSendData(List<string> messageList)
    {
        int messageCount = messageList.Count;
        var taskList = new List<Task>(messageCount);

        try
        {
            foreach (var message in messageList)
            {
                taskList.Add(_kafkaProducerService.SendAsync(_topic,message));
            }
        
            await Task.WhenAll(taskList);
            _logger.LogInformation("Kafka Send Data SUCCESS! Count:{messageCount}", messageCount);
        }
        catch (Exception e)
        {
            _logger.LogError(e , "Kafka Send Data ERROR... Count:{messageCount}", messageCount);
        }
        
    } 
}