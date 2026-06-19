using Confluent.Kafka;
using DemoDotnetKafka.Common.Kafka;

namespace DemoDotnetKafka.Service;

public class DataReceiver
{
    private readonly ILogger<DataReceiver> _logger;
    private readonly IKafkaFactory  _kafkaFactory;
    private readonly string _topic;
    private readonly string _groupId;

    private readonly int BatchSize = 100;
    private readonly TimeSpan BatchTimeout = TimeSpan.FromMilliseconds(500);

    public DataReceiver(ILogger<DataReceiver> logger, IKafkaFactory kafkaFactory , IConfiguration configuration)
    {
        _logger = logger;
        _kafkaFactory = kafkaFactory;
        
        _topic = configuration["Kafka:InputTopic"] ?? throw new NullReferenceException("找不到 Kafka:InputTopic");
        _groupId = configuration["Kafka:ConsumerGroupId"]  ?? throw new NullReferenceException("找不到 Kafka:ConsumerGroupId");
    }

    public async Task StartConsumerListener(Func<List<string>, Task> processLogic, CancellationToken stoppingToken)
    {
        // using 其實就是 try-with-resource
        using var consumer = _kafkaFactory.CreateConsumer(_groupId);
        consumer.Subscribe(_topic);
        
        _logger.LogInformation("開始訂閱{topic}",  _topic);
        
        // ConsumeResult 就是完整 kafka訊息的 MetaData , Null 部分決定 Partition
        var buffer = new List<ConsumeResult<Null, string>>(BatchSize);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                FillBuffer(consumer, buffer, stoppingToken);
                if (buffer.Count == 0)
                {
                    await Task.Delay(50, stoppingToken);
                    continue;
                }

                try
                {
                    // 卸貨
                    // buffer 轉 List 
                    var messageList = buffer.Select(x => x.Message.Value).ToList();
                    
                    // 給傳入要用的方法
                    await processLogic(messageList);
                    consumer.Commit(buffer.Last());
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "本次批次處理失敗");
                    await Task.Delay(1000, stoppingToken); // 避免失敗太快清除
                    buffer.Clear();
                }
                finally
                {
                    buffer.Clear();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // 這是正常的關閉Consumer流程
            _logger.LogWarning("接收到停止訊號，DataReceiver 正常關閉。");
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Consumer 發生嚴重錯誤，停止listener");
            throw;
        }
         
    }

    private void FillBuffer(IConsumer<Null, string> consumer, List<ConsumeResult<Null,string>> buffer, CancellationToken stoppingToken)
    {
        var startTime = DateTime.Now;
        
        // 當 buffer容量大小 <  規定的大小 
        while (buffer.Count < BatchSize) 
        {
            // 1. 計算「剩餘的等待時間」
            var elapsed = DateTime.Now - startTime;
            var remaining = BatchTimeout - elapsed;
            
            // 2. 時間到 [開車] ( 但buffer 還沒滿 )
            if (remaining <= TimeSpan.Zero)  break; 
            
            // 阻塞機制：只等待「剩餘時間」
            var result = consumer.Consume(remaining); // 把 kafka 資料拿下來

            // 避免kafka資料被加到 buffer裡面 :
            // 1. 是Null 
            // 2. 是Kafka好心提醒的 EOF資料 true 
            if (result != null && !result.IsPartitionEOF)
            {
                // 資料加入到 buffer 
                buffer.Add(result);
            }
        }
    }
}