namespace demo.Service.Business;

public class SubscribeService
{
    private readonly ILogger<SubscribeService> _logger;
    private readonly DataReceiver _dataReceiver;
    private readonly DataSender _dataSender;

    public SubscribeService(ILogger<SubscribeService> logger, DataReceiver dataReceiver, DataSender dataSender)
    {   
        _logger = logger;
        _dataReceiver = dataReceiver;
        _dataSender = dataSender;
    }

    public async Task StartListener(CancellationToken cancellationToken)
    {
        
        await _dataReceiver.StartConsumerListener(ProcessData, cancellationToken);
    }

    // 處理收下來的邏輯
    private async Task ProcessData(List<string> batchData)
    {
        try
        {
            _logger.LogInformation("發送批次訊息，數量：{batchDataCount} ", batchData.Count);
            
            // 這邊沒有業務邏輯，無腦丟Topic 
            await _dataSender.HighPerfSendData(batchData);
        }
        catch (Exception e)
        {
            _logger.LogError(e ,"發送批次訊息失敗，數量：{batchDataCount} " , batchData.Count);
            throw;
        }
        
    }
}