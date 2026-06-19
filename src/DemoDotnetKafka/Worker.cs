using DemoDotnetKafka.Service.Business;

namespace DemoDotnetKafka;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly SubscribeService  _subscribeService;

    public Worker(ILogger<Worker> logger, SubscribeService subscribeService)
    {
        _logger = logger;
        _subscribeService = subscribeService;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker 服務啟動...");
        await _subscribeService.StartListener(stoppingToken);
        _logger.LogWarning("Worker 服務已停止");
    }
    
    
}
