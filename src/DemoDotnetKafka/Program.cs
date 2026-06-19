using DemoDotnetKafka;
using DemoDotnetKafka.DiCollection;


var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaCollection(builder.Configuration); // Kafka基建
builder.Services.AddBusinessCollection(); // 業務邏輯

builder.Services.AddHostedService<Worker>(); // 生命週期

var host = builder.Build();
host.Run();
