using demo.Service.Business;

namespace demo.DiCollection;

public static class BusinessCollection
{
    public static IServiceCollection AddBusinessCollection(this IServiceCollection services)
    {
        // 這裡註冊我們剛剛抽出來的邏輯服務
        services.AddSingleton<SubscribeService>();
        
        // 未來如果有 DB Service 也可以放這，或再開一個 AddDatabase()
        return services;
    }
}