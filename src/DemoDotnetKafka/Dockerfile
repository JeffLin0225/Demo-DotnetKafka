# 1. 使用 .NET 8 SDK (既然要降級，Build 也要對齊)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# 2. 複製專案檔
COPY ["DemoDotnetKafka.csproj", "./"]
RUN dotnet restore "DemoDotnetKafka.csproj"

# 3. 複製所有程式碼並發布
COPY . .
RUN dotnet publish "DemoDotnetKafka.csproj" -c Release -o /app/publish

# 這裡必須用 aspnet 而不是 runtime，因為你有用 Web (Prometheus Metrics)
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .

# 5. 啟動點
ENTRYPOINT ["dotnet", "DemoDotnetKafka.dll"]