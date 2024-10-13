using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace MongoBalancer;

public sealed class ReadBalancer : IReadBalancer
{
    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(30);
    private readonly ImmutableDictionary<string, MongoBalancer> _balancers;
    private readonly CancellationTokenSource _cts = new();
    private readonly ILogger<ReadBalancer> _logger;
    private readonly LruCache<string, MongoUrl> _mongoUrlCache;
    private readonly Channel<MonitorCommand> _monitorChannel;

    public ReadBalancer(IEnumerable<MongoUrl> mongoUrls, TimeSpan maxStaleness, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ReadBalancer>();
        _mongoUrlCache = new LruCache<string, MongoUrl>(1024, CacheDuration);

        _balancers = mongoUrls
            .AsParallel()
            .Select(url => new MongoBalancer(MongoClientSettings.FromUrl(url), maxStaleness, loggerFactory))
            .ToImmutableDictionary(b => b.ConnectionString, b => b);

        _monitorChannel = Channel.CreateUnbounded<MonitorCommand>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = true
        });

        Task.Factory.StartNew(MonitorAndAdjustReadPreference, _cts.Token, TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _cts.CancelAsync();
            _monitorChannel.Writer.Complete();

            await DisposeResourceAsync(_cts);
            await DisposeResourceAsync(_mongoUrlCache);

            foreach (var balancer in _balancers.Values) await balancer.DisposeAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during disposal");
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task<ReadPreference> GetReadPreference(string connectionString,
        CancellationToken cancellationToken = default)
    {
        var normalizedConnectionString = await NormalizeConnectionString(connectionString);

        if (_balancers.TryGetValue(normalizedConnectionString, out var balancer))
            return await balancer.GetReadPreferenceAsync(cancellationToken);

        _logger.LogWarning("No balancer found for connection string: {ConnectionString}", normalizedConnectionString);
        return ReadPreference.Primary;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask DisposeResourceAsync(object resource)
    {
        return resource switch
        {
            IAsyncDisposable asyncDisposable => asyncDisposable.DisposeAsync(),
            IDisposable disposable => new ValueTask(Task.Run(disposable.Dispose)),
            _ => ValueTask.CompletedTask
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task<MongoUrl?> GetOrCreateMongoUrl(string connectionString)
    {
        return await _mongoUrlCache.GetOrAdd(connectionString, _ =>
        {
            try
            {
                return Task.FromResult(new MongoUrl(connectionString))!;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating MongoUrl for connection string: {ConnectionString}",
                    connectionString);
                return Task.FromResult<MongoUrl?>(null);
            }
        });
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task<string> NormalizeConnectionString(string connectionString)
    {
        try
        {
            var url = await GetOrCreateMongoUrl(connectionString);
            if (url is null) return connectionString;

            if (!string.IsNullOrEmpty(url.ReplicaSetName)) return url.ReplicaSetName;
            if (url.Server is not null) return url.Server.Host;
            if (url.Servers is not null && url.Servers.Any()) return url.Servers.First().Host;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error normalizing connection string: {ConnectionString}", connectionString);
        }

        return connectionString;
    }

    private async Task MonitorAndAdjustReadPreference()
    {
        try
        {
            await foreach (var _ in _monitorChannel.Reader.ReadAllAsync(_cts.Token))
            foreach (var balancer in _balancers.Values)
                await balancer.Update(_cts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Monitoring task was cancelled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during monitoring");
        }
    }

    private readonly struct MonitorCommand;
}