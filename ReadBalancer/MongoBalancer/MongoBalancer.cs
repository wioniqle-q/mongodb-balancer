using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Servers;

namespace MongoBalancer;

internal sealed class MongoBalancer : IAsyncDisposable
{
    private const double LowBalanceFraction = 0.05;
    private const double HighBalanceFraction = 0.30;
    private const double BalanceStep = 0.01;
    private const double LowRatio = 0.9;
    private const double HighRatio = 1.2;
    private readonly CancellationTokenSource _cts = new();

    private readonly ILogger<MongoBalancer> _logger;
    private readonly TimeSpan _maxStaleness;
    private readonly IMongoClient _mongoClient;
    private readonly ConcurrentQueue<double> _roundTripTimes = new();
    private readonly ConcurrentQueue<double> _secondaryStalenessBounds = new();

    private readonly Channel<UpdateCommand> _updateChannel;

    public readonly string ConnectionString;

    private double _currentBalanceFraction = LowBalanceFraction;

    public MongoBalancer(MongoClientSettings settings, TimeSpan maxStaleness, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<MongoBalancer>();
        _mongoClient = new MongoClient(settings);
        _maxStaleness = maxStaleness;

        ConnectionString = settings.Server?.Host ?? settings.Servers.FirstOrDefault()?.Host ?? "unknown";

        _updateChannel = Channel.CreateUnbounded<UpdateCommand>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = true
        });

        Task.Factory.StartNew(ProcessUpdateCommands, _cts.Token, TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        if (_cts is IAsyncDisposable ctsAsyncDisposable)
            await ctsAsyncDisposable.DisposeAsync();
        else
            _cts.Dispose();

        switch (_mongoClient)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync();
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ReadPreference> GetReadPreferenceAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var fraction = (double)(DateTime.UtcNow.Ticks % 10000) / 10000;
        return ValueTask.FromResult(fraction < Volatile.Read(ref _currentBalanceFraction)
            ? ReadPreference.Secondary
            : ReadPreference.Primary);
    }

    public ValueTask Update(CancellationToken cancellationToken)
    {
        return _updateChannel.Writer.WriteAsync(new UpdateCommand(), cancellationToken);
    }

    private async Task ProcessUpdateCommands()
    {
        await foreach (var _ in _updateChannel.Reader.ReadAllAsync(_cts.Token))
        {
            await UpdateSecondaryStalenessBounds(_cts.Token);
            await UpdateRoundTripTimes(_cts.Token);
            AdjustBalanceFraction();
        }
    }

    private async Task UpdateSecondaryStalenessBounds(CancellationToken cancellationToken)
    {
        try
        {
            var cluster = _mongoClient.Cluster;
            var command = new BsonDocument("replSetGetStatus", 1);
            var result = await _mongoClient.GetDatabase("admin")
                .RunCommandAsync<BsonDocument>(command, ReadPreference.Primary, cancellationToken);

            var bounds = ArrayPool<double>.Shared.Rent(cluster.Description.Servers.Count);
            var index = 0;

            foreach (var secondary in cluster.Description.Servers.Where(s => s.Type is ServerType.ReplicaSetSecondary))
            {
                var secondaryMember = result["members"].AsBsonArray
                    .FirstOrDefault(m => m["name"].AsString == secondary.EndPoint.ToString());

                if (secondaryMember is null)
                {
                    bounds[index++] = _maxStaleness.TotalMilliseconds;
                    continue;
                }

                var secondaryLastAppliedOpTime = ConvertToDateTime(secondaryMember["optimeDate"]);
                var lastCommittedOpTime = result["optimes"]["lastCommittedOpTime"];
                var primaryLastAppliedOpTime = ConvertToDateTime(lastCommittedOpTime);
                bounds[index++] = (primaryLastAppliedOpTime - secondaryLastAppliedOpTime).TotalMilliseconds;
            }

            _secondaryStalenessBounds.Clear();
            for (var i = 0; i < index; i++) _secondaryStalenessBounds.Enqueue(bounds[i]);

            ArrayPool<double>.Shared.Return(bounds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating secondary staleness bounds");
        }
    }

    private Task UpdateRoundTripTimes(CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            var cluster = _mongoClient.Cluster;
            var times = cluster.Description.Servers
                .OrderBy(s => s.Type is ServerType.ReplicaSetPrimary ? 0 : 1)
                .Select(s => s.AverageRoundTripTime.TotalMilliseconds)
                .ToArray();

            _roundTripTimes.Clear();
            foreach (var time in times) _roundTripTimes.Enqueue(time);

            if (_roundTripTimes.IsEmpty) _logger.LogWarning("No servers found when updating round trip times");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating round trip times");
        }

        return Task.CompletedTask;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AdjustBalanceFraction()
    {
        try
        {
            if (_roundTripTimes.TryPeek(out var primaryLatency) is not true) return;

            var secondaryLatency = _roundTripTimes.Skip(1).DefaultIfEmpty(0).Average() +
                                   _secondaryStalenessBounds.Average();

            if (secondaryLatency <= 0) return;

            var latencyRatio = primaryLatency / secondaryLatency;
            var newFraction = latencyRatio switch
            {
                > HighRatio => Math.Min(Volatile.Read(ref _currentBalanceFraction) + BalanceStep, HighBalanceFraction),
                < LowRatio => Math.Max(Volatile.Read(ref _currentBalanceFraction) - BalanceStep, LowBalanceFraction),
                _ => Volatile.Read(ref _currentBalanceFraction)
            };

            Volatile.Write(ref _currentBalanceFraction, newFraction);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error adjusting balance fraction");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTime ConvertToDateTime(BsonValue value)
    {
        return value switch
        {
            BsonDateTime dateTime => dateTime.ToUniversalTime(),
            BsonTimestamp timestamp => DateTimeOffset.FromUnixTimeSeconds(timestamp.Timestamp).UtcDateTime,
            BsonDocument doc when doc.Contains("ts") => DateTimeOffset
                .FromUnixTimeSeconds(doc["ts"].AsBsonTimestamp.Timestamp).UtcDateTime,
            _ => throw new InvalidOperationException($"Unexpected datetime format: {value}")
        };
    }

    private readonly struct UpdateCommand;
}