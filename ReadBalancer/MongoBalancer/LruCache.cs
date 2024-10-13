using System.Collections.Concurrent;

namespace MongoBalancer;

internal sealed class LruCache<TKey, TValue>(int capacity, TimeSpan slidingExpiration) : IAsyncDisposable where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, CacheItem> _cache = new();
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public async ValueTask DisposeAsync()
    {
        foreach (var item in _cache.Values) await DisposeIfNeeded(item.Value);

        _cache.Clear();
        _semaphore.Dispose();
    }

    public async Task<TValue?> GetOrAdd(TKey key, Func<TKey, Task<TValue?>> valueFactory)
    {
        if (_cache.TryGetValue(key, out var item))
        {
            if (item.IsExpired)
            {
                await Remove(key);
            }
            else
            {
                item.LastAccessed = DateTime.UtcNow;
                return item.Value;
            }
        }

        await _semaphore.WaitAsync();
        try
        {
            if (_cache.TryGetValue(key, out item))
            {
                if (item.IsExpired)
                {
                    await Remove(key);
                }
                else
                {
                    item.LastAccessed = DateTime.UtcNow;
                    return item.Value;
                }
            }

            if (_cache.Count >= capacity) await RemoveLeastRecentlyUsed();

            var value = await valueFactory(key);
            var newItem = new CacheItem(value, slidingExpiration);
            _cache[key] = newItem;
            return value;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task Remove(TKey key)
    {
        if (_cache.TryRemove(key, out var item)) await DisposeIfNeeded(item.Value);
    }

    private async Task RemoveLeastRecentlyUsed()
    {
        var oldest = _cache.OrderBy(kvp => kvp.Value.LastAccessed).FirstOrDefault();
        if (oldest.Key != null) await Remove(oldest.Key);
    }

    private static async Task DisposeIfNeeded(TValue? value)
    {
        switch (value)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync();
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }

    private class CacheItem
    {
        public CacheItem(TValue? value, TimeSpan slidingExpiration)
        {
            Value = value;
            LastAccessed = DateTime.UtcNow;
            Expiration = LastAccessed.Add(slidingExpiration);
        }

        public TValue? Value { get; }
        public DateTime LastAccessed { get; set; }
        private DateTime Expiration { get; }

        public bool IsExpired => DateTime.UtcNow > Expiration;
    }
}