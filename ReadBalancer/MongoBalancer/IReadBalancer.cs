using MongoDB.Driver;

namespace MongoBalancer;

public interface IReadBalancer : IAsyncDisposable
{
    Task<ReadPreference> GetReadPreference(string connectionString, CancellationToken cancellationToken = default);
}