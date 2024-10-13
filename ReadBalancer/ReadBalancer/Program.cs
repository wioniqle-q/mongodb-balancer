using System.Collections.Concurrent;
using System.Diagnostics;
using MongoBalancer;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ReadBalancer;

internal static class Program
{
    private const string ConnectionString = "";
    private const string DatabaseName = "";
    private const string CollectionName = "";
    
    private const int WarmUpIterations = 100;
    private const int BenchmarkIterations = 10000;
    private const int MaxConcurrency = 100;

    private static readonly TimeSpan MaxStaleness = TimeSpan.FromSeconds(30);

    private static async Task Main()
    {
        var mongoUrl = new MongoUrl(ConnectionString);
        var settings = MongoClientSettings.FromUrl(mongoUrl);
        ConfigureMongoSettings(settings);

        var client = new MongoClient(settings);
        var database = client.GetDatabase(DatabaseName);
        var collection = database.GetCollection<BsonDocument>(CollectionName);

        await using var readBalancer =
            new MongoBalancer.ReadBalancer([mongoUrl], MaxStaleness, new NullLoggerFactory());

        await EnsureTestData(collection);
        await WarmUp(readBalancer, collection);

        await RunBenchmark(readBalancer, collection, "Parallel Load Test", BenchmarkIterations);

        Console.WriteLine("Benchmarks completed. Press any key to exit.");
        Console.ReadKey();
    }

    private static void ConfigureMongoSettings(MongoClientSettings settings)
    {
        settings.MaxConnectionPoolSize = 1000;
        settings.WaitQueueTimeout = TimeSpan.FromSeconds(60);
        settings.SocketTimeout = TimeSpan.FromSeconds(60);
        settings.ServerSelectionTimeout = TimeSpan.FromSeconds(60);
    }

    private static async Task EnsureTestData(IMongoCollection<BsonDocument> collection)
    {
        if (await collection.CountDocumentsAsync(new BsonDocument()) == 0)
        {
            var documents = Enumerable.Range(1, 1000).Select(i => new BsonDocument("_id", i));
            await collection.InsertManyAsync(documents);
            Console.WriteLine("Test data inserted.");
        }
    }

    private static async Task WarmUp(IReadBalancer readBalancer, IMongoCollection<BsonDocument> collection)
    {
        Console.WriteLine("Warming up...");
        await Parallel.ForEachAsync(Enumerable.Range(0, WarmUpIterations),
            new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrency },
            async (_, _) => await PerformQuery(readBalancer, collection));
        Console.WriteLine("Warm-up complete.");
    }

    private static async Task RunBenchmark(IReadBalancer readBalancer, IMongoCollection<BsonDocument> collection,
        string scenarioName, int iterations)
    {
        Console.WriteLine($"\nRunning benchmark: {scenarioName} ({iterations} iterations)");

        var stopwatch = Stopwatch.StartNew();
        var results = new ConcurrentBag<(ReadPreference, BsonDocument)>();

        await Parallel.ForEachAsync(Enumerable.Range(0, iterations),
            new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrency },
            async (_, _) =>
            {
                var result = await Retry(() => PerformQuery(readBalancer, collection));
                results.Add(result);
            });

        stopwatch.Stop();

        PrintBenchmarkResults(results, iterations, stopwatch.ElapsedMilliseconds);
    }

    private static async Task<(ReadPreference, BsonDocument)> PerformQuery(IReadBalancer readBalancer,
        IMongoCollection<BsonDocument> collection)
    {
        try
        {
            var readPreference = await readBalancer.GetReadPreference(ConnectionString);
            var result = await collection.WithReadPreference(readPreference)
                .Find(new BsonDocument())
                .FirstOrDefaultAsync();
            return (readPreference, result);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Error: {e.Message}");
            return (ReadPreference.Primary, null)!;
        }
    }

    private static async Task<T> Retry<T>(Func<Task<T>> operation, int maxRetries = 3)
    {
        for (var i = 0; i < maxRetries; i++)
            try
            {
                return await operation();
            }
            catch (MongoWaitQueueFullException)
            {
                if (i == maxRetries - 1) throw;
                await Task.Delay(TimeSpan.FromSeconds(1 * (i + 1)));
            }

        throw new Exception("Max retries exceeded");
    }

    private static void PrintBenchmarkResults(ConcurrentBag<(ReadPreference, BsonDocument)> results, int iterations,
        long elapsedMilliseconds)
    {
        var primaryCount = results.Count(r => r.Item1.Equals(ReadPreference.Primary));
        var secondaryCount = results.Count(r => r.Item1.Equals(ReadPreference.Secondary));

        Console.WriteLine($"Total time: {elapsedMilliseconds} ms");
        Console.WriteLine($"Average time per request: {(double)elapsedMilliseconds / iterations:F3} ms");
        Console.WriteLine($"Requests per second: {iterations * 1000.0 / elapsedMilliseconds:F2}");
        Console.WriteLine($"Primary queries: {primaryCount}, Secondary queries: {secondaryCount}");
        Console.WriteLine($"Primary percentage: {(double)primaryCount / iterations:P2}");
        Console.WriteLine($"Secondary percentage: {(double)secondaryCount / iterations:P2}");
    }
}