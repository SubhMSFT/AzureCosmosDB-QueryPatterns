using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

public class Program
{
    private static readonly string _endpointUri = "https://netsdkcoresql2021.documents.azure.com:443/";
    private static readonly string _primaryKey = "R69RuwXTv7j2y7ZPf6UgHqnDDwjdxges01I6NTxtnDyh1ZmlYqCqFWHVTN7Grksu5RQUDE3Wttf3qk7XwRHyyg==";
    private static readonly string _databaseId = "NutritionDatabase";
    private static readonly string _containerId = "FoodCollection";

    /* Azure Cosmos DB supports 2 types of Network Connectivity Modes: Gateway & Direct.
     * A 3rd one is in *Preview*: Dedicated-Gateway with Integrated Cache (but we shall not discuss it till its Production Ready and in GA).
     * Supporting documentation > https://docs.microsoft.com/en-us/azure/cosmos-db/sql/sql-sdk-connection-modes
    * 
    * 1. Gateway means you're telling Cosmos DB SDK to:
    *    a) use HTTPS port & a single DNS endpoint.
    *    b) this mode involves an additional network hop every time data is read from / written to Azure Cosmos DB. Why?
    *          Explanation of how requests are routed: Every request(s) made by your client are routed to a so-called "Gateway" server in the Azure Cosmos DB front end, 
    *                                             which in turn fans out your requests to the appropriate partition(s) in the Azure Cosmos DB back-end.
    *    c) only to be recommended in use-cases wherein socket connections are limited.
    *    d) limitation: does *not* use concept of Request-Multiplexing, hence poorer in performance than Direct Connectivity mode.
    *    e) advatange: available in all SDKs by default.
    *
    * 2. Direct means you're telling Cosmos DB SDK to:
    *   a) use TCP protocol and lesser hops. Why?
    *          Explanation of how requests are routed: This uses the concept of Request-Multiplexing wherein SDK interacts with physical partition end-points concurrently & NOT sequentially.
    *   c) so any operation wherein you're doing highly optimized read/writes will be faster by multiple times of X than Gateway mode.
    *   d) only limit: (as of today) only available in NET SDK and Java SDK (not in Python, Node etc.).
    *   
    *   IMPORTANT: Catching a '503' Service Unavailable Error (eg. customer moves from Gateway to Direct, and starts getting Error 503.). WHY?
    *   Answer: When you use direct mode, in addition to the gateway ports, you need to ensure the port range between 10000 and 20000 is open because Azure Cosmos DB uses dynamic TCP ports. 
    *           When using direct mode on private endpoints, the full range of TCP ports - from 0 to 65535 should be open. If these ports aren't open and you try to use the TCP protocol, 
    *           you might receive a 503 Service Unavailable error.
    *   Supporting documentation > https://docs.microsoft.com/en-us/azure/cosmos-db/sql/sql-sdk-connection-modes#service-port-ranges
    */

    private static CosmosClient _client = new CosmosClient(_endpointUri, _primaryKey, new CosmosClientOptions() { ConnectionMode = ConnectionMode.Gateway });

    public static async Task Main(string[] args)
    {
        Database database = _client.GetDatabase(_databaseId);
        Container container = database.GetContainer(_containerId);

        // Identifying 6 types of Query Patterns in Cosmos DB:

        // 1) full-table scan
        // 2) point-read / point-lookup
        // 3) in-partition query (simple)
        // 4) in-partition query (multiple)
        // 5) cross-partition query (aka fan-out query)
        // 6) parallel cross-partition query

        /* RUs in Cosmos DB (CoreSQL API):
         * 1 read of a 1 KB document        = 1.00 RU
         * 1 create of a 1 KB document      = 5.71 RU
         * 1 update of 1 KB document        = 10.67 RU
         * 1 delete for 1 KB document       = 5.71 RU
        */

        // 1) full-table scan (hopeless query, inefficient, most expensive)
        string sql = "SELECT * FROM c";
        FeedIterator<Food> query = container.GetItemQueryIterator<Food>(sql);
        FeedResponse<Food> queryResponse = await query.ReadNextAsync();
        await Console.Out.WriteLineAsync($"Query is: {sql}");
        await Console.Out.WriteLineAsync($"{queryResponse.RequestCharge} RUs");
        Console.Out.WriteLine();
        // Observations:
        // running SELECT * FROM c in ConnectionMode = ConnectionMode.Direct throws Error 503.
        // runs perfectly when mode is changed from Direct - Gateway.
        // Error 503 reason: TCP dynamic port exhaustion (SINCE ports are not cleared for usage at app level, and are blocked).
        // 2 observations:
        // 1. SAME CODE runs perfectly without change on 'Gateway' mode cause of Hop Server in between.
        // 2. SAME CODE when full-table scan commented out, also runs to perfection without Errors.
        //                -- Which also means, your code query patterns also play a vital role (which determines the "Payload").
        // So 2 things to take into consideration always: ConnectivityMode and QueryPatterns (for Payload).

        // 2) point-read / point-lookup (best query, highly recommended by Cosmos Engineering team, the primary reason why NoSQL databases were engineered).
        //    why? A point read is a key/value lookup on a single item ID + its corresponding partition key.
        //    mandatory post here to read with RU numbers > https://devblogs.microsoft.com/cosmosdb/point-reads-versus-queries/

        // Let us see some solid examples:
        // 2a) read a Single Document in Azure Cosmos DB using ReadItemAsync from /foodGroup = Sweets
        // Non-technical reason: Note the 'ReadItemAsync' - must use for fast & cheap lookups.
        // Technical reason: 'ReadItemAsync' is an object deserializer whichs reads + deserializes JSON into a strong-typed C# object.
        ItemResponse<Food> candyResponse = await container.ReadItemAsync<Food>("19293", new PartitionKey("Sweets"));
        Food candy = candyResponse.Resource;
        Console.Out.WriteLine($"Read {candy.Description}");
        Console.Out.WriteLine("This is a point-read: looking for item: 19293 with partitionKey = Sweets");
        await Console.Out.WriteLineAsync($"{candyResponse.RequestCharge} RU/s");
        Console.Out.WriteLine();

        // 2b) 2nd example: 
        // read a Single Document in Azure Cosmos DB using ReadItemAsync from /foodGroup = Beef Products
        ItemResponse<Food> beefResponse = await container.ReadItemAsync<Food>("08065", new PartitionKey("Breakfast Cereals"));
        Food beef = beefResponse.Resource;
        Console.Out.WriteLine($"Read {beef.Description}");
        Console.Out.WriteLine("This is a point-read: looking for item: 08065 with partitionKey = Breakfast Cereals");
        await Console.Out.WriteLineAsync($"{beefResponse.RequestCharge} RU/s");
        Console.Out.WriteLine();

        /* 1RU = 1 read of 1 Kb.
            * 4 conditions are met: 1) Direct C, 2) point-read, 3) data does not have skew, 4) Default indexing policy = OFF.
        */

        // 3) In-partition Query: execute a query against & will be restricted within a Single Azure Cosmos DB Partition.
        // How does this work? Query has to have partitionKey filter specified. CosmosDB SDK does 2 things:
        //                      a) automatically optimizes the query.
        //                      b) routes the query to the physical partitions corresponding to the logical partitions vis-a-vis partitionKey value specified in query.
        // mandatory read > https://docs.microsoft.com/en-us/azure/cosmos-db/sql/how-to-query-container#in-partition-query

        // In-partition Query (simple):
        string sqlA = "SELECT * FROM c WHERE c.foodGroup = 'Fats and Oils'";                                        // please note: /foodGroup is partitionKey for collection.
        FeedIterator<Food> query2 = container.GetItemQueryIterator<Food>(sqlA);
        FeedResponse<Food> queryResponse2 = await query.ReadNextAsync();
        await Console.Out.WriteLineAsync($"Query is: {sqlA}");
        await Console.Out.WriteLineAsync($"{queryResponse2.RequestCharge} RUs");
        Console.Out.WriteLine();

        // 4) In-partition Query (multiple): the same logic & routing applies, but here same filter applies across 3 fields
        string sqlB = "SELECT c.description, c.manufacturerName, c.servings FROM c WHERE c.foodGroup = 'Sweets'";   // please note: /foodGroup is partitionKey for collection.
        FeedIterator<Food> query3 = container.GetItemQueryIterator<Food>(sqlB);
        FeedResponse<Food> queryResponse3 = await query.ReadNextAsync();
        await Console.Out.WriteLineAsync($"Query is: {sqlB}");
        await Console.Out.WriteLineAsync($"{queryResponse3.RequestCharge} RUs");
        Console.Out.WriteLine();

        // this query will select all food where the foodGroup is set to the value Sweets. It will also only select documents that have description, manufacturerName,
        // and servings properties defined. You'll note that the syntax is very familiar if you've done work with SQL before.
        // Also note that because this query has the partition key in the WHERE clause, this query can execute within a single partition.
        string sqlC = "SELECT c.description, c.manufacturerName, c.servings FROM c WHERE c.foodGroup = 'Sweets' and IS_DEFINED(c.description) and IS_DEFINED(c.manufacturerName) and IS_DEFINED(c.servings)";
        FeedIterator<Food> query4 = container.GetItemQueryIterator<Food>(sqlC);
        FeedResponse<Food> queryResponse4 = await query.ReadNextAsync();
        await Console.Out.WriteLineAsync($"Query is: {sqlC}");
        await Console.Out.WriteLineAsync($"{queryResponse4.RequestCharge} RUs");
        Console.Out.WriteLine();

        // this query has AND and 2 filters but still does not matter. This is still treated by SDK as 'In-partition Query' since /foodGroup = partitionKey is 1 filter.
        string sqlD = "SELECT * FROM c WHERE c.foodGroup = 'Fats and Oils' AND c.isFromSurvey = 'false'";
        FeedIterator<Food> query5 = container.GetItemQueryIterator<Food>(sqlD);
        FeedResponse<Food> queryResponse5 = await query.ReadNextAsync();
        await Console.Out.WriteLineAsync($"Query is: {sqlD}");
        await Console.Out.WriteLineAsync($"{queryResponse5.RequestCharge} RUs");
        Console.Out.WriteLine();

        // 5) Cross-Partition Query (query fan-out): 
        /* Below example shows a bad query pattern since SDK does not know which logical partition to look to, so scans every physical partition
        and runs the query repeatedly against every partition's INDEX to check yes OR no. High RU and poor performance.
        mandatory read > https://docs.microsoft.com/en-us/azure/cosmos-db/sql/how-to-query-container#cross-partition-query
        sample examples with best practices from Microsoft > https://docs.microsoft.com/en-us/azure/cosmos-db/sql/how-to-query-container#useful-example
        */
        string fanout = "SELECT * FROM c WHERE c.version = 1";
        FeedIterator<Food> query6 = container.GetItemQueryIterator<Food>(fanout);
        FeedResponse<Food> queryResponse6 = await query.ReadNextAsync();
        await Console.Out.WriteLineAsync($"Query is: {fanout}");
        await Console.Out.WriteLineAsync($"{queryResponse5.RequestCharge} RUs");
        Console.WriteLine("Execution paused for verification. Press any key to continue to delete.");
        Console.ReadKey();

        // 6) Parallel Cross-Partition Query: for efficient parallel execution, needs to set parameters in SDK.
        /*  - MaxConcurrency: Sets the maximum number of simultaneous network connections to the container's partitions. Recommended to set to -1 for SDK to manage Parallelism. If the MaxConcurrency set to 0, there is a single network connection to the container's partitions.
            - MaxBufferedItemCount: If this option is omitted or to set to - 1, the SDK manages the number of items buffered during parallel query execution. 
            - MaxItemCount: Sets the maximum number of items to be returned in the enumeration (pagination) operation.
        */

        // In below example, we are running a query, and then when the output comes, we're telling SDK to limit the MaxItemCount to 100 items only.
        // This will result in paging if there are more than 100 items that match the query.

        string sqlpagination = "SELECT c.id, c.description, c.manufacturerName, c.servings FROM c WHERE c.manufacturerName != null";
        FeedIterator<Food> queryB = container.GetItemQueryIterator<Food>(sqlpagination, requestOptions: new QueryRequestOptions { MaxConcurrency = -1, MaxItemCount = 100 });
        int pageCount = 0;
        while (queryB.HasMoreResults)
        {
            Console.Out.WriteLine($"---Page #{++pageCount:0000}---");
            foreach (var c in await queryB.ReadNextAsync())
            {
                Console.Out.WriteLine($"\t[{c.Id}]\t{c.Description,-20}\t{c.ManufacturerName,-40}");
            }
        }
    }
}