# Azure CosmosDB - QueryPatterns
Get your hands dirty with 6 types of Query Patterns in Azure Cosmos DB Core SQL API using .NET SDK.

        // 1) full-table scan
        // 2) point-read / point-lookup
        // 3) in-partition query (simple)
        // 4) in-partition query (multiple)
        // 5) cross-partition query (aka fan-out query)
        // 6) parallel cross-partition query

# Steps to do:
- First, create an Azure Cosmos DB account with Core SQL API.
- In the Container Id field, enter the value FoodCollection. 
- In the Partition key field, enter the value /foodGroup.
- In the Throughput field, enter the value 10000.
- Download the NutritionData.zip file, unzip it, and then use the NutritionData.json file and upload it via Portal (you may also use Azure Data Factory to upload it into Azure Cosmos DB). Follow steps as outlined [here](https://azurecosmosdb.github.io/labs/dotnet/labs/02-load_data_with_adf.html).
- Open the program.cs file in Visual Studio and edit the Environment Variables (account name, PRIMARY KEY etc.).
- Build and execute to check the types of Queries and RU charges for each operation.
