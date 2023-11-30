using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.IO;
using System.Linq;
using Microsoft.Azure.Cosmos;

namespace GatewayLib
{
    /// <summary>
    /// UDL responsible for creating records in CosmosDB for the metadata associated with
    /// a (k, v) pair put into Cascade's store. Assumes that you have environment variables
    /// COSMOS_ENDPOINT set to the URI of the CosmosDB instance, and COSMOS_KEY as the primary
    /// key of the CosmosDB instance.
    /// Note that building this does not work yet.
    /// </summary>

    // C# record representing an item in the container
    public record CascadeRecord(
        string id,
        UInt32 sender,
        UInt32 worker_id,
        long timestamp
    );

    public class CosmosDBCreateUDL
    {
        public static async void OcdpoHandler(
            UInt32 sender,
            IntPtr objectPoolPathname,
            IntPtr keyString,
            IntPtr objectKey,
            IntPtr objectBytes,
            UInt64 objectBytesSize,
            UInt32 workerId,
            IntPtr emitFunc,
            IntPtr emitInvokePtr)
        {   
            Console.WriteLine("aaa");
            using CosmosClient client = new(
                accountEndpoint: Environment.GetEnvironmentVariable("COSMOS_ENDPOINT")!,
                authKeyOrResourceToken: Environment.GetEnvironmentVariable("COSMOS_KEY")!
            );

            Console.WriteLine("bbb");
            // Database reference with creation if it does not already exist
            Database database = await client.CreateDatabaseIfNotExistsAsync(
                id: "cascade-cosmos"
            );

            Console.WriteLine($"[CosmosDBCreateUDL.cs]: New database:\t{database.Id}");

            // Container reference with creation if it does not already exist
            Container container = await database.CreateContainerIfNotExistsAsync(
                id: "records",
                partitionKeyPath: "/senderId",
                throughput: 400
            );

            Console.WriteLine($"[CosmosDBCreateUDL.cs]: New container:\t{container.Id}");

            string idForRecord = Marshal.PtrToStringAnsi(objectBytes, (Int32) objectBytesSize);
            DateTime currDateTime = DateTime.Now;
            CascadeRecord newRecord = new(
                id: idForRecord,
                sender: sender,
                worker_id: workerId,
                timestamp: ((DateTimeOffset) currDateTime).ToUnixTimeSeconds()
            );

            Console.WriteLine("ccc");

            CascadeRecord createdRecord = await container.CreateItemAsync<CascadeRecord>(
                item: newRecord,
                partitionKey: new PartitionKey(sender)
            );

            Console.WriteLine($"[CosmosDBCreateUDL.cs]: Created item:\t{createdRecord.id}]");
        }
    }
}
 