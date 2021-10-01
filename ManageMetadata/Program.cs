using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace ManageStorageTiers
{
    class Program
    {
        static void Main(string[] args)
        {
            DoWorkAsync().GetAwaiter().GetResult();

            Console.WriteLine("Press any key to exit the sample application.");
            Console.ReadLine();
        }

        static async Task DoWorkAsync()
        {
            string connectionString = Environment.GetEnvironmentVariable("STORAGE_CONNECTION_STRING");
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);

            string containerName = Environment.GetEnvironmentVariable("CONTAINER_NAME");
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);

            // Add metadata to containers and blobs
            //await AddContainerMetadataAsync(blobContainerClient);
            //await AddBlobMetadataAsync (blobContainerClient);

            // Display the details of the blobs
            await DisplayBlobMetadata(blobContainerClient);
        }
        private static async Task DisplayBlobMetadata(BlobContainerClient blobContainerClient)
        {

            Console.WriteLine("Container Properties");
            Console.WriteLine("------------------------------------------------------------");
            Response<BlobContainerProperties> response = await blobContainerClient.GetPropertiesAsync();
            BlobContainerProperties containerProperties = response.Value;
            
            Console.WriteLine($"Container name  : {blobContainerClient.Name}");
            Console.WriteLine($"  Last modified : {containerProperties.LastModified}");

            Console.WriteLine("  Container Metadata");
            foreach (var key in containerProperties.Metadata.Keys)
            {
                var value = containerProperties.Metadata[key];
                Console.WriteLine($"    Key: {key}  Value: {value}");
            }

            Console.WriteLine("Blob Properties");
            Console.WriteLine("------------------------------------------------------------");
            AsyncPageable<BlobItem> blobs = blobContainerClient.GetBlobsAsync(BlobTraits.Metadata);
            await foreach (var blobItem in blobs)
            {                
                // Print out some useful blob properties
                Console.WriteLine($"Blob name: {blobItem.Name}" );
                Console.WriteLine($"  Created on   : {blobItem.Properties.CreatedOn}");
                Console.WriteLine($"  Last modified: {blobItem.Properties.LastModified}");

                // Enumerate the metadata
                Console.WriteLine("  Blob Metadata");
                foreach (var key in blobItem.Metadata.Keys)
                {
                    var value = blobItem.Metadata[key];
                    Console.WriteLine($"    Key: {key}  Value: {value}");
                }               
            }
        }

        public static async Task AddContainerMetadataAsync(BlobContainerClient blobContainerClient)
        {
            Response<BlobContainerProperties> response = await blobContainerClient.GetPropertiesAsync();
            IDictionary<string, string> metadata = response.Value.Metadata;

            // Add metadata to the container
            metadata.Add("docType", "safetyReports");

            // Save the updated container metadata
            await blobContainerClient.SetMetadataAsync(metadata);
        }

        public static async Task AddBlobMetadataAsync(BlobContainerClient blobContainerClient)
        {
            AsyncPageable<BlobItem> blobs = blobContainerClient.GetBlobsAsync(BlobTraits.Metadata);
            await foreach (var blobItem in blobs)
            {
                IDictionary<string, string> metadata = blobItem.Metadata;

                // Add a value to the metadata for the blob
                metadata.Add("reportStatus", "included");

                // You need a BlobClient object to update the metadata for a blob
                BlobClient blobClient = blobContainerClient.GetBlobClient(blobItem.Name);
                await blobClient.SetMetadataAsync(metadata);
            }
        }
    }
}
