using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using CsvHelper;
using System.Globalization;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;

public static class TimerFunction
{
    private static IConfiguration _configuration;
    [FunctionName("TimerFunction")]
    public static void Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer, ILogger log, ExecutionContext context)
    {
        log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
        InitializeConfiguration(context);

        // Authenticate to Azure Key Vault
        string keyVaultUri = _configuration["KeyVaultUri"];
        string secretName = _configuration["SecretName"];
        string storageAccountName = _configuration["StorageAccountName"];

        string sasKey = GetSasKeyFromKeyVault(keyVaultUri, secretName);

        // Authenticate to Azure Storage Account using SAS Key
        CloudStorageAccount storageAccount = new CloudStorageAccount(
            new StorageCredentials(sasKey),
            storageAccountName,
            endpointSuffix: null,
            useHttps: true);

        // Get blob container reference
        CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
        CloudBlobContainer container = blobClient.GetContainerReference("your-container-name");

        // Create the container if it doesn't exist
        container.CreateIfNotExistsAsync();
        log.LogInformation($"Container '{container.Name}' created.");
        // Generate CSV content
        var csvContent = GenerateCsvContent("My Account","My Group", "Pending");

        // Count records before adding
        log.LogInformation($"Number of records before adding: {csvContent.Count}");

        // Write CSV to Azure Blob
        string blobName = $"useraccountspoc-{DateTime.UtcNow:yyyyMMddHHmmss}.csv";
        CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
        using (var stream = new MemoryStream())
        using (var writer = new StreamWriter(stream))
        using (var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            csvWriter.WriteRecords(csvContent);
            writer.Flush();
            stream.Position = 0;
            blob.UploadFromStreamAsync(stream);
        }

        log.LogInformation($"CSV file '{blobName}' uploaded to Azure Blob Storage.");

        // Read CSV from Azure Blob and update column
        using (var stream = new MemoryStream())
        {
            blob.DownloadToStreamAsync(stream);
            stream.Position = 0;

            using (var reader = new StreamReader(stream))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                var records = csv.GetRecords<MyCsvRecord>().ToList();

                // Update the 'status' column as needed
                foreach (var record in records)
                {
                    record.Status = "UpdatedStatus";
                }

                // Re-upload the updated CSV
                stream.Position = 0;
                blob.UploadFromStreamAsync(stream);

                // Count records after updating
                log.LogInformation($"Number of records after updating: {records.Count}");
            }
        }
    }
    private static string GetSasKeyFromKeyVault(string keyVaultUri, string secretName)
    {
        var client = new SecretClient(new Uri(keyVaultUri), new DefaultAzureCredential());
        var secret = client.GetSecret(secretName).Value;

        return (string)secret.Value;
    }

    private static List<MyCsvRecord> GenerateCsvContent(string AccoutName, string GroupName, String AccountStatus)
    {
        // Generate your CSV content here
        // For simplicity, I'm creating a sample record
        var records = new List<MyCsvRecord>
        {
            new MyCsvRecord
            {
                AccountName = AccoutName,
                UtcDateTime = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss"),
                GroupName = GroupName,
                Status = AccountStatus
            }
        };

        return records;
    }
    private static void InitializeConfiguration(ExecutionContext context)
    {
        var configBuilder = new ConfigurationBuilder()
            .SetBasePath(context.FunctionAppDirectory)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
        .AddEnvironmentVariables();

        _configuration = configBuilder.Build();
    }
}

public class MyCsvRecord
{
    public string AccountName { get; set; }
    public string UtcDateTime { get; set; }
    public string GroupName { get; set; }
    public string Status { get; set; }
}
