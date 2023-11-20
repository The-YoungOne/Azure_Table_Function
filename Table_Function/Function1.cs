using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
//using Microsoft.Azure.Cosmos.Table;


namespace Table_Function
{
    public class Function1
    {
        [FunctionName("Function1")]
        public async Task Run([QueueTrigger("vaccination-queue", Connection = "storage_connection")]string myQueueItem, ILogger log, ExecutionContext context)
        {

            try
            {
                log.LogInformation($"Processing queue item: {myQueueItem}");

                //securely connectes to the local.settings.json
                var config = new ConfigurationBuilder()
                    .SetBasePath(Environment.CurrentDirectory)
                    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();

                // Gets the securely stored connection string and azure storage table name
                var connection = config["Values:storage_connection"];
                var tableName = config["Values:table_name"];

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connection);
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                CloudTable table = tableClient.GetTableReference(tableName);

                //creates table storage if does not exist
                if (!await table.ExistsAsync())
                {
                    await table.CreateAsync();
                }

                // Split the message into parts based on ":"
                var parts = myQueueItem.Split(':');

                // Create an instance of the Values class
                var obj = new Values();
                if (parts[0].Length == 13)
                {
                    obj = new Values
                    {
                        id = parts[0],
                        vaccineCenter = parts[1],
                        date = parts[2],
                        serialNumber = int.Parse(parts[3])
                    };
                }
                else if (parts[0].Length == 6)
                {
                    obj = new Values
                    {
                        id = parts[3],
                        vaccineCenter = parts[2],
                        date = parts[1],
                        serialNumber = int.Parse(parts[0])
                    };
                }
                else
                {
                    log.LogError("Invaild Format for: {myQueueItem}");
                }

                //creates table entity with values for entry
                vaccinationEntity vaccinator = new vaccinationEntity(obj.id)
                {
                    vaccine_center = obj.vaccineCenter,
                    date = DateTime.Parse(obj.date),
                    serial_number = obj.serialNumber
                };

                //inserts into the table storage
                TableOperation insertOp = TableOperation.Insert(vaccinator);
                await table.ExecuteAsync(insertOp);

                log.LogInformation($"Data inserted into the table for ID: {obj.id}");
            }
            catch (Exception ex)
            {
                log.LogError($"Error while processing the queue item!\n\nError: {ex}");
            }
        }

    }

    public class vaccinationEntity : TableEntity
    {
        public vaccinationEntity(string id) 
        {
            this.PartitionKey = "1";
            this.RowKey = id;
        }

        public string vaccine_center { get; set; }
        public DateTime date { get; set; }
        public int serial_number { get; set; }
    }
    public class Values
    {
        public string id { get; set; }
        public string vaccineCenter { get; set; }
        public string date { get; set; }
        public int serialNumber { get; set; }

    }
}
