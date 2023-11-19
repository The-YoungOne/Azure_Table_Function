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

            //attempts to first connect to the queue storage
            try
            {
                //proceeds to read every message from the queue and break it up into an array of messages
                Values obj = new Values();
                Console.WriteLine(myQueueItem);

                string[] split = myQueueItem.Split(':');
                Console.WriteLine(split[0].Length);
                if (split[0].Length == 13)
                {
                    obj.id = split[0];
                    obj.vaccineCenter = split[1];
                    obj.date = split[2];
                    obj.serialNumber = int.Parse(split[3]);
                }
                else if (split[0].Length == 6)
                {
                    obj.serialNumber = int.Parse(split[0]);
                    obj.date = split[1];
                    obj.vaccineCenter = split[2];
                    obj.id = split[3];
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    log.LogError($"Invaild Format: {myQueueItem}");
                }

                Console.ForegroundColor = ConsoleColor.Green;
                log.LogInformation("Message(s) successfully read from the queue storage.");

                Console.ForegroundColor = ConsoleColor.Yellow;
                log.LogInformation("\nGetting table name from secure location...");
                Console.ForegroundColor = ConsoleColor.White;


                //create a secure configuration connection to the local settings variables
                var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

                //gets the securely stored connection string
                var connection = config.GetConnectionString("storage_connection");

                //gets the securely stored azure table name
                var tableName = config.GetConnectionString("table_name");

                if (String.IsNullOrEmpty(tableName))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    log.LogError("The table name failed to be retreived!");
                    Console.ForegroundColor = ConsoleColor.White;
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    log.LogInformation("Table name securely retrieved.");

                    Console.ForegroundColor = ConsoleColor.Yellow;
                    log.LogInformation("\nInterting data into the table...");
                    Console.ForegroundColor = ConsoleColor.White;

                    //links to the azure table account
                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connection);

                    //creates a client with reference to the table name to start inserting into
                    CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                    CloudTable table = tableClient.GetTableReference(tableName);
                    
                    //creates the table in an instance where it might not exist
                    if(!await table.ExistsAsync())
                    {
                        await table.CreateAsync();
                    }

                    vaccinationEntity vaccinator = new vaccinationEntity(obj.id);
                    TableOperation insertOp = TableOperation.Insert(vaccinator);

                    _ = table.ExecuteAsync(insertOp);
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                log.LogError($"Error while executing the program!\n\nError: {ex}");
                Console.ForegroundColor = ConsoleColor.White;
            }

            Console.ReadLine(); 
        }

    }

    public class vaccinationEntity : TableEntity
    {
        public vaccinationEntity(string id) 
        {
            this.PartitionKey = "1";
            this.RowKey = obj.id;
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
