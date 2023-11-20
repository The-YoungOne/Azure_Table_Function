using Azure.Storage.Queues;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using System.Text;

Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine("Welcome to the Queue Storage Function App");

//connection string to storage queue
string connection = "DefaultEndpointsProtocol=https;AccountName=saluciany;AccountKey=mWm/9/G4F04OQX/G/XJWTqbx3o92aYlE5uxA1i3AV/s6GYdEpj36S3wDNgCvjQ2kwzcOKRofxPhB+AStH6CW5w==;EndpointSuffix=core.windows.net";

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine("\nCreating queue...");

//gets or creates the queue
string queueName = "vaccination-queue";
QueueClient client = new QueueClient(connection, queueName);
await client.CreateIfNotExistsAsync();

Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine("\nQueue Created.");


//inserting into queue

//random number generator
Random generator = new Random();

//diff center names
string[] vaccinationCenters = { "La Lucia Clinic", "Kingsburg Clinic", "Adams Clinic", "Bluff Clinic", "Cape Canal Clinic" };

//random date
int range = 5 * 365; //5 years          

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine($"\nAdding Messages to {queueName}");

try
{
    //sends message to queue
    for (int i = 0; i < 5; i++)
    {
        String id = generator.Next(0, 1000000).ToString("D6") + "" + generator.Next(0, 1000000).ToString("D7");
        DateTime date = DateTime.Today.AddDays(-generator.Next(range));
        string serialNumber = "" + generator.Next(100000, 1000000);

        string data;
        if (i % 2 == 0)
        {
            data = $"{id}:{vaccinationCenters[i]}:{date.ToShortDateString()}:{serialNumber}";
        }
        else
        {
            data = $"{serialNumber}:{date.ToShortDateString()}:{vaccinationCenters[i]}:{id}";
        }

        byte[] dataBytes = Encoding.UTF8.GetBytes(data);
        string base64Data = Convert.ToBase64String(dataBytes);

        await client.SendMessageAsync(base64Data);
    }

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"\nMessages added to {queueName}");

    Console.ForegroundColor = ConsoleColor.DarkGreen;
    Console.WriteLine("\nApplication Execution Complete.");
}
catch (Exception ex)
{
    Console.WriteLine($"Error trying to insert messages into queue: {ex}");
}
