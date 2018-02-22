// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SampleEphReceiver
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Configuration;

    public class Program
    {
        private static string EventHubConnectionString;
        private static string EventHubName;
        private static string EventHubConsumerGroupName;
        private static string StorageContainerName ;
        private static string StorageAccountName;
        private static string StorageAccountKey;

        private static string StorageConnectionString;

        public static void Main(string[] args)
        {

            var builder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();
            EventHubConnectionString = configuration["EventHub:ConnectionString"];
            EventHubName = configuration["EventHub:Name"];
            EventHubConsumerGroupName = configuration["EventHub:ConsumerGroup"];
            StorageContainerName = configuration["StorageAccount:StorageContainerName"];
            StorageAccountName = configuration["StorageAccount:StorageAccountName"];
            StorageAccountKey = configuration["StorageAccount:StorageAccountKey"];
            StorageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", StorageAccountName, StorageAccountKey);

            MainAsync().GetAwaiter().GetResult();
        }

        private static async Task MainAsync()
        {
            Console.WriteLine("Registering EventProcessor...");

            var eventProcessorHost = new EventProcessorHost(
                EventHubName,
                EventHubConsumerGroupName,
                EventHubConnectionString,
                StorageConnectionString,
                StorageContainerName);

            // Registers the Event Processor Host and starts receiving messages
            await eventProcessorHost.RegisterEventProcessorAsync<SimpleEventProcessor>();

            Console.WriteLine("Receiving. Press enter key to stop worker.");
            Console.ReadLine();

            // Disposes of the Event Processor Host
            await eventProcessorHost.UnregisterEventProcessorAsync();
        }
    }
}
