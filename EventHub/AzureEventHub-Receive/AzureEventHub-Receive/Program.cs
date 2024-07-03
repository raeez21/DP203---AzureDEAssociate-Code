using Azure.Messaging.EventHubs.Consumer;
using System.Text;


  string connection_string = "Enter-connection-string-here;EntityPath=appeventhub"
  string consumer_group = "$Default";

EventHubConsumerClient _client = new EventHubConsumerClient(consumer_group, connection_string);

    await foreach (PartitionEvent _event in _client.ReadEventsAsync())
    {
        Console.WriteLine($"Partition ID {_event.Partition.PartitionId}");
        Console.WriteLine($"Data Offset {_event.Data.Offset}");
        Console.WriteLine($"Sequence Number {_event.Data.SequenceNumber}");
        Console.WriteLine($"Partition Key {_event.Data.PartitionKey}");
        Console.WriteLine(Encoding.UTF8.GetString(_event.Data.EventBody));
    }
    Console.ReadKey();

