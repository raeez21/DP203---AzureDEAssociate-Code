using Azure.Messaging.EventHubs.Consumer;
using System.Text;


  string connection_string = "Endpoint=sb://datanamespacedp203.servicebus.windows.net/;SharedAccessKeyName=ProgramPolicy;SharedAccessKey=yLmhZRZyg1OS/JK8xsnDRxzVm/bC6Bvlm+AEhKia7n8=;EntityPath=appeventhub;EntityPath=appeventhub"
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

