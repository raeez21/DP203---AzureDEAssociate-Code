using Azure.Messaging.EventHubs.Producer;
using AzureEventHub_Send;
using System.Text;

string connection_string = "Endpoint=sb://datanamespacedp203.servicebus.windows.net/;SharedAccessKeyName=ProgramPolicy;SharedAccessKey=yLmhZRZyg1OS/JK8xsnDRxzVm/bC6Bvlm+AEhKia7n8=;EntityPath=appeventhub";
                           
EventHubProducerClient _client = new EventHubProducerClient(connection_string);

    EventDataBatch _batch = _client.CreateBatchAsync().GetAwaiter().GetResult();

    List<Order> _orders = new List<Order>()
            {
                new Order() {OrderID="O6",Quantity=10,UnitPrice=9.99m,DiscountCategory="Tier 1R" },
                new Order() {OrderID="O7",Quantity=15,UnitPrice=10.99m,DiscountCategory="Tier 2R" },
                new Order() {OrderID="O8",Quantity=20,UnitPrice=11.99m,DiscountCategory="Tier 3R" },
                new Order() {OrderID="O9",Quantity=25,UnitPrice=12.99m,DiscountCategory="Tier 1R" },
                new Order() {OrderID="10",Quantity=30,UnitPrice=13.99m,DiscountCategory="Tier 2R" }
            };

    foreach (Order _order in _orders)
        _batch.TryAdd(new Azure.Messaging.EventHubs.EventData(Encoding.UTF8.GetBytes(_order.ToString())));

    _client.SendAsync(_batch).GetAwaiter().GetResult();
    Console.WriteLine("Batch of events sent");

