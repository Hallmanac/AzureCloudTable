using Microsoft.WindowsAzure.Storage.Table;

namespace HallmanacAzureTable.EventStore
{
    public class PartitionedProperty : TableEntity
    {
        public string JsvSerializedPropertyValue { get; set; }
        public string AggregateRootData { get; set; }
    }
}