using Microsoft.WindowsAzure.Storage.Table;

namespace HallmanacAzureTable.EventStore
{
    public class PartitionedAggregateVersion : TableEntity
    {
        public string DataForVersionedInstance { get; set; }
        public string AggregateRootData { get; set; }
    }
}