using Microsoft.WindowsAzure.Storage.Table;

namespace HallmanacAzureTable.EventStore
{
    public class PartitionedType : TableEntity
    {
        public string AggregateRootData { get; set; }
    }
}