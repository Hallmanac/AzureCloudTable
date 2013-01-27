using System.Collections.Generic;

namespace HallmanacAzureTable.EventStore
{
    internal class TableMetaData<TDomainObject> where TDomainObject : class, new()
    {
        public List<string> PartitionSchemes { get; set; }

        public TableMetaData()
        {
            PartitionSchemes = new List<string>();
        }
    }
}