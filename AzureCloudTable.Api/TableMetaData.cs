using System;
using System.Collections.Generic;

namespace HallmanacAzureTable.EventStore
{
    public class TableMetaData<TDomainObject> where TDomainObject : class, new()
    {
        public Dictionary<string, Func<TDomainObject, bool>> PartitionSchemes { get; set; } 

        public TableMetaData()
        {
            PartitionSchemes = new Dictionary<string, Func<TDomainObject, bool>>();
        }
    }
}