using System;
using System.Collections.Generic;

namespace AzureCloudTable.EventStore
{
    internal class TableMetaData<TDomainObject> where TDomainObject : class, new()
    {
        public Dictionary<string, Func<TDomainObject, bool>> PartitionSchemes { get; set; } 

        public TableMetaData()
        {
            PartitionSchemes = new Dictionary<string, Func<TDomainObject, bool>>();
        }
    }
}