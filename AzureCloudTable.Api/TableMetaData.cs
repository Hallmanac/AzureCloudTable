using System;
using System.Collections.Generic;

namespace AzureCloudTable.Api
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