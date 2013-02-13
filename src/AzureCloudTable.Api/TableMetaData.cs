using System;
using System.Collections.Generic;

namespace AzureCloudTable.Api
{
    public class TableMetaData<TDomainObject> where TDomainObject : class, new()
    {
        public List<PartitionSchema<TDomainObject>> PartitionSchemes { get; set; } 

        public TableMetaData()
        {
            PartitionSchemes = new List<PartitionSchema<TDomainObject>>();
        }
    }
}