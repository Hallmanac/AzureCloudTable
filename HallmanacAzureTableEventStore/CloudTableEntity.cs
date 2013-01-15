using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace HallmanacAzureTable.EventStore
{
    public class CloudTableEntity<TDomainObject> : ITableEntity where TDomainObject : class, new()
    {
        public CloudTableEntity()
        {

        }
        
        public CloudTableEntity(string partitionKey = null, string rowKey = null, ) 

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            throw new NotImplementedException();
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }
    }
}