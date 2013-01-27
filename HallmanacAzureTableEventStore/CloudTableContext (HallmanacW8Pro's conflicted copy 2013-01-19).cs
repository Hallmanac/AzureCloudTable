using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using SimpleCqrs.Eventing;

namespace HallmanacAzureTable.EventStore
{
    public class CloudTableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private readonly CloudTable _table;

        public CloudTableContext(CloudStorageAccount storageAccount)
        {
            var tableName = string.Format("{0}Table", typeof(TDomainEntity).Name);
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
        }

        public CloudTableContext(CloudStorageAccount storageAccount, string tableName)
        {
            tableName = string.IsNullOrWhiteSpace(tableName)
                ? string.Format("{0}Table", typeof(TDomainEntity).Name) : tableName;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
        }

        public List<string> PartitionKeys { get; set; }
        //CreatePartition(string partitionKeyValue, string rowKeyValue = "")
    }
}