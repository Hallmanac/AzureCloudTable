using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using SimpleCqrs.Eventing;

namespace HallmanacAzureTable.EventStore
{
    public class CloudTableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private CloudTable _table;
        private readonly AzureTableContext<AzureTableEntity<TDomainEntity>> _tableContext; 

        private List<Tuple<string, Func<TDomainEntity, bool>, List<AzureTableEntity<TDomainEntity>>>> _partitionSchemas; 

        private AzureTableEntity<TableMetaData<TDomainEntity>> _tableMetaDataEntity;  

        public CloudTableContext(CloudStorageAccount storageAccount)
        {
            var tableName = string.Format("{0}Table", typeof(TDomainEntity).Name);
            Init(storageAccount, tableName);
        }

        public CloudTableContext(CloudStorageAccount storageAccount, string tableName)
        {
            tableName = string.IsNullOrWhiteSpace(tableName) ? string.Format("{0}Table", typeof(TDomainEntity).Name) : tableName;
            Init(storageAccount, tableName);
        }

        private void Init(CloudStorageAccount storageAccount, string tableName)
        {
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
            _tableMetaDataEntity = new AzureTableEntity<TableMetaData<TDomainEntity>>(tableName + "_Metadata", tableName);
            _partitionSchemas = new List<Tuple<string, Func<TDomainEntity, bool>, List<AzureTableEntity<TDomainEntity>>>>();
        }

        public void AddPartitionSchema(string partitionName, Func<TDomainEntity, bool> validationMethod)
        {
            var tupleExists = false;
            foreach(var tuple in _partitionSchemas)
            {
                if(tuple.Item1 == partitionName)
                {
                    tupleExists = true;
                }
            }
            if(tupleExists)
                return;
            var entityList = new List<AzureTableEntity<TDomainEntity>>();
            _partitionSchemas.Add(
                                new Tuple<string, Func<TDomainEntity, bool>, List<AzureTableEntity<TDomainEntity>>>(
                                    partitionName, validationMethod, entityList));
        }

        private void CheckEntitiesAgainstPartitionSchemes(IEnumerable<AzureTableEntity<TDomainEntity>> givenEntities)
        {
            foreach(var domainEntity in givenEntities)
            {
                AddTableEntityToPartitionScheme(domainEntity);
            }
        }

        private void AddTableEntityToPartitionScheme(AzureTableEntity<TDomainEntity> domainEntity)
        {
            foreach(var partitionSchema in _partitionSchemas)
            {
                if(partitionSchema.Item2(domainEntity.DomainObjectInstance))
                {
                    partitionSchema.Item3.Add(domainEntity);
                }
            }
        }

        //List<IPartitionScheme<TDomainEntity>> PartitionSchemes { get; set; } 

        //Insert(TDomainEntity domainObject){}
        //BatchInsert(TDomainEntity[] domainObjects{}
        //InsertOrMerge(TDomainEntity domainObject){}
        //BatchInsertOrMerge(TDomainEntity[] domainObjects){}
        //InsertOrReplace(TDomainEntity domainObject){}
        //BatchInsertOrReplace(TDomainEntity[] domainObjects){}
        //Delete(TDomainEntity domainObject){}
        //BatchDelete(TDomainEntity[] domainObjects){}

        //TDomainEntity Find(object entityId){/*Get by using the default partition Key and the entityId as the rowKey*/}
        //IEnumerable<TDomainEntity> GetPartition(string partitionKey){}
        //IEnumerable<TDomainEntity> GetPartitionWithinRowKeyRange(string primaryKey, string minRowKey = "", string maxRowKey = ""){}
        //QueryByIndexedValue(string partitionKey, object indexedProperty){}

        //CreateNewPartition(string partitionKeyName, string rowKeyName){}
    }
}