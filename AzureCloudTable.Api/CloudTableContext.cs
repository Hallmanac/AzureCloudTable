using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace HallmanacAzureTable.EventStore
{
    public class CloudTableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private readonly string _getAllEntitiesPartitionName = default(Guid).ToString();
        private CloudTable _table;
        private TableReadWriteContext<CloudTableEntity<TDomainEntity>> _tableReadWriteContext;

        private List<Tuple<string, Func<TDomainEntity, bool>, List<CloudTableEntity<TDomainEntity>>>> _partitionSchemas; 

        private CloudTableEntity<TableMetaData<TDomainEntity>> _tableMetaDataEntity;
        private TableReadWriteContext<CloudTableEntity<TableMetaData<TDomainEntity>>> _metadataReadWriteContext;  

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

        public TableReadWriteContext<CloudTableEntity<TDomainEntity>> TableQueryContext()
        {
            return _tableReadWriteContext;
        }

        public void AddMultiplePartitionSchemas(Dictionary<string, Func<TDomainEntity, bool>> partitionScheme)
        {
            var canWritePartition = false;
            foreach(var pair in partitionScheme)
            {
                if(PartitionExists(pair.Key)) continue;
                canWritePartition = true;
            }
            if(canWritePartition)
                _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
        }

        public void AddPartitionSchema(string partitionName, Func<TDomainEntity, bool> validationMethod)
        {
            if(PartitionExists(partitionName)) return;
            CreateNewPartitionSchema(partitionName, validationMethod);
            _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
        }

        public void InsertOrMerge(CloudTableEntity<TDomainEntity> cloudTableEntity)
        {
            ValidateTableEntityAgainstPartitionSchemes(cloudTableEntity);
            foreach(var partitionSchema in _partitionSchemas)
            {
                partitionSchema.Item3.ForEach(tableEntity => _tableReadWriteContext.InsertOrMerge(tableEntity));
            }
        }

        public void InsertOrMerge(CloudTableEntity<TDomainEntity>[] cloudTableEntities)
        {
            foreach (var azureTableEntity in cloudTableEntities)
            {
                ValidateTableEntityAgainstPartitionSchemes(azureTableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.Item3.ToArray();
                _tableReadWriteContext.InsertOrMerge(entitiesInSchemaList);
            }
        }

        public void InsertOrReplace(CloudTableEntity<TDomainEntity> cloudTableEntity)
        {
            ValidateTableEntityAgainstPartitionSchemes(cloudTableEntity);
            foreach (var partitionSchema in _partitionSchemas)
            {
                partitionSchema.Item3.ForEach(tableEntity => _tableReadWriteContext.InsertOrReplace(tableEntity));
            }
        }

        public void InsertOrReplace(CloudTableEntity<TDomainEntity>[] cloudTableEntities)
        {
            foreach (var azureTableEntity in cloudTableEntities)
            {
                ValidateTableEntityAgainstPartitionSchemes(azureTableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.Item3.ToArray();
                _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
            }
        }

        public void Insert(CloudTableEntity<TDomainEntity> cloudTableEntity)
        {
            ValidateTableEntityAgainstPartitionSchemes(cloudTableEntity);
            foreach (var partitionSchema in _partitionSchemas)
            {
                partitionSchema.Item3.ForEach(tableEntity => _tableReadWriteContext.Insert(tableEntity));
            }
        }

        public void Insert(CloudTableEntity<TDomainEntity>[] cloudTableEntities)
        {
            foreach (var azureTableEntity in cloudTableEntities)
            {
                ValidateTableEntityAgainstPartitionSchemes(azureTableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.Item3.ToArray();
                _tableReadWriteContext.Insert(entitiesInSchemaList);
            }
        }

        public void Delete(CloudTableEntity<TDomainEntity> cloudTableEntity)
        {
            ValidateTableEntityAgainstPartitionSchemes(cloudTableEntity);
            foreach (var partitionSchema in _partitionSchemas)
            {
                partitionSchema.Item3.ForEach(tableEntity => _tableReadWriteContext.Delete(tableEntity));
            }
        }

        public void Delete(CloudTableEntity<TDomainEntity>[] cloudTableEntities)
        {
            foreach (var azureTableEntity in cloudTableEntities)
            {
                ValidateTableEntityAgainstPartitionSchemes(azureTableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.Item3.ToArray();
                _tableReadWriteContext.Delete(entitiesInSchemaList);
            }
        }

        public void Replace(CloudTableEntity<TDomainEntity> cloudTableEntity)
        {
            ValidateTableEntityAgainstPartitionSchemes(cloudTableEntity);
            foreach (var partitionSchema in _partitionSchemas)
            {
                partitionSchema.Item3.ForEach(tableEntity => _tableReadWriteContext.Replace(tableEntity));
            }
        }

        public void Replace(CloudTableEntity<TDomainEntity>[] cloudTableEntities )
        {
            foreach (var azureTableEntity in cloudTableEntities)
            {
                ValidateTableEntityAgainstPartitionSchemes(azureTableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.Item3.ToArray();
                _tableReadWriteContext.Replace(entitiesInSchemaList);
            }
        }

        public TDomainEntity FindDomainObject(string partitionKey, string rowKey)
        {
            var tableEntity = _tableReadWriteContext.Find(partitionKey, rowKey);
            return tableEntity.DomainObjectInstance;
        }

        public IEnumerable<TDomainEntity> GetAll()
        {
            foreach(var azureTableEntity in _tableReadWriteContext.GetByPartitionKey(_getAllEntitiesPartitionName))
            {
                yield return azureTableEntity.DomainObjectInstance;
            }
        }

        public IEnumerable<TDomainEntity> GetByPartitionKey(string partitionKey)
        {
            foreach(var azureTableEntity in _tableReadWriteContext.GetByPartitionKey(partitionKey))
            {
                yield return azureTableEntity.DomainObjectInstance;
            } 
        }

        public IEnumerable<TDomainEntity> GetByPartitionKeyWithinRowkeyRange(string partitionKey, string minRowKey = "",
            string maxRowKey = "")
        {
            foreach(var azureTableEntity in _tableReadWriteContext.GetByPartitionKeyWithRowKeyRange(partitionKey, minRowKey, maxRowKey))
            {
                yield return azureTableEntity.DomainObjectInstance;
            }
        }

        public IEnumerable<TDomainEntity> GetIndexedPropertyEqualTo(string partitionKey, object indexedProperty)
        {
            var indexedPropertyJsv = indexedProperty.ToJsv();
            var azureEntity = new CloudTableEntity<TDomainEntity>();
            var returnedEntities = _tableReadWriteContext.QueryWherePropertyEquals(partitionKey,
                azureEntity.GetPropertyName(() => azureEntity.IndexedProperty), indexedPropertyJsv);
            foreach(var cloudTableEntity in returnedEntities)
            {
                yield return cloudTableEntity.DomainObjectInstance;
            }
        }

        private void Init(CloudStorageAccount storageAccount, string tableName)
        {
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
            _partitionSchemas = new List<Tuple<string, Func<TDomainEntity, bool>, List<CloudTableEntity<TDomainEntity>>>>();

            _metadataReadWriteContext = new TableReadWriteContext<CloudTableEntity<TableMetaData<TDomainEntity>>>(storageAccount,
                tableName);
            _tableMetaDataEntity = _metadataReadWriteContext.Find(tableName + "_Metadata", tableName);
            if(_tableMetaDataEntity != null)
            {
                foreach(var partitionScheme in _tableMetaDataEntity.DomainObjectInstance.PartitionSchemes)
                {
                    CreateNewPartitionSchema(partitionScheme.Key, partitionScheme.Value);
                }
            }
            else
            {
                _tableMetaDataEntity = new CloudTableEntity<TableMetaData<TDomainEntity>>(tableName + "_Metadata", tableName);
                CreateNewPartitionSchema(_getAllEntitiesPartitionName, entity => true);
                _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
            }
            _tableReadWriteContext = new TableReadWriteContext<CloudTableEntity<TDomainEntity>>(storageAccount,
                tableName);
        }

        private bool PartitionExists(string partitionName)
        {
            return _partitionSchemas.Any(tuple => tuple.Item1 == partitionName);
        }

        private void CreateNewPartitionSchema(string partitionName, Func<TDomainEntity, bool> validationMethod)
        {
            var entityList = new List<CloudTableEntity<TDomainEntity>>();
            _partitionSchemas.Add(
                                  new Tuple<string, Func<TDomainEntity, bool>, List<CloudTableEntity<TDomainEntity>>>(
                                      partitionName, validationMethod, entityList));
            if(!_tableMetaDataEntity.DomainObjectInstance.PartitionSchemes.ContainsKey(partitionName))
                _tableMetaDataEntity.DomainObjectInstance.PartitionSchemes.Add(partitionName, validationMethod);
        }

        private void ValidateTableEntityAgainstPartitionSchemes(CloudTableEntity<TDomainEntity> domainEntity)
        {
            foreach(var partitionSchema in _partitionSchemas)
            {
                if(partitionSchema.Item2(domainEntity.DomainObjectInstance))
                {
                    domainEntity.PartitionKey = partitionSchema.Item1;
                    partitionSchema.Item3.Add(domainEntity);
                }
            }
        }
    }
}