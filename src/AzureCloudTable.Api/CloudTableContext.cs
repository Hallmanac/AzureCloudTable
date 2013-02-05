using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace AzureCloudTable.Api
{
    public class CloudTableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private CloudTable _table;
        private TableReadWriteContext<CloudTableEntity<TDomainEntity>> _tableReadWriteContext;

        private List<PartitionSchema<TDomainEntity>> _partitionSchemas; 

        private CloudTableEntity<TableMetaData<TDomainEntity>> _tableMetaDataEntity;
        private TableReadWriteContext<CloudTableEntity<TableMetaData<TDomainEntity>>> _metadataReadWriteContext;  

        public CloudTableContext(CloudStorageAccount storageAccount, string nameOfEntityIdProperty)
        {
            var tableName = string.Format("{0}Table", typeof(TDomainEntity).Name);
            Init(storageAccount, nameOfEntityIdProperty, tableName);
        }

        public CloudTableContext(CloudStorageAccount storageAccount, string nameOfEntityIdProperty, string tableName)
        {
            tableName = string.IsNullOrWhiteSpace(tableName) ? string.Format("{0}Table", typeof(TDomainEntity).Name) : tableName;
            Init(storageAccount, nameOfEntityIdProperty, tableName);
        }

        /// <summary>
        /// This is the name of the property that is used to store the ID of the Domain Entity.
        /// <para>For example, if there is a domain entity of type User that has a property named "Id" then one would pass
        /// the name of that property ("Id") into the constructor of the CloudTableContext class.</para>
        /// <para>This could be done using the extension method (on Object) called "GetPropertyName"</para>
        /// </summary>
        public string NameOfEntityIdProperty { get; set; }

        public PartitionSchema<TDomainEntity> DefaultSchema { get; private set; }

        public TableReadWriteContext<CloudTableEntity<TDomainEntity>> TableQueryContext()
        {
            return _tableReadWriteContext;
        }

        public void AddMultiplePartitionSchemas(List<PartitionSchema<TDomainEntity>> partitionSchemas)
        {
            var canWritePartition = false;
            foreach(var schema in partitionSchemas)
            {
                if(PartitionExists(schema.PartitionName)) continue;
                CreateNewPartitionSchema(schema);
                canWritePartition = true;
            }
            if(canWritePartition)
                _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
        }

        public void AddPartitionSchema(PartitionSchema<TDomainEntity> partitionSchema)
        {
            if (PartitionExists(partitionSchema.PartitionName)) return;
            CreateNewPartitionSchema(partitionSchema);
            _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
        }

        public void InsertOrMerge(TDomainEntity domainEntity)
        {
            var cloudTableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
            ValidateTableEntityAgainstPartitionSchemas(cloudTableEntity);
            foreach(var partitionSchema in _partitionSchemas)
            {
                var tableEntitiesInSchema = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.InsertOrMerge(tableEntitiesInSchema);
            }
        }

        public void InsertOrMerge(TDomainEntity[] domainEntities)
        {
            foreach (var domainEntity in domainEntities)
            {
                var tableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
                ValidateTableEntityAgainstPartitionSchemas(tableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.InsertOrMerge(entitiesInSchemaList);
            }
        }

        public void InsertOrReplace(TDomainEntity domainEntity)
        {
            var cloudTableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
            ValidateTableEntityAgainstPartitionSchemas(cloudTableEntity);
            foreach (var partitionSchema in _partitionSchemas)
            {
                var tableEntitiesSchema = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.InsertOrReplace(tableEntitiesSchema);
            }
        }

        public void InsertOrReplace(TDomainEntity[] domainEntities)
        {
            foreach (var domainEntity in domainEntities)
            {
                var tableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
                ValidateTableEntityAgainstPartitionSchemas(tableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
            }
        }

        public void Insert(TDomainEntity domainEntity)
        {
            var cloudTableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
            ValidateTableEntityAgainstPartitionSchemas(cloudTableEntity);
            foreach (var partitionSchema in _partitionSchemas)
            {
                var tableEntities = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.Insert(tableEntities);
            }
        }

        public void Insert(TDomainEntity[] domainEntities)
        {
            foreach (var domainEntity in domainEntities)
            {
                var tableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
                ValidateTableEntityAgainstPartitionSchemas(tableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.Insert(entitiesInSchemaList);
            }
        }

        public void Delete(TDomainEntity domainEntity)
        {
            var cloudTableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
            ValidateTableEntityAgainstPartitionSchemas(cloudTableEntity);
            foreach (var partitionSchema in _partitionSchemas)
            {
                var tableEntities = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.Delete(tableEntities);
            }
        }

        public void Delete(TDomainEntity[] domainEntities)
        {
            foreach (var domainEntity in domainEntities)
            {
                var tableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
                ValidateTableEntityAgainstPartitionSchemas(tableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.Delete(entitiesInSchemaList);
            }
        }

        public void Replace(TDomainEntity domainEntity)
        {
            var cloudTableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
            ValidateTableEntityAgainstPartitionSchemas(cloudTableEntity);
            foreach (var partitionSchema in _partitionSchemas)
            {
                var tableEntities = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.Replace(tableEntities);
            }
        }

        /// <summary>
        /// Replaces an array of Domain Entities in all Partition Schemas associated with the current CloudTableContext.
        /// </summary>
        /// <param name="domainEntities"></param>
        public void Replace(TDomainEntity[] domainEntities)
        {
            foreach (var domainEntity in domainEntities)
            {
                var tableEntity = new CloudTableEntity<TDomainEntity>
                    {
                        DomainObjectInstance = domainEntity
                    };
                ValidateTableEntityAgainstPartitionSchemas(tableEntity);
            }
            foreach (var partitionSchema in _partitionSchemas)
            {
                var entitiesInSchemaList = partitionSchema.CloudTableEntities.ToArray();
                _tableReadWriteContext.Replace(entitiesInSchemaList);
            }
        }

        /// <summary>
        /// Gets a domain entity using the partition schema's PartitionName (for the PartitionKey) and the entity's Id (for the RowKey).
        /// If the <param name="schema"></param> parameter is left null then the DefaultSchema is used.
        /// </summary>
        /// <param name="entityId"></param>
        /// <param name="schema"></param>
        /// <returns></returns>
        public TDomainEntity GetDomainEntity(object entityId, PartitionSchema<TDomainEntity> schema = null)
        {
            if(entityId == null)
                throw new ArgumentNullException("entityId");
            var entityIdToJsv = entityId.ToJsv();
            if(schema == null)
                schema = DefaultSchema;
            var tableEntity = _tableReadWriteContext.Find(schema.PartitionName, entityIdToJsv);
            return tableEntity.DomainObjectInstance;
        }

        /// <summary>
        /// Gets all the entities via the DefaultSchema.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetAll()
        {
            return
                _tableReadWriteContext.GetByPartitionKey(DefaultSchema.PartitionName)
                    .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        }

        public IEnumerable<TDomainEntity> GetAllInPartitionSchema(PartitionSchema<TDomainEntity> partitionSchema)
        {
            return
                _tableReadWriteContext.GetByPartitionKey(partitionSchema.PartitionName)
                    .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        }

        public IEnumerable<TDomainEntity> GetByPartitionSchemaWithRowkeyRange(string partitionKey, string minRowKey = "",
            string maxRowKey = "")
        {
            return
                _tableReadWriteContext.GetByPartitionKeyWithRowKeyRange(partitionKey, minRowKey, maxRowKey)
                    .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        }

        public IEnumerable<TDomainEntity> GetByPartitionFilteredOnIndexProperty(PartitionSchema<TDomainEntity> schema, object indexedProperty)
        {
            var indexedPropertyJsv = indexedProperty.ToJsv();
            var tempCloudTableEntity = new CloudTableEntity<TDomainEntity>();
            var nameOfIndexedProp = tempCloudTableEntity.GetPropertyName(() => tempCloudTableEntity.IndexedProperty);
            var returnedEntities = _tableReadWriteContext.QueryWherePropertyEquals(schema.PartitionName,
                nameOfIndexedProp, indexedPropertyJsv);
            return returnedEntities.Select(cloudTableEntity => cloudTableEntity.DomainObjectInstance);
        }

        private void Init(CloudStorageAccount storageAccount, string propertyNameOfEntityId, string tableName)
        {
            NameOfEntityIdProperty = propertyNameOfEntityId;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
            _partitionSchemas = new List<PartitionSchema<TDomainEntity>>();

            _metadataReadWriteContext = new TableReadWriteContext<CloudTableEntity<TableMetaData<TDomainEntity>>>(storageAccount,
                tableName);
            LoadTableMetaData(tableName);
            _tableReadWriteContext = new TableReadWriteContext<CloudTableEntity<TDomainEntity>>(storageAccount,
                tableName);
        }

        private void LoadTableMetaData(string tableName)
        {
            _tableMetaDataEntity = _metadataReadWriteContext.Find(tableName + "_Metadata", tableName);
            if(_tableMetaDataEntity != null)
            {
                foreach(var partitionScheme in _tableMetaDataEntity.DomainObjectInstance.PartitionSchemes)
                {
                    if(partitionScheme.PartitionName == "DefaultPartition")
                        DefaultSchema = partitionScheme;
                    CreateNewPartitionSchema(partitionScheme);
                }
            }
            else
            {
                _tableMetaDataEntity = new CloudTableEntity<TableMetaData<TDomainEntity>>(tableName + "_Metadata", tableName);
                DefaultSchema = new PartitionSchema<TDomainEntity>("DefaultPartition", entity => true);
                CreateNewPartitionSchema(DefaultSchema);
                _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
            }
        }

        private bool PartitionExists(string partitionName)
        {
            return _partitionSchemas.Any(partitionSchema => partitionSchema.PartitionName == partitionName);
        }

        private void CreateNewPartitionSchema(PartitionSchema<TDomainEntity> schema)
        {
            _partitionSchemas.Add(schema);

            if (_tableMetaDataEntity.DomainObjectInstance.PartitionSchemes.All(partitionSchema => partitionSchema.PartitionName != schema.PartitionName))
                _tableMetaDataEntity.DomainObjectInstance.PartitionSchemes.Add(schema);
        }

        private void ValidateTableEntityAgainstPartitionSchemas(CloudTableEntity<TDomainEntity> tableEntity)
        {
            foreach(var partitionSchema in _partitionSchemas)
            {
                if(partitionSchema.ValidationMethod(tableEntity.DomainObjectInstance))
                {
                    tableEntity.PartitionKey = partitionSchema.PartitionName;
                    tableEntity.RowKey = partitionSchema.SetRowKey(tableEntity);
                    if(tableEntity.RowKey == null)
                    {
                        if(string.IsNullOrWhiteSpace(NameOfEntityIdProperty))
                            tableEntity.SetDefaultRowKey();
                        else
                        {
                            var idPropertyValue =
                                typeof(TDomainEntity).GetProperty(NameOfEntityIdProperty)
                                    .GetValue(tableEntity.DomainObjectInstance);
                            if(idPropertyValue != null)
                            {
                                var idValueToJsv = idPropertyValue.ToJsv();
                                tableEntity.RowKey = idValueToJsv;
                            }
                            else
                            {
                                tableEntity.SetDefaultRowKey();
                            }
                        }
                    }
                    tableEntity.IndexedProperty = partitionSchema.SetIndexedProperty(tableEntity);
                    partitionSchema.CloudTableEntities.Add(tableEntity);
                }
            }
        }
    }
}