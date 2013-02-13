using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace AzureCloudTable.Api
{
    /// <summary>
    /// Class used to wrap a domain entity for use with Azure Table Storage via using PartitionKey strategies (known as PartitionSchemas) 
    /// for grouping and filtering.
    /// </summary>
    /// <typeparam name="TDomainEntity"></typeparam>
    public class CloudTableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private CloudTable _table;
        private TableReadWriteContext<CloudTableEntity<TDomainEntity>> _tableReadWriteContext;

        private List<PartitionSchema<TDomainEntity>> _partitionSchemas; 

        private CloudTableEntity<TableMetaData<TDomainEntity>> _tableMetaDataEntity;
        private TableReadWriteContext<CloudTableEntity<TableMetaData<TDomainEntity>>> _metadataReadWriteContext;
        private string _defaultSchemaName;

        /*public CloudTableContext(CloudStorageAccount storageAccount, string nameOfEntityIdProperty)
        {
            var tableName = string.Format("{0}Table", typeof(TDomainEntity).Name);
            Init(storageAccount, nameOfEntityIdProperty, tableName);
        }*/

        /// <summary>
        /// Initializes a new CloudTableContext object. If the <param name="tableName"></param> parameter is left null, then the default 
        /// naming scheme used is the name of the generic type's name with "Table" appended to it. For example "SomeClass" + "Table" for
        /// the table name of "SomeClassTable".
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="nameOfEntityIdProperty"></param>
        /// <param name="tableName"></param>
        public CloudTableContext(CloudStorageAccount storageAccount, string nameOfEntityIdProperty, string tableName = null)
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

        /// <summary>
        /// Gets the default partition partitionKey used for the table.
        /// </summary>
        public PartitionSchema<TDomainEntity> DefaultSchema { get; private set; }

        /// <summary>
        /// Returns a TableReadWriteContext class which allows for more options in constructing custom queries against the table.
        /// </summary>
        /// <returns></returns>
        public TableQuery<CloudTableEntity<TDomainEntity>> TableQuery()
        {
            return _tableReadWriteContext.Query();
        }

        /// <summary>
        /// Adds multiple PartitionSchema types to the current CloudTableContext. 
        /// </summary>
        /// <param name="partitionSchemas"></param>
        public void AddMultiplePartitionSchemas(List<PartitionSchema<TDomainEntity>> partitionSchemas)
        {
            var canWritePartition = false;
            foreach(var schema in partitionSchemas)
            {
                if(PartitionExists(schema.SchemaName)) continue;
                CreateNewPartitionSchema(schema);
                canWritePartition = true;
            }
            if(canWritePartition)
                _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
        }

        /// <summary>
        /// Adds a single PartitionSchema to the current CloudTableContext.
        /// </summary>
        /// <param name="partitionSchema"></param>
        public void AddPartitionSchema(PartitionSchema<TDomainEntity> partitionSchema)
        {
            if (PartitionExists(partitionSchema.SchemaName)) return;
            CreateNewPartitionSchema(partitionSchema);
            _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
        }

        /// <summary>
        /// Executes a single "InsertOrMerge" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes a batch "InsertOrMerge" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes a single "InsertOrReplace" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes batch "InsertOrReplace" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes a single "Insert" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes a batch "Insert" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes a single "Delete" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes a batch "Delete" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes a single "Replace" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Executes a batch "Replace" table operation.
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
                partitionSchema.CloudTableEntities.Clear();
            }
        }

        /// <summary>
        /// Gets all the entities via the DefaultSchema.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetAll()
        {
            return
                _tableReadWriteContext.GetByPartitionKey(DefaultSchema.SchemaName)
                    .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        }

        /// <summary>
        /// Gets a domain entity using the partition partitionKey's SchemaName (for the PartitionKey) and the entity's Id (for the RowKey).
        /// If the <param name="partitionKey"></param> parameter is left null then the DefaultSchema is used.
        /// </summary>
        /// <param name="entityId"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public TDomainEntity GetById(object entityId, string partitionKey = null)
        {
            if(entityId == null)
                throw new ArgumentNullException("entityId");
            var entityIdToJsv = entityId.ToJsv();
            if(partitionKey == null)
                partitionKey = DefaultSchema.SchemaName;
            var tableEntity = _tableReadWriteContext.Find(partitionKey, entityIdToJsv);
            return tableEntity.DomainObjectInstance;
        }

        /// <summary>
        /// Retrieves all domain entities within a given PartitionKey.
        /// </summary>
        /// <param name="partitionKey">If the object being passed in is not a string, it gets serialized to a Jsv string (a la 
        /// ServiceStack.Text library) and that string gets used as a PartitionKey.</param>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetByPartitionKey(object partitionKey)
        {
            if(partitionKey is string)
            {
                return
                    _tableReadWriteContext.GetByPartitionKey(partitionKey as string)
                        .Select(tableEntity => tableEntity.DomainObjectInstance);
            }
            return
                _tableReadWriteContext.GetByPartitionKey(partitionKey.ToJsv())
                    .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        } 

        /// <summary>
        /// Retrieves a set of domain entities based on a given PartitionScheme and an optional RowKey range.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetByPartitionKeyWithRowkeyRange(string partitionKey, string minRowKey = "",
            string maxRowKey = "")
        {
            return
                _tableReadWriteContext.GetByPartitionKeyWithRowKeyRange(partitionKey, minRowKey, maxRowKey)
                    .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        }

        /// <summary>
        /// Gets a set of domain entities based on a given ParitionSchema with a filter based on the <param name="indexedProperty"></param> that 
        /// gets passed in.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="indexedProperty"></param>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> QueryWhereIndexedPropertyEquals(object partitionKey, object indexedProperty)
        {
            var indexedPropertyJsv = indexedProperty.ToJsv();
            var tempCloudTableEntity = new CloudTableEntity<TDomainEntity>();
            var nameOfIndexedProp = tempCloudTableEntity.GetPropertyName(() => tempCloudTableEntity.IndexedProperty);
            if(partitionKey is string)
            {
                return
                    _tableReadWriteContext.QueryWherePropertyEquals(partitionKey as string, nameOfIndexedProp, indexedPropertyJsv)
                        .Select(tableEntity => tableEntity.DomainObjectInstance);
            }

            return _tableReadWriteContext.QueryWherePropertyEquals(partitionKey.ToJsv(),
                nameOfIndexedProp, indexedPropertyJsv).Select(cloudTableEntity => cloudTableEntity.DomainObjectInstance);
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
            var metadataRowKeyName = "metadata";
            _tableMetaDataEntity = _metadataReadWriteContext.Find(tableName + "_Metadata", metadataRowKeyName);
            _defaultSchemaName = "DefaultPartition";
            if(_tableMetaDataEntity != null)
            {
                foreach(var partitionScheme in _tableMetaDataEntity.DomainObjectInstance.PartitionSchemes)
                {
                    if(partitionScheme.SchemaName == _defaultSchemaName)
                        DefaultSchema = partitionScheme;
                    CreateNewPartitionSchema(partitionScheme);
                }
            }
            else
            {
                _tableMetaDataEntity = new CloudTableEntity<TableMetaData<TDomainEntity>>(tableName + "_Metadata",
                    metadataRowKeyName);
                DefaultSchema = new PartitionSchema<TDomainEntity>(_defaultSchemaName, entity => true,
                    entity => _defaultSchemaName, entity => "");
                CreateNewPartitionSchema(DefaultSchema);
                _metadataReadWriteContext.InsertOrReplace(_tableMetaDataEntity);
            }
        }

        private bool PartitionExists(string partitionName)
        {
            return _partitionSchemas.Any(partitionSchema => partitionSchema.SchemaName == partitionName);
        }

        private void CreateNewPartitionSchema(PartitionSchema<TDomainEntity> schema)
        {
            _partitionSchemas.Add(schema);

            if (_tableMetaDataEntity.DomainObjectInstance.PartitionSchemes.All(partitionSchema => partitionSchema.SchemaName != schema.SchemaName))
                _tableMetaDataEntity.DomainObjectInstance.PartitionSchemes.Add(schema);
        }

        private void ValidateTableEntityAgainstPartitionSchemas(CloudTableEntity<TDomainEntity> tableEntity)
        {
            foreach(var partitionSchema in _partitionSchemas)
            {
                if(partitionSchema.ValidationMethod(tableEntity.DomainObjectInstance))
                {
                    tableEntity.PartitionKey = partitionSchema.SetPartitionKey(tableEntity.DomainObjectInstance);
                    tableEntity.RowKey = partitionSchema.SetRowKeyValue(tableEntity.DomainObjectInstance);
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
                    var indexedPropertyObject = partitionSchema.SetIndexedProperty(tableEntity.DomainObjectInstance);
                    tableEntity.IndexedProperty = new IndexedObject
                        {
                            ValueBeingIndexed = indexedPropertyObject
                        };
                    partitionSchema.CloudTableEntities.Add(tableEntity);
                }
            }
        }
    }
}