using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace AzureCloudTableContext.Api
{
    /// <summary>
    /// Class used to wrap a domain entity for use with Azure Table Storage via using PartitionKey strategies (known as PartitionSchemas) 
    /// for grouping and filtering.
    /// </summary>
    /// <typeparam name="TDomainEntity"></typeparam>
    public class CloudTableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private readonly string _tableMetaDataPartitionKey = "TableMetaData";
        private readonly string _partitionSchemasRowKey = "PartitionSchemas";
        
        private CloudTable _table;
        private TableReadWriteContext<CloudTableEntity<TDomainEntity>> _tableReadWriteContext;

        private List<PartitionSchema<TDomainEntity>> _partitionSchemas; 

        private CloudTableEntity<PartitionMetaData> _partitionMetaDataEntity;
        private TableReadWriteContext<CloudTableEntity<PartitionMetaData>> _metadataReadWriteContext;
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
        /// Gets a list of the PartitionKeys that are used in the table.
        /// </summary>
        public List<string> PartitionKeysInTable { get; private set; } 
        
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
                _metadataReadWriteContext.InsertOrReplace(_partitionMetaDataEntity);
        }

        /// <summary>
        /// Adds a single PartitionSchema to the current CloudTableContext.
        /// </summary>
        /// <param name="partitionSchema"></param>
        public void AddPartitionSchema(PartitionSchema<TDomainEntity> partitionSchema)
        {
            if (PartitionExists(partitionSchema.SchemaName)) return;
            CreateNewPartitionSchema(partitionSchema);
            _metadataReadWriteContext.InsertOrReplace(_partitionMetaDataEntity);
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    foreach(var keyValuePair in partitionSchema.CloudTableEntities)
                    {
                        var entitiesInSchemaList = keyValuePair.Value.ToArray();
                        _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                        keyValuePair.Value.Clear();
                    }
                    partitionSchema.CloudTableEntities.Clear();
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
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
                if(partitionSchema.CloudTableEntities.Count > 0)
                {
                    if (partitionSchema.CloudTableEntities.Count > 0)
                    {
                        foreach (var keyValuePair in partitionSchema.CloudTableEntities)
                        {
                            var entitiesInSchemaList = keyValuePair.Value.ToArray();
                            _tableReadWriteContext.InsertOrReplace(entitiesInSchemaList);
                            keyValuePair.Value.Clear();
                        }
                        partitionSchema.CloudTableEntities.Clear();
                    }
                }
            }
        }

        /// <summary>
        /// Gets all the entities via the DefaultSchema.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetAll()
        {
            return
                _tableReadWriteContext.GetByPartitionKey(_defaultSchemaName)
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
        public IEnumerable<TDomainEntity> QueryWhereIndexedPropertyEquals(string partitionKey, object indexedProperty)
        {
            var tempCloudTableEntity = new CloudTableEntity<TDomainEntity>
                {
                    IndexedProperty =
                        {
                            ValueBeingIndexed = indexedProperty
                        }
                };
            var indexedPropertyJsv = tempCloudTableEntity.IndexedProperty.ToJsv();
            var nameOfIndexedProp = tempCloudTableEntity.GetPropertyName(() => tempCloudTableEntity.IndexedProperty);
            
            return _tableReadWriteContext.QueryWherePropertyEquals(partitionKey,
                nameOfIndexedProp, indexedPropertyJsv).Select(cloudTableEntity => cloudTableEntity.DomainObjectInstance);
        }

        private void Init(CloudStorageAccount storageAccount, string propertyNameOfEntityId, string tableName)
        {
            PartitionKeysInTable = new List<string>();
            NameOfEntityIdProperty = propertyNameOfEntityId;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
            _partitionSchemas = new List<PartitionSchema<TDomainEntity>>();

            _metadataReadWriteContext = new TableReadWriteContext<CloudTableEntity<PartitionMetaData>>(storageAccount,
                tableName);
            LoadTableMetaData();
            _tableReadWriteContext = new TableReadWriteContext<CloudTableEntity<TDomainEntity>>(storageAccount,
                tableName);
        }

        private void LoadTableMetaData()
        {
            _partitionMetaDataEntity = _metadataReadWriteContext.Find(_tableMetaDataPartitionKey,
                _partitionSchemasRowKey);
            _defaultSchemaName = "DefaultPartition";
            if(_partitionMetaDataEntity != null)
            {
                bool metaDataPkIsInList = false;
                foreach(var partitionKeyString in _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys)
                {
                    if(partitionKeyString == _tableMetaDataPartitionKey)
                        metaDataPkIsInList = true;
                    bool isInList = false;
                    foreach(var item in PartitionKeysInTable)
                    {
                        if(item == partitionKeyString)
                            isInList = true;
                    }
                    if(!isInList)
                        PartitionKeysInTable.Add(partitionKeyString);
                }
                if(!metaDataPkIsInList)
                    PartitionKeysInTable.Add(_tableMetaDataPartitionKey);
                DefaultSchema = new PartitionSchema<TDomainEntity>(schemaName: _defaultSchemaName,
                    validateEntityForPartition: entity => true, setPartitionKey: entity => _defaultSchemaName);
                if(!(PartitionExists(DefaultSchema.SchemaName)))
                {
                    CreateNewPartitionSchema(DefaultSchema);
                }
            }
            else
            {
                _partitionMetaDataEntity = new CloudTableEntity<PartitionMetaData>(_tableMetaDataPartitionKey,
                    _partitionSchemasRowKey);
                DefaultSchema = new PartitionSchema<TDomainEntity>(schemaName: _defaultSchemaName,
                    validateEntityForPartition: entity => true, setPartitionKey: entity => _defaultSchemaName);
                CreateNewPartitionSchema(DefaultSchema);
                _metadataReadWriteContext.InsertOrReplace(_partitionMetaDataEntity);
            }
        }

        private bool PartitionExists(string partitionName)
        {
            return _partitionSchemas.Any(partitionSchema => partitionSchema.SchemaName == partitionName);
        }

        private void CreateNewPartitionSchema(PartitionSchema<TDomainEntity> schema)
        {
            _partitionSchemas.Add(schema);

            /*if (_partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.All(partitionSchema => partitionSchema != schema.SchemaName))
                _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(schema.SchemaName);*/
        }

        private void ValidateTableEntityAgainstPartitionSchemas(CloudTableEntity<TDomainEntity> tableEntity)
        {
            foreach(var partitionSchema in _partitionSchemas)
            {
                if(partitionSchema.ValidateEntityForPartition(tableEntity.DomainObjectInstance))
                {
                    var tempTableEntity =
                        new CloudTableEntity<TDomainEntity>(domainObject: tableEntity.DomainObjectInstance);
                    tempTableEntity.PartitionKey = partitionSchema.SetPartitionKey(tempTableEntity.DomainObjectInstance);
                    if(
                        _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.All(
                                                                                               schemaName =>
                                                                                                   schemaName !=
                                                                                                       tempTableEntity
                                                                                                           .PartitionKey))
                    {
                        _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(tempTableEntity.PartitionKey);
                        SavePartitionKeys();
                    }
                    
                    tempTableEntity.RowKey = partitionSchema.SetRowKeyValue(tempTableEntity.DomainObjectInstance);
                    if(tempTableEntity.RowKey == null)
                    {
                        if(string.IsNullOrWhiteSpace(NameOfEntityIdProperty))
                            tempTableEntity.SetDefaultRowKey();
                        else
                        {
                            var idPropertyValue =
                                typeof(TDomainEntity).GetProperty(NameOfEntityIdProperty)
                                    .GetValue(tempTableEntity.DomainObjectInstance);
                            if(idPropertyValue != null)
                            {
                                var idValueToJsv = idPropertyValue.ToJsv();
                                tempTableEntity.RowKey = idValueToJsv;
                            }
                            else
                            {
                                tempTableEntity.SetDefaultRowKey();
                            }
                        }
                    }
                    var indexedPropertyObject = partitionSchema.SetIndexedProperty(tempTableEntity.DomainObjectInstance);
                    tempTableEntity.IndexedProperty = new IndexedObject
                        {
                            ValueBeingIndexed = indexedPropertyObject
                        };
                    if((partitionSchema.CloudTableEntities.ContainsKey(tempTableEntity.PartitionKey)))
                    {
                        partitionSchema.CloudTableEntities[tempTableEntity.PartitionKey].Add(tempTableEntity);
                    }
                    else
                    {
                        partitionSchema.CloudTableEntities.Add(tempTableEntity.PartitionKey,
                            new List<CloudTableEntity<TDomainEntity>>
                                {
                                    tempTableEntity
                                });
                    }
                }
            }
        }

        private void SavePartitionKeys()
        {
            _metadataReadWriteContext.InsertOrReplace(_partitionMetaDataEntity);
        }
    }
}