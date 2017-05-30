using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace AzureCloudTableContext.Api
{
    /// <summary>
    ///     Class used to wrap a domain entity for use with Azure Table Storage via using PartitionKey strategies (known as
    ///     PartitionSchemas) for grouping and filtering.
    /// </summary>
    /// <typeparam name="TDomainEntity"></typeparam>
    public class CloudTableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private string _defaultIndexDefinitionName;
        private bool _needToRunTableIndices;
        private CloudTableEntity<PartitionMetaData> _partitionMetaDataEntity;
        private readonly TableAccessContext<CloudTableEntity<PartitionMetaData>> _tableMetaDataContext;


        /// <summary>
        /// Initializes a new CloudTableContext object. If the "tableName" parameter is left null, then 
        /// the default naming scheme used is the name of the generic type's name with "Table" appended 
        /// to it. For example "SomeClass" + "Table" for the table name of "SomeClassTable".
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="nameOfEntityIdProperty"></param>
        /// <param name="tableName"></param>
        public CloudTableContext(CloudStorageAccount storageAccount, string nameOfEntityIdProperty, string tableName = null)
        {
            tableName = string.IsNullOrWhiteSpace(tableName) ? $"{typeof(TDomainEntity).Name}Table" : tableName;
            NameOfEntityIdProperty = nameOfEntityIdProperty;
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();
            _tableMetaDataContext = new TableAccessContext<CloudTableEntity<PartitionMetaData>>(storageAccount, tableName);
            LoadTableMetaData();
            TableAccessContext = new TableAccessContext<CloudTableEntity<TDomainEntity>>(storageAccount, tableName);
        }

        /// <summary>
        /// Gives direct access to the underlying TableAccessContext class that does the interaction with the Azure Table.
        /// </summary>
        public TableAccessContext<CloudTableEntity<TDomainEntity>> TableAccessContext { get; }

        /// <summary>
        ///     Gets a list of the PartitionKeys that are used in the table.
        /// </summary>
        public List<string> PartitionKeysInTable { get; } = new List<string>();

        /// <summary>
        ///     Runtime list of active partition schemas.
        /// </summary>
        public List<AzureTableIndexDefinition<TDomainEntity>> IndexDefinitions { get; set; } = new List<AzureTableIndexDefinition<TDomainEntity>>();

        /// <summary>
        ///     This is the name of the property that is used to store the ID of the Domain Entity.
        ///     <para>
        ///         For example, if there is a domain entity of type User that has a property named "Id" then one would pass
        ///         the name of that property ("Id") into the constructor of the CloudTableContext class.
        ///     </para>
        ///     <para>This could be done using the extension method (on Object) called "GetPropertyName"</para>
        /// </summary>
        public string NameOfEntityIdProperty { get; }

        /// <summary>
        ///     Gets the default partition partitionKey used for the table.
        /// </summary>
        public AzureTableIndexDefinition<TDomainEntity> DefaultIndex { get; private set; }

        /// <summary>
        ///     Returns a TableAccessContext class which allows for more options in constructing custom queries against the table.
        /// </summary>
        /// <returns></returns>
        public TableQuery<CloudTableEntity<TDomainEntity>> TableQuery()
        {
            return TableAccessContext.Query();
        }

        /// <summary>
        ///     Creates a new PartitionSchema for the "TDomainEntity" based on the given partitionKey.
        ///     The PartitionSchema RowKey will be set based on the ID property of the "TDomainEntity".
        /// </summary>
        /// <param name="indexName"></param>
        /// <returns></returns>
        public AzureTableIndexDefinition<TDomainEntity> CreateIndexDefinition(string indexName)
        {
            var schema = new AzureTableIndexDefinition<TDomainEntity>(NameOfEntityIdProperty)
                .SetIndexNameKey(indexName);
            return schema;
        }

        /// <summary>
        /// Creates a new PartitionSchema object for the "TDomainEntity" with the PartitionKey being set based on
        /// the name of the type by default. The PartitionSchema RowKey will be set based on the ID property of the "TDomainEntity".
        /// </summary>
        /// <returns></returns>
        public AzureTableIndexDefinition<TDomainEntity> CreateIndexDefinition()
        {
            return CreateIndexDefinition(typeof(TDomainEntity).Name);
        }

        /// <summary>
        ///     Adds multiple PartitionSchema types to the current CloudTableContext.
        /// </summary>
        /// <param name="partitionSchemas"></param>
        public void AddMultipleIndexDefinitions(List<AzureTableIndexDefinition<TDomainEntity>> partitionSchemas)
        {
            foreach(var partitionSchema in partitionSchemas)
            {
                if(IndexDefinitions.Any(indexDef => indexDef.IndexNameKey == partitionSchema.IndexNameKey))
                {
                    continue;
                }
                IndexDefinitions.Add(partitionSchema);
            }
        }

        /// <summary>
        ///     Adds a single PartitionSchema to the current CloudTableContext.
        /// </summary>
        /// <param name="azureTableIndexDefinition"></param>
        public void AddIndexDefinition(AzureTableIndexDefinition<TDomainEntity> azureTableIndexDefinition)
        {
            if(IndexDefinitions.Any(indexDef => indexDef.IndexNameKey == azureTableIndexDefinition.IndexNameKey))
            {
                return;
            }
            IndexDefinitions.Add(azureTableIndexDefinition);
        }

        /// <summary>
        ///     A string for a row key that provides a default ordering of oldest to newest.
        /// </summary>
        /// <returns></returns>
        public string GetChronologicalBasedRowKey()
        {
            var now = DateTimeOffset.UtcNow;
            return $"{now.Ticks:D20}_{JsonConvert.SerializeObject(Guid.NewGuid())}";
        }

        /// <summary>
        ///     A Row key that can be used for an ordering of newest to oldest.
        /// </summary>
        /// <returns></returns>
        public string GetReverseChronologicalBasedRowKey()
        {
            return $"{DateTimeOffset.MaxValue.Ticks - DateTimeOffset.UtcNow.Ticks:D20}_{Guid.NewGuid()}";
        }

        private void LoadTableMetaData()
        {
            // Try to load the partition meta data from the existing table (which contains a list of the partition keys in the table).
            _partitionMetaDataEntity = _tableMetaDataContext.Find(CtConstants.TableMetaDataPartitionKey, CtConstants.PartitionSchemasRowKey);
            // Set the default PartitionKey using the combination below in case there are more than one CloudTableContext objects
            // on the same table.
            _defaultIndexDefinitionName = $"DefaultIndex_ofType_{typeof(TDomainEntity).Name}";
            if(_partitionMetaDataEntity != null)
            {
                /* This is going through and populating the local PartitionKeysInTable property with the list of keys retrieved
                 * from the Azure table.
                 * This also checks to see if there is a PartitionKey for the table meta data and the DefaultPartition
                 * and adds that if there isn't*/
                var metaDataPkIsInList = false;
                foreach(var partitionKeyString in _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys)
                {
                    if(partitionKeyString == CtConstants.TableMetaDataPartitionKey)
                    {
                        metaDataPkIsInList = true;
                    }
                    var isInList = false;
                    foreach(var item in PartitionKeysInTable)
                    {
                        if(item == partitionKeyString)
                        {
                            isInList = true;
                        }
                    }
                    if(!isInList)
                    {
                        PartitionKeysInTable.Add(partitionKeyString);
                    }
                }
                if(!metaDataPkIsInList)
                {
                    PartitionKeysInTable.Add(CtConstants.TableMetaDataPartitionKey);
                }
                // The RowKey for the DefaultSchema is set by the given ID property of the TDomainEntity object
                DefaultIndex = CreateIndexDefinition(_defaultIndexDefinitionName)
                    .DefineIndexCriteria(entity => true)
                    .SetIndexedPropertyCriteria(entity => entity.GetType().Name); // Enables searching directly on the type.
                if(IndexDefinitions.All(indexDefinition => indexDefinition.IndexNameKey != DefaultIndex.IndexNameKey))
                {
                    AddIndexDefinition(DefaultIndex);
                }
            }
            else
            {
                /* Creates a new partition meta data entity and adds the appropriate default partitions and metadata partitions*/
                _partitionMetaDataEntity = new CloudTableEntity<PartitionMetaData>(CtConstants.TableMetaDataPartitionKey,
                    CtConstants.PartitionSchemasRowKey);
                DefaultIndex = CreateIndexDefinition(_defaultIndexDefinitionName)
                    .DefineIndexCriteria(entity => true)
                    .SetIndexedPropertyCriteria(entity => entity.GetType().Name); // Enables searching directly on the type
                AddIndexDefinition(DefaultIndex);
            }
        }

        private void ValidateTableEntityAgainstIndexDefinitions(CloudTableEntity<TDomainEntity> tableEntity)
        {
            foreach(var indexDefinition in IndexDefinitions)
            {
                if (!indexDefinition.DomainObjectMatchesIndexCriteria(tableEntity.DomainObjectInstance))
                    continue;
                var tempTableEntity = new CloudTableEntity<TDomainEntity>(domainObject: tableEntity.DomainObjectInstance)
                {
                    PartitionKey = indexDefinition.IndexNameKey
                };
                // Checks if the current partition key has been registered with the list of partition keys for the table
                if(_partitionMetaDataEntity.DomainObjectInstance.PartitionKeys
                                           .All(schemaPartitionKey => schemaPartitionKey == tempTableEntity.PartitionKey))
                {
                    _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(tempTableEntity.PartitionKey);
                    SavePartitionKeys();
                }
                tempTableEntity.RowKey = indexDefinition.GetRowKeyFromCriteria(tempTableEntity.DomainObjectInstance);
                // Need to get the Object that is to be indexed and then wrap it in a reference object for proper JSV serialization.
                var indexedPropertyObject = indexDefinition.GetIndexedPropertyFromCriteria(tempTableEntity.DomainObjectInstance);
                tempTableEntity.IndexedProperty = new IndexedObject
                {
                    ValueBeingIndexed = indexedPropertyObject
                };
                indexDefinition.CloudTableEntities.Add(tempTableEntity);
            }
        }

        private async Task ValidateTableEntityAgainstIndexDefinitionsAsync(CloudTableEntity<TDomainEntity> tableEntity)
        {
            foreach(var partitionSchema in IndexDefinitions)
            {
                if(partitionSchema.DomainObjectMatchesIndexCriteria(tableEntity.DomainObjectInstance))
                {
                    var tempTableEntity = new CloudTableEntity<TDomainEntity>(domainObject: tableEntity.DomainObjectInstance)
                    {
                        PartitionKey = partitionSchema.IndexNameKey
                    };
                    // Checks if the current partition key has been registered with the list of partition keys for the table
                    if(_partitionMetaDataEntity.DomainObjectInstance.PartitionKeys
                                               .All(schemaPartitionKey => schemaPartitionKey == tempTableEntity.PartitionKey))
                    {
                        _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(tempTableEntity.PartitionKey);
                        await SavePartitionKeysAsync();
                    }
                    tempTableEntity.RowKey = partitionSchema.GetRowKeyFromCriteria(tempTableEntity.DomainObjectInstance);
                    // Need to get the Object that is to be indexed and then wrap it in a reference object for proper JSV serialization.
                    var indexedPropertyObject = partitionSchema.GetIndexedPropertyFromCriteria(tempTableEntity.DomainObjectInstance);
                    tempTableEntity.IndexedProperty = new IndexedObject
                    {
                        ValueBeingIndexed = indexedPropertyObject
                    };
                    partitionSchema.CloudTableEntities.Add(tempTableEntity);
                }
            }
        }

        private void SavePartitionKeys() { _tableMetaDataContext.InsertOrReplace(_partitionMetaDataEntity); }

        private async Task SavePartitionKeysAsync() { await _tableMetaDataContext.InsertOrReplaceAsync(_partitionMetaDataEntity); }

        private void ExecuteTableOperation(IEnumerable<TDomainEntity> domainEntities, SaveType batchOperation)
        {
            VerifyAllPartitionsExist();
            RunTableIndexing();
            foreach(var domainEntity in domainEntities)
            {
                var tempTableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
                ValidateTableEntityAgainstIndexDefinitions(tempTableEntity);
            }
            WritePartitionSchemasToTable(batchOperation);
        }

        private async Task ExecuteTableOperationAsync(IEnumerable<TDomainEntity> domainEntities, SaveType batchOperation)
        {
            await VerifyAllPartitionsExistAsync();
            await RunTableIndexingAsync();
            foreach(var tempTableEntity in domainEntities.Select(domainEntity => new CloudTableEntity<TDomainEntity>
            {
                DomainObjectInstance = domainEntity
            }))
            {
                await ValidateTableEntityAgainstIndexDefinitionsAsync(tempTableEntity);
            }
            WritePartitionSchemasToTable(batchOperation);
        }

        private void ExecuteTableOperation(TDomainEntity domainEntity, SaveType batchOperation)
        {
            VerifyAllPartitionsExist();
            RunTableIndexing();
            var tempTableEntity = new CloudTableEntity<TDomainEntity>
            {
                DomainObjectInstance = domainEntity
            };
            ValidateTableEntityAgainstIndexDefinitions(tempTableEntity);
            WritePartitionSchemasToTable(batchOperation);
        }

        private async Task ExecuteTableOperationAsync(TDomainEntity domainEntity, SaveType batchOperation)
        {
            await VerifyAllPartitionsExistAsync();
            await RunTableIndexingAsync();
            var tempTableEntity = new CloudTableEntity<TDomainEntity>
            {
                DomainObjectInstance = domainEntity
            };
            await ValidateTableEntityAgainstIndexDefinitionsAsync(tempTableEntity);
            await WritePartitionSchemasToTableAsync(batchOperation).ConfigureAwait(false);
        }

        private void VerifyAllPartitionsExist()
        {
            var shouldWriteToTable = false;
            // Check local list of Partition Schemas against the list of partition keys in _table Context
            IndexDefinitions.ForEach(schema =>
            {
                if(!_partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Contains(schema.IndexNameKey))
                {
                    _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(schema.IndexNameKey);
                    if(!PartitionKeysInTable.Contains(schema.IndexNameKey))
                    {
                        PartitionKeysInTable.Add(schema.IndexNameKey);
                    }
                    shouldWriteToTable = true;
                    _needToRunTableIndices = true;
                }
            });
            if(shouldWriteToTable)
            {
                _tableMetaDataContext.InsertOrReplace(_partitionMetaDataEntity);
            }
        }

        private async Task VerifyAllPartitionsExistAsync()
        {
            var shouldWriteToTable = false;
            // Check local list of Partition Schemas against the list of partition keys in _table Context
            IndexDefinitions.ForEach(schema =>
            {
                if(!_partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Contains(schema.IndexNameKey))
                {
                    _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(schema.IndexNameKey);
                    if(!PartitionKeysInTable.Contains(schema.IndexNameKey))
                    {
                        PartitionKeysInTable.Add(schema.IndexNameKey);
                    }
                    shouldWriteToTable = true;
                    _needToRunTableIndices = true;
                }
            });
            if(shouldWriteToTable)
            {
                await _tableMetaDataContext.InsertOrReplaceAsync(_partitionMetaDataEntity);
            }
        }

        private void RunTableIndexing()
        {
            if(!_needToRunTableIndices)
            {
                return;
            }
            var defaultPartitionEntities = GetAll().ToList();
            _needToRunTableIndices = false;
            if(defaultPartitionEntities.Count > 1)
            {
                Save(defaultPartitionEntities.ToArray(), SaveType.InsertOrReplace);
            }
        }

        private async Task RunTableIndexingAsync()
        {
            if(!_needToRunTableIndices)
            {
                return;
            }
            var defaultPartitionEntities = await GetAllAsync();
            _needToRunTableIndices = false;
            if(defaultPartitionEntities.Count > 1)
            {
                await SaveAsync(defaultPartitionEntities.ToArray(), SaveType.InsertOrReplace);
            }
        }

        private void WritePartitionSchemasToTable(SaveType batchOperation)
        {
            Parallel.ForEach(IndexDefinitions, schema =>
            {
                if(schema.CloudTableEntities.Count > 0)
                {
                    var entitiesArray = schema.CloudTableEntities.ToArray();
                    switch(batchOperation)
                    {
                        case SaveType.InsertOrReplace:
                            TableAccessContext.InsertOrReplace(entitiesArray);
                            break;
                        case SaveType.InsertOrMerge:
                            // Even if the client calls for a merge we need to replace since the whole object is being serialized anyways.
                            TableAccessContext.InsertOrReplace(entitiesArray);
                            break;
                        case SaveType.Insert:
                            TableAccessContext.Insert(entitiesArray);
                            break;
                        case SaveType.Replace:
                            TableAccessContext.Replace(entitiesArray);
                            break;
                        case SaveType.Delete:
                            TableAccessContext.Delete(entitiesArray);
                            break;
                    }
                }
                schema.CloudTableEntities.Clear();
            });
        }

        private async Task WritePartitionSchemasToTableAsync(SaveType batchOperation)
        {
            await Task.Run(() => Parallel.ForEach(IndexDefinitions, async schema =>
            {
                if(schema.CloudTableEntities.Count > 0)
                {
                    var entitiesArray = schema.CloudTableEntities.ToArray();
                    switch(batchOperation)
                    {
                        case SaveType.InsertOrReplace:
                            await TableAccessContext.InsertOrReplaceAsync(entitiesArray);
                            break;
                        case SaveType.InsertOrMerge:
                            // Even if the client calls for a merge we need to replace since the whole object is being serialized anyways.
                            await TableAccessContext.InsertOrReplaceAsync(entitiesArray);
                            break;
                        case SaveType.Insert:
                            await TableAccessContext.InsertAsync(entitiesArray);
                            break;
                        case SaveType.Replace:
                            await TableAccessContext.ReplaceAsync(entitiesArray);
                            break;
                        case SaveType.Delete:
                            await TableAccessContext.DeleteAsync(entitiesArray);
                            break;
                    }
                }
                schema.CloudTableEntities.Clear();
            }));
        }

        #region ---- Write Operations ----
        /// <summary>
        ///     Writes the domain entity to the Table based on the kind of Table Operation specified in the SaveType enum.
        /// </summary>
        /// <param name="domainEntity"></param>
        /// <param name="typeOfSave"></param>
        public void Save(TDomainEntity domainEntity, SaveType typeOfSave)
        {
            ExecuteTableOperation(domainEntity, typeOfSave);
        }

        /// <summary>
        ///     Writes the domain entities to their respective tables based on the kind of table operation specified by the
        ///     SaveType enum parameter.
        /// </summary>
        /// <param name="domainEntities"></param>
        /// <param name="typeOfSave"></param>
        public void Save(IEnumerable<TDomainEntity> domainEntities, SaveType typeOfSave)
        {
            ExecuteTableOperation(domainEntities, typeOfSave);
        }

        /// <summary>
        ///     Asynchronously writes the domain entity to the Table based on the kind of Table Operation specified in the SaveType
        ///     enum.
        /// </summary>
        /// <param name="domainEntity"></param>
        /// <param name="typeOfSave"></param>
        /// <returns></returns>
        public async Task SaveAsync(TDomainEntity domainEntity, SaveType typeOfSave)
        {
            await ExecuteTableOperationAsync(domainEntity, typeOfSave);
        }

        /// <summary>
        ///     Asynchronously writes the domain entities to their respective tables based on the kind of table operation specified
        ///     by the
        ///     SaveType enum parameter.
        /// </summary>
        /// <param name="domainEntities"></param>
        /// <param name="typeOfSave"></param>
        /// <returns></returns>
        public async Task SaveAsync(IEnumerable<TDomainEntity> domainEntities, SaveType typeOfSave)
        {
            await ExecuteTableOperationAsync(domainEntities, typeOfSave);
        }

        /// <summary>
        ///     Executes a single "InsertOrMerge" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        public void InsertOrMerge(TDomainEntity domainEntity)
        {
            ExecuteTableOperation(domainEntity, SaveType.InsertOrMerge);
        }

        /// <summary>
        ///     Executes a batch "InsertOrMerge" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        public void InsertOrMerge(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperation(domainEntities, SaveType.InsertOrMerge);
        }

        /// <summary>
        /// Runs an InsertOrMerge table operation on the given entities asynchronously
        /// </summary>
        /// <param name="domainEntities"></param>
        /// <returns></returns>
        public async Task InsertOrMergeAsync(TDomainEntity[] domainEntities)
        {
            await ExecuteTableOperationAsync(domainEntities, SaveType.InsertOrMerge).ConfigureAwait(false);
        }

        /// <summary>
        ///     Executes a single "InsertOrReplace" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        public void InsertOrReplace(TDomainEntity domainEntity)
        {
            ExecuteTableOperation(domainEntity, SaveType.InsertOrReplace);
        }

        /// <summary>
        ///     Executes batch "InsertOrReplace" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        public void InsertOrReplace(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperation(domainEntities, SaveType.InsertOrReplace);
        }

        /// <summary>
        /// Runs an InsertOrReplace table operation asynchrounously.
        /// </summary>
        /// <param name="domainEntities"></param>
        /// <returns></returns>
        public async Task InsertOrReplaceAsync(TDomainEntity[] domainEntities)
        {
            await ExecuteTableOperationAsync(domainEntities, SaveType.InsertOrReplace).ConfigureAwait(false);
        }

        /// <summary>
        ///     Executes a single "Insert" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        public void Insert(TDomainEntity domainEntity)
        {
            ExecuteTableOperation(domainEntity, SaveType.Insert);
        }

        /// <summary>
        ///     Executes a batch "Insert" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        public void Insert(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperation(domainEntities, SaveType.Insert);
        }

        /// <summary>
        ///     Executes a single "Delete" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        public void Delete(TDomainEntity domainEntity)
        {
            ExecuteTableOperation(domainEntity, SaveType.Delete);
        }

        /// <summary>
        ///     Executes a batch "Delete" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        public void Delete(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperation(domainEntities, SaveType.Delete);
        }

        /// <summary>
        /// Deletes entities in a table asynchronously
        /// </summary>
        /// <param name="domainEntities"></param>
        /// <returns></returns>
        public async Task DeleteAsync(TDomainEntity[] domainEntities)
        {
            await ExecuteTableOperationAsync(domainEntities, SaveType.Delete).ConfigureAwait(false);
        }

        /// <summary>
        ///     Executes a single "Replace" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        public void Replace(TDomainEntity domainEntity)
        {
            ExecuteTableOperation(domainEntity, SaveType.Replace);
        }

        /// <summary>
        ///     Executes a batch "Replace" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        public void Replace(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperation(domainEntities, SaveType.Replace);
        }
        #endregion ---- Write Operations ----

        #region ---- Read Operations ----
        /// <summary>
        ///     Gets all the entities via the Default Index.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetAll()
        {
            return TableAccessContext.GetByPartitionKey(_defaultIndexDefinitionName)
                                     .Select(cloudTableEntity => cloudTableEntity.DomainObjectInstance);
        }

        /// <summary>
        ///     Gets all the entities via the Default Index asynchronously.
        /// </summary>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetAllAsync()
        {
            var partition = await TableAccessContext.GetByPartitionKeyAsync(_defaultIndexDefinitionName);
            return partition.Select(cte => cte.DomainObjectInstance).ToList();
        }

        /// <summary>
        ///     Gets a domain entity using the partition partitionKey's PartitionKey (for the PartitionKey) and the entity's Id
        ///     (for the RowKey).
        ///     If the indexNameKey parameter is left null then the DefaultSchema is used.
        /// </summary>
        /// <param name="entityId"></param>
        /// <param name="indexNameKey"></param>
        /// <returns></returns>
        public TDomainEntity GetById(object entityId, string indexNameKey = "Default")
        {
            if(entityId == null)
            {
                throw new ArgumentNullException(nameof(entityId));
            }
            var serializedEntityId = JsonConvert.SerializeObject(entityId);
            if(string.IsNullOrEmpty(indexNameKey) || string.Equals(indexNameKey, "Default", StringComparison.CurrentCultureIgnoreCase))
            {
                indexNameKey = DefaultIndex.IndexNameKey;
            }
            var tableEntity = TableAccessContext.Find(indexNameKey, serializedEntityId);
            return tableEntity.DomainObjectInstance;
        }

        /// <summary>
        ///     Asynchronously gets a domain entity by the ID using the given entityId and based on the index defined by the given
        ///     partitionKey.
        ///     If the indexNameKey parameter is left null then the DefaultSchema is used.
        /// </summary>
        /// <param name="entityId"></param>
        /// <param name="indexNameKey"></param>
        /// <returns></returns>
        public async Task<TDomainEntity> GetByIdAsync(object entityId, string indexNameKey = null)
        {
            if(entityId == null)
            {
                return null;
            }
            var serializedEntityId = JsonConvert.SerializeObject(entityId);
            if(indexNameKey == null)
            {
                indexNameKey = DefaultIndex.IndexNameKey;
            }
            var tableEntity = await TableAccessContext.FindAsync(indexNameKey, serializedEntityId);
            return tableEntity.DomainObjectInstance;
        }

        /// <summary>
        ///     Retrieves all domain entities within a given Partition.
        /// </summary>
        /// <param name="indexKey">
        ///     If the object being passed in is not a string, it gets serialized to a JSON string and that string gets used as a PartitionKey (a.k.a IndexNameKey).
        /// </param>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetAllItemsFromIndex(object indexKey)
        {
            if (indexKey is string key)
            {
                return
                    TableAccessContext.GetByPartitionKey(key)
                                      .Select(tableEntity => tableEntity.DomainObjectInstance);
            }
            var serializedPartitionKey = JsonConvert.SerializeObject(indexKey);
            return
                TableAccessContext.GetByPartitionKey(serializedPartitionKey)
                                  .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        }

        /// <summary>
        ///     Asynchronously retrieves all domain entities within a given Partition.
        /// </summary>
        /// <param name="indexKey"></param>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetAllItemsFromIndexAsync(object indexKey)
        {
            if (indexKey is string key)
            {
                var entities = await TableAccessContext.GetByPartitionKeyAsync(key);
                return entities.Select(tableEntity => tableEntity.DomainObjectInstance).ToList();
            }
            var serializedPartitionKey = JsonConvert.SerializeObject(indexKey);
            var ents = await TableAccessContext.GetByPartitionKeyAsync(serializedPartitionKey);
            return ents.Select(azureTableEntity => azureTableEntity.DomainObjectInstance).ToList();
        }

        /// <summary>
        ///     Retrieves a set of domain entities based on a given PartitionScheme and an optional RowKey range.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetFromIndexWithinValueRange(string partitionKey, string minRowKey = "", string maxRowKey = "")
        {
            return
                TableAccessContext.GetByPartitionKeyWithRowKeyRange(partitionKey, minRowKey, maxRowKey)
                                  .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        }

        /// <summary>
        ///     Retrieves a set of domain entities based on a given PartitionScheme and an optional RowKey range.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetFromIndexWithinValueRangeAsync(string partitionKey, string minRowKey = "", string maxRowKey = "")
        {
            var entites = await TableAccessContext.GetByPartitionKeyWithRowKeyRangeAsync(partitionKey, minRowKey, maxRowKey);
            return entites.Select(ent => ent.DomainObjectInstance).ToList();
        }

        /// <summary>
        ///     Gets a set of domain entities based on a given ParitionSchema with a filter based on the indexProperty that gets passed in.
        /// </summary>
        /// <param name="indexNameKey"></param>
        /// <param name="indexedProperty"></param>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetByIndexedProperty(string indexNameKey, object indexedProperty)
        {
            var tempCloudTableEntity = new CloudTableEntity<TDomainEntity>
            {
                IndexedProperty =
                {
                    ValueBeingIndexed = indexedProperty
                }
            };
            var serializedIndexedProperty = JsonConvert.SerializeObject(tempCloudTableEntity.IndexedProperty);
            return TableAccessContext.QueryWherePropertyEquals(indexNameKey,
                CtConstants.PropNameIndexedProperty, serializedIndexedProperty).Select(cloudTableEntity => cloudTableEntity.DomainObjectInstance);
        }

        /// <summary>
        ///     Asynchronously gets a set of domain entities based on a given ParitionSchema with a filter based on the that gets passed in.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="indexedProperty"></param>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetByIndexedPropertyAsync(string partitionKey, object indexedProperty)
        {
            var tempCloudTableEntity = new CloudTableEntity<TDomainEntity>
            {
                IndexedProperty =
                {
                    ValueBeingIndexed = indexedProperty
                }
            };
            var serializedIndexedProperty = JsonConvert.SerializeObject(tempCloudTableEntity.IndexedProperty);
            var entities =
                await TableAccessContext.QueryWherePropertyEqualsAsync(partitionKey, CtConstants.PropNameIndexedProperty, serializedIndexedProperty);
            return entities.Select(cte => cte.DomainObjectInstance).ToList();
        }
        #endregion ---- Read Operations ----
    }
}