using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json;
using Nito.AsyncEx.Synchronous;

namespace Hallmanac.AzureCloudTable.API
{
    /// <summary>
    /// This is the primary class used in this library.
    /// <para>
    /// This class is used to provide high level interaction with Azure Table Storage. It allows for simply saving and retrieving 
    /// or creating simple indexes to allow for easier and faster searching than using the conventional methods of interacting
    /// with Azure table storage.
    /// </para>
    /// </summary>
    /// <typeparam name="TDomainEntity"></typeparam>
    public class TableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private readonly TableOperationsService<TableEntityWrapper<PartitionMetaData>> _tableMetaDataContext;
        private string _defaultIndexDefinitionName;
        private bool _needToRunTableIndices;
        private TableEntityWrapper<PartitionMetaData> _partitionMetaDataEntityWrapper;
        private readonly TableKeyEncoder _encoder = new TableKeyEncoder();


        /// <summary>
        /// Initializes a new CloudTableContext object. If the "tableName" parameter is left null, then 
        /// the default naming scheme used is the name of the generic type's name with "Table" appended 
        /// to it. For example "SomeClass" + "Table" for the table name of "SomeClassTable".
        /// </summary>
        public TableContext(CloudStorageAccount storageAccount, string nameOfEntityIdProperty, string tableName = null)
        {
            if (string.IsNullOrWhiteSpace(nameOfEntityIdProperty))
                throw new ArgumentNullException(nameof(nameOfEntityIdProperty));

            // Need to make sure that the table name is for the domain type and not the TableEntityWrapper type
            var tn = _encoder.CleanTableNameOfInvalidCharacters(string.IsNullOrWhiteSpace(tableName) ? $"{typeof(TDomainEntity).Name}Table" : tableName);
            NameOfEntityIdProperty = nameOfEntityIdProperty;
            TableOperationsService = new TableOperationsService<TableEntityWrapper<TDomainEntity>>(storageAccount, tn);
            _tableMetaDataContext = new TableOperationsService<TableEntityWrapper<PartitionMetaData>>(storageAccount, TableOperationsService.TableName);
            LoadTableMetaData();
        }


        /// <summary>
        /// Initializes a new CloudTableContext object. If the "tableName" parameter is left null, then 
        /// the default naming scheme used is the name of the generic type's name with "Table" appended 
        /// to it. For example "SomeClass" + "Table" for the table name of "SomeClassTable".
        /// </summary>
        public TableContext(string connectionString, string nameOfEntityIdProperty, string tableName = null)
        {
            if (string.IsNullOrWhiteSpace(nameOfEntityIdProperty))
                throw new ArgumentNullException(nameof(nameOfEntityIdProperty));

            // Need to make sure that the table name is for the domain type and not the TableEntityWrapper type
            var tn = _encoder.CleanTableNameOfInvalidCharacters(string.IsNullOrWhiteSpace(tableName) ? $"{typeof(TDomainEntity).Name}Table" : tableName);
            NameOfEntityIdProperty = nameOfEntityIdProperty;
            TableOperationsService = new TableOperationsService<TableEntityWrapper<TDomainEntity>>(connectionString, tn);
            _tableMetaDataContext = new TableOperationsService<TableEntityWrapper<PartitionMetaData>>(connectionString, TableOperationsService.TableName);
            LoadTableMetaData();
        }


        /// <summary>
        /// Gives direct access to the underlying TableOperationsService class that does the interaction with the Azure Table.
        /// </summary>
        public TableOperationsService<TableEntityWrapper<TDomainEntity>> TableOperationsService { get; }

        /// <summary>
        /// Gets a list of the index name keys that are used in the table.
        /// </summary>
        public List<string> IndexNameKeysInTable { get; } = new List<string>();

        /// <summary>
        /// Runtime list of active partition schemas.
        /// </summary>
        public List<TableIndexDefinition<TDomainEntity>> IndexDefinitions { get; set; } = new List<TableIndexDefinition<TDomainEntity>>();

        /// <summary>
        /// This is the name of the property that is used to store the ID of the Domain Entity.
        /// <para>
        /// For example, if there is a domain entity of type User that has a property named "Id" then one would pass
        /// the name of that property ("Id") into the constructor of the CloudTableContext class.
        /// </para>
        /// <para>This could be done using the extension method (on Object) called "GetPropertyName"</para>
        /// </summary>
        public string NameOfEntityIdProperty { get; }

        /// <summary>
        /// Gets the default index definition used for the table.
        /// </summary>
        public TableIndexDefinition<TDomainEntity> DefaultIndex { get; private set; }


        /// <summary>
        /// Returns a TableOperationsService class which allows for more options in constructing custom queries against the table.
        /// </summary>
        /// <returns></returns>
        public TableQuery<TableEntityWrapper<TDomainEntity>> TableQuery()
        {
            return TableOperationsService.Query();
        }


        /// <summary>
        /// Creates a new index definition for the {TDomainEntity} based on the given "indexName".
        /// The index definition's indexed value will be set based on the ID property of the "TDomainEntity".
        /// </summary>
        /// <param name="indexName"></param>
        /// <returns></returns>
        public TableIndexDefinition<TDomainEntity> CreateIndexDefinition(string indexName)
        {
            var schema = new TableIndexDefinition<TDomainEntity>(NameOfEntityIdProperty)
                .SetIndexNameKey(indexName);
            return schema;
        }


        /// <summary>
        /// Creates a new Index Definition object for the {TDomainEntity} with the index name key being set based on
        /// the name of the type by default. The index definition's indexed value will be set based on the ID property of the {TDomainEntity}.
        /// </summary>
        /// <returns></returns>
        public TableIndexDefinition<TDomainEntity> CreateIndexDefinition()
        {
            return CreateIndexDefinition(typeof(TDomainEntity).Name);
        }


        /// <summary>
        /// Adds multiple Index Definitions types to the current <see cref="TableContext{TDomainEntity}"/>.
        /// </summary>
        /// <param name="indexDefinitions"></param>
        public void AddMultipleIndexDefinitions(List<TableIndexDefinition<TDomainEntity>> indexDefinitions)
        {
            foreach (var indexDefinition in indexDefinitions)
            {
                if (IndexDefinitions.Any(indexDef => indexDef.IndexNameKey == indexDefinition.IndexNameKey))
                {
                    continue;
                }
                IndexDefinitions.Add(indexDefinition);
            }
        }


        /// <summary>
        /// Adds a single Index Definition to the current <see cref="TableContext{TDomainEntity}"/>.
        /// </summary>
        /// <param name="tableIndexDefinition"></param>
        public void AddIndexDefinition(TableIndexDefinition<TDomainEntity> tableIndexDefinition)
        {
            if (IndexDefinitions.Any(indexDef => indexDef.IndexNameKey == tableIndexDefinition.IndexNameKey))
            {
                return;
            }
            IndexDefinitions.Add(tableIndexDefinition);
        }


        /// <summary>
        /// A string for a row key that provides a default ordering of oldest to newest.
        /// </summary>
        /// <returns></returns>
        public string GetChronologicalBasedRowKey()
        {
            var now = DateTimeOffset.UtcNow;
            return $"{now.Ticks:D20}_{JsonConvert.SerializeObject(Guid.NewGuid())}";
        }


        /// <summary>
        /// A Row key that can be used for an ordering of newest to oldest.
        /// </summary>
        /// <returns></returns>
        public string GetReverseChronologicalBasedRowKey()
        {
            return $"{DateTimeOffset.MaxValue.Ticks - DateTimeOffset.UtcNow.Ticks:D20}_{Guid.NewGuid()}";
        }


        private void LoadTableMetaData()
        {
            // Try to load the partition meta data from the existing table (which contains a list of the partition keys in the table).
            _partitionMetaDataEntityWrapper = _tableMetaDataContext.FindAsync(CtConstants.TableMetaDataPartitionKey, CtConstants.PartitionSchemasRowKey).Result;

            // Set the default PartitionKey using the combination below in case there are more than one CloudTableContext objects
            // on the same table.
            _defaultIndexDefinitionName = $"DefaultIndex_ofType_{typeof(TDomainEntity).Name}";
            if (_partitionMetaDataEntityWrapper != null)
            {
                /* This is going through and populating the local PartitionKeysInTable property with the list of keys retrieved
                 * from the Azure table.
                 * This also checks to see if there is a PartitionKey for the table meta data and the DefaultPartition
                 * and adds that if there isn't*/
                var metaDataPkIsInList = false;
                foreach (var partitionKeyString in _partitionMetaDataEntityWrapper.DomainObjectInstance.PartitionKeys)
                {
                    if (partitionKeyString == CtConstants.TableMetaDataPartitionKey)
                    {
                        metaDataPkIsInList = true;
                    }
                    var isInList = false;
                    foreach (var item in IndexNameKeysInTable)
                    {
                        if (item == partitionKeyString)
                        {
                            isInList = true;
                        }
                    }
                    if (!isInList)
                    {
                        IndexNameKeysInTable.Add(partitionKeyString);
                    }
                }
                if (!metaDataPkIsInList)
                {
                    IndexNameKeysInTable.Add(CtConstants.TableMetaDataPartitionKey);
                }

                // The RowKey for the DefaultSchema is set by the given ID property of the TDomainEntity object
                DefaultIndex = CreateIndexDefinition(_defaultIndexDefinitionName)
                    .DefineIndexCriteria(entity => true)
                    .SetIndexedPropertyCriteria(entity => entity.GetType().Name); // Enables searching directly on the type.
                if (IndexDefinitions.All(indexDefinition => indexDefinition.IndexNameKey != DefaultIndex.IndexNameKey))
                {
                    AddIndexDefinition(DefaultIndex);
                }
            }
            else
            {
                /* Creates a new partition meta data entity and adds the appropriate default partitions and metadata partitions*/
                _partitionMetaDataEntityWrapper = new TableEntityWrapper<PartitionMetaData>(CtConstants.TableMetaDataPartitionKey, CtConstants.PartitionSchemasRowKey);
                DefaultIndex = CreateIndexDefinition(_defaultIndexDefinitionName)
                    .DefineIndexCriteria(entity => true)
                    .SetIndexedPropertyCriteria(entity => entity.GetType().Name); // Enables searching directly on the type
                AddIndexDefinition(DefaultIndex);
            }
        }


        private async Task ValidateTableEntityAgainstIndexDefinitionsAsync(TableEntityWrapper<TDomainEntity> tableEntityWrapper)
        {
            foreach (var partitionSchema in IndexDefinitions)
            {
                if (!partitionSchema.DomainObjectMatchesIndexCriteria(tableEntityWrapper.DomainObjectInstance))
                    continue;
                var tempTableEntity = new TableEntityWrapper<TDomainEntity>(domainObject: tableEntityWrapper.DomainObjectInstance)
                {
                    PartitionKey = partitionSchema.IndexNameKey
                };

                // Checks if the current partition key has been registered with the list of partition keys for the table
                if (_partitionMetaDataEntityWrapper.DomainObjectInstance.PartitionKeys
                                            .All(schemaPartitionKey => schemaPartitionKey == tempTableEntity.PartitionKey))
                {
                    _partitionMetaDataEntityWrapper.DomainObjectInstance.PartitionKeys.Add(tempTableEntity.PartitionKey);
                    await SaveIndexNameKeysAsync();
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


        private async Task SaveIndexNameKeysAsync()
        {
            await _tableMetaDataContext.InsertOrReplaceAsync(_partitionMetaDataEntityWrapper);
        }


        private async Task ExecuteTableOperationAsync(IEnumerable<TDomainEntity> domainEntities, SaveType batchOperation)
        {
            await VerifyAllPartitionsExistAsync().ConfigureAwait(false);
            await RunTableIndexingAsync().ConfigureAwait(false);
            foreach (var tempTableEntity in domainEntities.Select(domainEntity => new TableEntityWrapper<TDomainEntity>
            {
                DomainObjectInstance = domainEntity
            }))
            {
                await ValidateTableEntityAgainstIndexDefinitionsAsync(tempTableEntity);
            }
            await WriteIndexDefinitionsToTableAsync(batchOperation).ConfigureAwait(false);
        }


        private async Task ExecuteTableOperationAsync(TDomainEntity domainEntity, SaveType batchOperation)
        {
            await VerifyAllPartitionsExistAsync();
            await RunTableIndexingAsync();
            var tempTableEntity = new TableEntityWrapper<TDomainEntity>
            {
                DomainObjectInstance = domainEntity
            };
            await ValidateTableEntityAgainstIndexDefinitionsAsync(tempTableEntity);
            await WriteIndexDefinitionsToTableAsync(batchOperation).ConfigureAwait(false);
        }


        private async Task VerifyAllPartitionsExistAsync()
        {
            var shouldWriteToTable = false;

            // Check local list of Partition Schemas against the list of partition keys in _table Context
            for (var i = 0; i < IndexDefinitions.Count; i++)
            {
                var schema = IndexDefinitions[i];
                if (_partitionMetaDataEntityWrapper.DomainObjectInstance.PartitionKeys.Contains(schema.IndexNameKey))
                    continue;
                _partitionMetaDataEntityWrapper.DomainObjectInstance.PartitionKeys.Add(schema.IndexNameKey);
                if (!IndexNameKeysInTable.Contains(schema.IndexNameKey))
                {
                    IndexNameKeysInTable.Add(schema.IndexNameKey);
                }
                shouldWriteToTable = true;
                _needToRunTableIndices = true;
            }
            if (shouldWriteToTable)
            {
                await _tableMetaDataContext.InsertOrReplaceAsync(_partitionMetaDataEntityWrapper);
            }
        }


        private async Task RunTableIndexingAsync()
        {
            if (!_needToRunTableIndices)
            {
                return;
            }
            var defaultPartitionEntities = await GetAllAsync();
            _needToRunTableIndices = false;
            if (defaultPartitionEntities.Count > 1)
            {
                await SaveAsync(defaultPartitionEntities.ToArray(), SaveType.InsertOrReplace);
            }
        }


        private async Task WriteIndexDefinitionsToTableAsync(SaveType batchOperation)
        {
            for (var i = 0; i < IndexDefinitions.Count; i++)
            {
                var indexDefinition = IndexDefinitions[i];
                if (indexDefinition.CloudTableEntities.Count > 0)
                {
                    var entitiesArray = indexDefinition.CloudTableEntities.ToArray();
                    switch (batchOperation)
                    {
                        case SaveType.InsertOrReplace:
                            await TableOperationsService.InsertOrReplaceAsync(entitiesArray);
                            break;
                        case SaveType.InsertOrMerge:
                            // Even if the client calls for a merge we need to replace since the whole object is being serialized anyways.
                            await TableOperationsService.InsertOrReplaceAsync(entitiesArray);
                            break;
                        case SaveType.Insert:
                            await TableOperationsService.InsertAsync(entitiesArray);
                            break;
                        case SaveType.Replace:
                            await TableOperationsService.ReplaceAsync(entitiesArray);
                            break;
                        case SaveType.Delete:
                            await TableOperationsService.DeleteAsync(entitiesArray);
                            break;
                    }
                }
                indexDefinition.CloudTableEntities.Clear();
            }
        }


        #region ---- Write Operations ----

        /// <summary>
        ///  Writes the domain entity to the Table based on the kind of Table Operation specified in the SaveType enum.
        /// </summary>
        /// <param name="domainEntity"></param>
        /// <param name="typeOfSave"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Save(TDomainEntity domainEntity, SaveType typeOfSave)
        {
            ExecuteTableOperationAsync(domainEntity, typeOfSave).WaitAndUnwrapException();
        }


        /// <summary>
        /// Writes the domain entities to their respective tables based on the kind of table operation specified by the
        /// SaveType enum parameter.
        /// </summary>
        /// <param name="domainEntities"></param>
        /// <param name="typeOfSave"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Save(IEnumerable<TDomainEntity> domainEntities, SaveType typeOfSave)
        {
            ExecuteTableOperationAsync(domainEntities, typeOfSave).WaitAndUnwrapException();
        }


        /// <summary>
        /// Asynchronously writes the domain entity to the Table based on the kind of Table Operation specified in the SaveType enum.
        /// </summary>
        /// <param name="domainEntity"></param>
        /// <param name="typeOfSave"></param>
        /// <returns></returns>
        public async Task SaveAsync(TDomainEntity domainEntity, SaveType typeOfSave)
        {
            await ExecuteTableOperationAsync(domainEntity, typeOfSave).ConfigureAwait(false);
        }


        /// <summary>
        /// Asynchronously writes the domain entities to their respective tables based on the kind of table operation specified by the
        /// SaveType enum parameter.
        /// </summary>
        /// <param name="domainEntities"></param>
        /// <param name="typeOfSave"></param>
        /// <returns></returns>
        public async Task SaveAsync(IEnumerable<TDomainEntity> domainEntities, SaveType typeOfSave)
        {
            await ExecuteTableOperationAsync(domainEntities, typeOfSave).ConfigureAwait(false);
        }


        /// <summary>
        /// Executes a single "InsertOrMerge" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void InsertOrMerge(TDomainEntity domainEntity)
        {
            ExecuteTableOperationAsync(domainEntity, SaveType.InsertOrMerge).WaitAndUnwrapException();
        }


        /// <summary>
        /// Executes a batch "InsertOrMerge" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void InsertOrMerge(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperationAsync(domainEntities, SaveType.InsertOrMerge).WaitAndUnwrapException();
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
        /// Executes a single "InsertOrReplace" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void InsertOrReplace(TDomainEntity domainEntity)
        {
            ExecuteTableOperationAsync(domainEntity, SaveType.InsertOrReplace).WaitAndUnwrapException();
        }


        /// <summary>
        /// Executes batch "InsertOrReplace" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void InsertOrReplace(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperationAsync(domainEntities, SaveType.InsertOrReplace).WaitAndUnwrapException();
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
        /// Executes a single "Insert" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Insert(TDomainEntity domainEntity)
        {
            ExecuteTableOperationAsync(domainEntity, SaveType.Insert).WaitAndUnwrapException();
        }


        /// <summary>
        /// Executes a batch "Insert" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Insert(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperationAsync(domainEntities, SaveType.Insert).WaitAndUnwrapException();
        }


        /// <summary>
        /// Executes a single "Delete" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Delete(TDomainEntity domainEntity)
        {
            ExecuteTableOperationAsync(domainEntity, SaveType.Delete).WaitAndUnwrapException();
        }


        /// <summary>
        /// Executes a batch "Delete" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Delete(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperationAsync(domainEntities, SaveType.Delete).WaitAndUnwrapException();
        }


        /// <summary>
        /// Deletes entities in a table asynchronously
        /// </summary>
        /// <param name="domainEntities"></param>
        public async Task DeleteAsync(TDomainEntity[] domainEntities)
        {
            await ExecuteTableOperationAsync(domainEntities, SaveType.Delete).ConfigureAwait(false);
        }


        /// <summary>
        /// Executes a single "Replace" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Replace(TDomainEntity domainEntity)
        {
            ExecuteTableOperationAsync(domainEntity, SaveType.Replace).WaitAndUnwrapException();
        }


        /// <summary>
        /// Executes a batch "Replace" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Replace(TDomainEntity[] domainEntities)
        {
            ExecuteTableOperationAsync(domainEntities, SaveType.Replace).WaitAndUnwrapException();
        }

        #endregion ---- Write Operations ----


        #region ---- Read Operations ----

        /// <summary>
        /// Gets all the entities via the <see cref="DefaultIndex"/> asynchronously. This is usually the index that retrieves items by ID so all 
        /// entities should be unique by default
        /// </summary>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TDomainEntity> GetAll()
        {
            return GetAllAsync().Result;
        }


        /// <summary>
        /// Gets all the entities via the <see cref="DefaultIndex"/> asynchronously. This is usually the index that retrieves items by ID so all 
        /// entities should be unique by default
        /// </summary>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetAllAsync()
        {
            var partition = await TableOperationsService.GetByPartitionKeyAsync(_defaultIndexDefinitionName).ConfigureAwait(false);
            return partition.Select(cte => cte.DomainObjectInstance).ToList();
        }


        /// <summary>
        /// Asynchronously gets a domain entity by the ID using the given entityId and based on the index defined by the given indexNameKey.
        /// If the indexNameKey parameter is left null then the <see cref="DefaultIndex"/> is used.
        /// </summary>
        /// <param name="entityId">Value of the ID property to be used in finding by the ID. This object will get serialized to JSON before being used in the query</param>
        /// <param name="indexNameKey">Optional name of the index used when searching for items by ID. The <see cref="DefaultIndex"/> is usually the one that holds the ID index</param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public TDomainEntity GetById(object entityId, string indexNameKey = "Default")
        {
            return GetByIdAsync(entityId, indexNameKey).Result;
        }


        /// <summary>
        /// Asynchronously gets a domain entity by the ID using the given entityId and based on the index defined by the given indexNameKey.
        /// If the indexNameKey parameter is left null then the <see cref="DefaultIndex"/> is used.
        /// </summary>
        /// <param name="entityId">Value of the ID property to be used in finding by the ID. This object will get serialized to JSON before being used in the query</param>
        /// <param name="indexNameKey">Optional name of the index used when searching for items by ID. The <see cref="DefaultIndex"/> is usually the one that holds the ID index</param>
        /// <returns></returns>
        public async Task<TDomainEntity> GetByIdAsync(object entityId, string indexNameKey = null)
        {
            if (entityId == null)
            {
                return null;
            }
            var serializedEntityId = JsonConvert.SerializeObject(entityId);
            if (indexNameKey == null)
            {
                indexNameKey = DefaultIndex.IndexNameKey;
            }
            var tableEntity = await TableOperationsService.FindAsync(indexNameKey, serializedEntityId).ConfigureAwait(false);
            return tableEntity.DomainObjectInstance;
        }


        /// <summary>
        /// Asynchronously retrieves all domain entities within a given Index.
        /// </summary>
        /// <param name="indexKey">Key to be used as the index. If it's not a string it will be serialized to JSON first and then used as the index name key (a.k.a. Partition Key)</param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TDomainEntity> GetAllItemsFromIndex(object indexKey)
        {
            return GetAllItemsFromIndexAsync(indexKey).Result;
        }


        /// <summary>
        /// Asynchronously retrieves all domain entities within a given Index.
        /// </summary>
        /// <param name="indexKey">Key to be used as the index. If it's not a string it will be serialized to JSON first and then used as the index name key (a.k.a. Partition Key)</param>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetAllItemsFromIndexAsync(object indexKey)
        {
            if (indexKey is string key)
            {
                var entities = await TableOperationsService.GetByPartitionKeyAsync(key).ConfigureAwait(false);
                return entities.Select(tableEntity => tableEntity.DomainObjectInstance).ToList();
            }
            var serializedPartitionKey = JsonConvert.SerializeObject(indexKey);
            var ents = await TableOperationsService.GetByPartitionKeyAsync(serializedPartitionKey).ConfigureAwait(false);
            return ents.Select(azureTableEntity => azureTableEntity.DomainObjectInstance).ToList();
        }


        /// <summary>
        /// Retrieves a List of domain <see cref="TableEntityWrapper{TDomainObject}"/> based on a given Index Name (Azure table Partition Key) 
        /// and an optional RowKey range.
        /// </summary>
        /// <param name="indexNameKey">Name of index. Ultimately this is the Partition Key inside Azure Table Storage so if you wanted to get all indexed values
        /// inside a given index then you could use this method without the last two parameters</param>
        /// <param name="minIndexedValue">Optional minimum value of the index</param>
        /// <param name="maxIndexedValue">Optional maximum value of the index</param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TDomainEntity> GetFromIndexWithinValueRange(string indexNameKey, string minIndexedValue = "", string maxIndexedValue = "")
        {
            return GetFromIndexWithinValueRangeAsync(indexNameKey, minIndexedValue, maxIndexedValue).Result;
        }


        /// <summary>
        /// Retrieves a List of domain <see cref="TableEntityWrapper{TDomainObject}"/> based on a given Index Name (Azure table Partition Key) 
        /// and an optional RowKey range.
        /// </summary>
        /// <param name="indexNameKey">Name of index. Ultimately this is the Partition Key inside Azure Table Storage so if you wanted to get all indexed values
        /// inside a given index then you could use this method without the last two parameters</param>
        /// <param name="minIndexedValue">Optional minimum value of the index</param>
        /// <param name="maxIndexedValue">Optional maximum value of the index</param>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetFromIndexWithinValueRangeAsync(string indexNameKey, string minIndexedValue = "", string maxIndexedValue = "")
        {
            var entites = await TableOperationsService.GetByPartitionKeyWithRowKeyRangeAsync(indexNameKey, minIndexedValue, maxIndexedValue).ConfigureAwait(false);
            return entites.Select(ent => ent.DomainObjectInstance).ToList();
        }


        /// <summary>
        /// Gets a set of domain entities based on a given indexNameKey with a filter based on the indexProperty that gets passed in.
        /// </summary>
        /// <param name="indexNameKey">Name of the index</param>
        /// <param name="indexedProperty">Value to be searching for inside the index</param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TDomainEntity> GetByIndexedProperty(string indexNameKey, object indexedProperty)
        {
            return GetByIndexedPropertyAsync(indexNameKey, indexedProperty).Result;
        }


        /// <summary>
        /// Asynchronously gets a set of domain entities based on a given index definition with a filter based on the value object (indexedProperty) that gets passed in.
        /// </summary>
        /// <param name="indexDefinitionName">Index definition name</param>
        /// <param name="indexedProperty"></param>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetByIndexedPropertyAsync(string indexDefinitionName, object indexedProperty)
        {
            var tempCloudTableEntity = new TableEntityWrapper<TDomainEntity>
            {
                IndexedProperty =
                {
                    ValueBeingIndexed = indexedProperty
                }
            };
            var serializedIndexedProperty = JsonConvert.SerializeObject(tempCloudTableEntity.IndexedProperty);
            var entities = await TableOperationsService.QueryWherePropertyEqualsAsync(indexDefinitionName, CtConstants.PropNameIndexedProperty, serializedIndexedProperty).ConfigureAwait(false);
            return entities.Select(cte => cte.DomainObjectInstance).ToList();
        }

        #endregion ---- Read Operations ----
    }
}