namespace AzureCloudTableContext.Api
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using ServiceStack.Text;

    /// <summary>
    ///     Class used to wrap a domain entity for use with Azure Table Storage via using PartitionKey strategies (known as
    ///     PartitionSchemas)
    ///     for grouping and filtering.
    /// </summary>
    /// <typeparam name="TDomainEntity"></typeparam>
    public class CloudTableContext<TDomainEntity> where TDomainEntity : class, new()
    {
        private string _defaultSchemaName;
        private bool _needToRunTableIndices;
        private CloudTableEntity<PartitionMetaData> _partitionMetaDataEntity;
        private CloudTable _table;

        private TableAccessContext<CloudTableEntity<PartitionMetaData>> _tableMetaDataContext;

        /// <summary>
        ///     Initializes a new CloudTableContext object. If the
        ///     <param name="tableName"></param>
        ///     parameter is left null, then the default
        ///     naming scheme used is the name of the generic type's name with "Table" appended to it. For example "SomeClass" +
        ///     "Table" for
        ///     the table name of "SomeClassTable".
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="nameOfEntityIdProperty"></param>
        /// <param name="tableName"></param>
        public CloudTableContext(CloudStorageAccount storageAccount, string nameOfEntityIdProperty, string tableName = null)
        {
            tableName = string.IsNullOrWhiteSpace(tableName) ? string.Format("{0}Table", typeof(TDomainEntity).Name) : tableName;
            Init(storageAccount, nameOfEntityIdProperty, tableName);
        }

        public TableAccessContext<CloudTableEntity<TDomainEntity>> TableAccessContext { get; private set; }

        /// <summary>
        ///     Gets a list of the PartitionKeys that are used in the table.
        /// </summary>
        public List<string> PartitionKeysInTable { get; private set; }

        /// <summary>
        ///     Runtime list of active partition schemas.
        /// </summary>
        public List<PartitionSchema<TDomainEntity>> PartitionSchemas { get; set; }

        /// <summary>
        ///     This is the name of the property that is used to store the ID of the Domain Entity.
        ///     <para>
        ///         For example, if there is a domain entity of type User that has a property named "Id" then one would pass
        ///         the name of that property ("Id") into the constructor of the CloudTableContext class.
        ///     </para>
        ///     <para>This could be done using the extension method (on Object) called "GetPropertyName"</para>
        /// </summary>
        public string NameOfEntityIdProperty { get; set; }

        /// <summary>
        ///     Gets the default partition partitionKey used for the table.
        /// </summary>
        public PartitionSchema<TDomainEntity> DefaultSchema { get; private set; }

        /// <summary>
        ///     Returns a TableAccessContext class which allows for more options in constructing custom queries against the table.
        /// </summary>
        /// <returns></returns>
        public TableQuery<CloudTableEntity<TDomainEntity>> TableQuery()
        {
            return TableAccessContext.Query();
        }

        /// <summary>
        ///     Creates a new PartitionSchema for the <see cref="TDomainEntity" /> based on the given
        ///     <param name="partitionKey"></param>
        ///     .
        ///     The PartitionSchema RowKey will be set based on the ID property of the <see cref="TDomainEntity" />.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public PartitionSchema<TDomainEntity> CreatePartitionSchema(string partitionKey)
        {
            var schema = new PartitionSchema<TDomainEntity>(NameOfEntityIdProperty)
                .SetPartitionKey(partitionKey);
            return schema;
        }

        /// <summary>
        ///     Creates a new PartitionSchema object for the <see cref="TDomainEntity" /> with the PartitionKey being set based on
        ///     the name of the type by default. The PartitionSchema RowKey will be set based on the ID property of the
        ///     <see cref="TDomainEntity" />.
        /// </summary>
        /// <returns></returns>
        public PartitionSchema<TDomainEntity> CreatePartitionSchema()
        {
            return CreatePartitionSchema(typeof(TDomainEntity).Name);
        }

        /// <summary>
        ///     Adds multiple PartitionSchema types to the current CloudTableContext.
        /// </summary>
        /// <param name="partitionSchemas"></param>
        public void AddMultiplePartitionSchemas(List<PartitionSchema<TDomainEntity>> partitionSchemas)
        {
            foreach(var partitionSchema in partitionSchemas)
            {
                if(PartitionSchemas.Any(schema => schema.PartitionKey == partitionSchema.PartitionKey)) continue;
                PartitionSchemas.Add(partitionSchema);
            }
        }

        /// <summary>
        ///     Adds a single PartitionSchema to the current CloudTableContext.
        /// </summary>
        /// <param name="partitionSchema"></param>
        public void AddPartitionSchema(PartitionSchema<TDomainEntity> partitionSchema)
        {
            if(PartitionSchemas.Any(schema => schema.PartitionKey == partitionSchema.PartitionKey)) return;
            PartitionSchemas.Add(partitionSchema);
        }

        /// <summary>
        ///     A string for a row key that provides a default ordering of oldest to newest.
        /// </summary>
        /// <returns></returns>
        public string GetChronologicalBasedRowKey()
        {
            return string.Format("{0:D20}_{1}", (DateTimeOffset.Now.Ticks), Guid.NewGuid().ToJsv());
        }

        /// <summary>
        ///     A Row key that can be used for an ordering of newest to oldest.
        /// </summary>
        /// <returns></returns>
        public string GetReverseChronologicalBasedRowKey()
        {
            return string.Format("{0:D20}_{1}", (DateTimeOffset.MaxValue.Ticks - DateTimeOffset.Now.Ticks),
                Guid.NewGuid());
        }

        private void Init(CloudStorageAccount storageAccount, string propertyNameOfEntityId, string tableName)
        {
            PartitionKeysInTable = new List<string>();
            NameOfEntityIdProperty = propertyNameOfEntityId;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
            PartitionSchemas = new List<PartitionSchema<TDomainEntity>>();
            _tableMetaDataContext = new TableAccessContext<CloudTableEntity<PartitionMetaData>>(storageAccount,
                tableName);
            LoadTableMetaData();
            TableAccessContext = new TableAccessContext<CloudTableEntity<TDomainEntity>>(storageAccount,
                tableName);
        }

        private void LoadTableMetaData()
        {
            // Try to load the partition meta data from the existing table (which contains a list of the partition keys in the table).
            _partitionMetaDataEntity = _tableMetaDataContext.Find(CtConstants.TableMetaDataPartitionKey,
                CtConstants.PartitionSchemasRowKey);
            // Set the default PartitionKey using the combination below in case there are more than one CloudTableContext objects
            // on the same table.
            _defaultSchemaName = string.Format("DefaultPartition_ofType_{0}", typeof(TDomainEntity).Name);
            if(_partitionMetaDataEntity != null)
            {
                /* This is going through and populating the local PartitionKeysInTable property with the list of keys retrieved
                 * from the Azure table.
                 * This also checks to see if there is a PartitionKey for the table meta data and the DefaultPartition
                 * and adds that if there isn't*/
                var metaDataPkIsInList = false;
                foreach(var partitionKeyString in _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys)
                {
                    if(partitionKeyString == CtConstants.TableMetaDataPartitionKey) metaDataPkIsInList = true;
                    var isInList = false;
                    foreach(var item in PartitionKeysInTable)
                    {
                        if(item == partitionKeyString) isInList = true;
                    }
                    if(!isInList) PartitionKeysInTable.Add(partitionKeyString);
                }
                if(!metaDataPkIsInList) PartitionKeysInTable.Add(CtConstants.TableMetaDataPartitionKey);
                // The RowKey for the DefaultSchema is set by the given ID property of the TDomainEntity object
                DefaultSchema = CreatePartitionSchema(_defaultSchemaName)
                    .SetSchemaCriteria(entity => true)
                    .SetIndexedPropertyCriteria(entity => entity.GetType().Name); // Enables searching directly on the type.
                if(PartitionSchemas.All(schema => schema.PartitionKey != DefaultSchema.PartitionKey))
                {
                    AddPartitionSchema(DefaultSchema);
                }
            }
            else
            {
                /* Creates a new partition meta data entity and adds the appropriate default partitions and metadata partitions*/
                _partitionMetaDataEntity = new CloudTableEntity<PartitionMetaData>(CtConstants.TableMetaDataPartitionKey,
                    CtConstants.PartitionSchemasRowKey);
                DefaultSchema = CreatePartitionSchema(_defaultSchemaName)
                    .SetSchemaCriteria(entity => true)
                    .SetIndexedPropertyCriteria(entity => entity.GetType().Name); // Enables searching directly on the type
                AddPartitionSchema(DefaultSchema);
            }
        }

        private void ValidateTableEntityAgainstPartitionSchemas(CloudTableEntity<TDomainEntity> tableEntity)
        {
            foreach(var partitionSchema in PartitionSchemas)
            {
                if(partitionSchema.DomainObjectMatchesPartitionCriteria(tableEntity.DomainObjectInstance))
                {
                    var tempTableEntity = new CloudTableEntity<TDomainEntity>(domainObject: tableEntity.DomainObjectInstance);
                    tempTableEntity.PartitionKey = partitionSchema.PartitionKey;
                    // Checks if the current partition key has been registered with the list of partition keys for the table
                    if(_partitionMetaDataEntity.DomainObjectInstance.PartitionKeys
                                               .All(schemaPartitionKey => schemaPartitionKey == tempTableEntity.PartitionKey))
                    {
                        _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(tempTableEntity.PartitionKey);
                        SavePartitionKeys();
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

        private async Task ValidateTableEntityAgainstPartitionSchemasAsync(CloudTableEntity<TDomainEntity> tableEntity)
        {
            foreach(var partitionSchema in PartitionSchemas)
            {
                if(partitionSchema.DomainObjectMatchesPartitionCriteria(tableEntity.DomainObjectInstance))
                {
                    var tempTableEntity = new CloudTableEntity<TDomainEntity>(domainObject: tableEntity.DomainObjectInstance);
                    tempTableEntity.PartitionKey = partitionSchema.PartitionKey;
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
                ValidateTableEntityAgainstPartitionSchemas(tempTableEntity);
            }
            WritePartitionSchemasToTable(batchOperation);
        }

        private async Task ExecuteTableOperationAsync(IEnumerable<TDomainEntity> domainEntities, SaveType batchOperation)
        {
            await VerifyAllPartitionsExistAsync();
            await RunTableIndexingAsync();
            foreach(var domainEntity in domainEntities)
            {
                var tempTableEntity = new CloudTableEntity<TDomainEntity>
                {
                    DomainObjectInstance = domainEntity
                };
                await ValidateTableEntityAgainstPartitionSchemasAsync(tempTableEntity);
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
            ValidateTableEntityAgainstPartitionSchemas(tempTableEntity);
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
            await ValidateTableEntityAgainstPartitionSchemasAsync(tempTableEntity);
            await WritePartitionSchemasToTableAsync(batchOperation);
        }

        private void VerifyAllPartitionsExist()
        {
            var shouldWriteToTable = false;
            // Check local list of Partition Schemas against the list of partition keys in _table Context
            PartitionSchemas.ForEach(schema =>
            {
                if(!_partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Contains(schema.PartitionKey))
                {
                    _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(schema.PartitionKey);
                    if(!PartitionKeysInTable.Contains(schema.PartitionKey)) PartitionKeysInTable.Add(schema.PartitionKey);
                    shouldWriteToTable = true;
                    _needToRunTableIndices = true;
                }
            });
            if(shouldWriteToTable) _tableMetaDataContext.InsertOrReplace(_partitionMetaDataEntity);
        }

        private async Task VerifyAllPartitionsExistAsync()
        {
            var shouldWriteToTable = false;
            // Check local list of Partition Schemas against the list of partition keys in _table Context
            PartitionSchemas.ForEach(schema =>
            {
                if(!_partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Contains(schema.PartitionKey))
                {
                    _partitionMetaDataEntity.DomainObjectInstance.PartitionKeys.Add(schema.PartitionKey);
                    if(!PartitionKeysInTable.Contains(schema.PartitionKey)) PartitionKeysInTable.Add(schema.PartitionKey);
                    shouldWriteToTable = true;
                    _needToRunTableIndices = true;
                }
            });
            if(shouldWriteToTable) await _tableMetaDataContext.InsertOrReplaceAsync(_partitionMetaDataEntity);
        }

        private void RunTableIndexing()
        {
            if(!_needToRunTableIndices) return;
            var defaultPartitionEntities = GetByDefaultSchema().ToList();
            _needToRunTableIndices = false;
            if(defaultPartitionEntities.Count > 1)
            {
                Save(defaultPartitionEntities.ToArray(), SaveType.InsertOrReplace);
            }
        }

        private async Task RunTableIndexingAsync()
        {
            if(!_needToRunTableIndices) return;
            var defaultPartitionEntities = await GetByDefaultSchemaAsync();
            _needToRunTableIndices = false;
            if(defaultPartitionEntities.Count > 1)
            {
                await SaveAsync(defaultPartitionEntities.ToArray(), SaveType.InsertOrReplace);
            }
        }

        private void WritePartitionSchemasToTable(SaveType batchOperation)
        {
            Parallel.ForEach(PartitionSchemas, schema =>
            {
                if (schema.CloudTableEntities.Count > 0)
                {
                    var entitiesArray = schema.CloudTableEntities.ToArray();
                    switch (batchOperation)
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
                    //_tableAccessContext.InsertOrReplace(entitiesArray); --> not sure why this was here. Leaving it commented in case there was a valid reason.
                }
                schema.CloudTableEntities.Clear();
            });
        }

        private async Task WritePartitionSchemasToTableAsync(SaveType batchOperation)
        {
            foreach(var schema in PartitionSchemas)
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
                    // await _tableAccessContext.InsertOrReplaceAsync(entitiesArray); --> not sure why this was here. Leaving it commented in case there was a valid reason.
                }
                schema.CloudTableEntities.Clear();
            }
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
        ///     Gets all the entities via the DefaultSchema.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetByDefaultSchema()
        {
            return TableAccessContext.GetByPartitionKey(_defaultSchemaName)
                                     .Select(cloudTableEntity => cloudTableEntity.DomainObjectInstance);
        }

        /// <summary>
        ///     Gets all the entities via the DefaultSchema asynchronously.
        /// </summary>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetByDefaultSchemaAsync()
        {
            var partition = await TableAccessContext.GetByPartitionKeyAsync(_defaultSchemaName);
            return partition.Select(cte => cte.DomainObjectInstance).ToList();
        }

        /// <summary>
        ///     Gets a domain entity using the partition partitionKey's PartitionKey (for the PartitionKey) and the entity's Id
        ///     (for the RowKey).
        ///     If the
        ///     <param name="partitionKey"></param>
        ///     parameter is left null then the DefaultSchema is used.
        /// </summary>
        /// <param name="entityId"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public TDomainEntity GetById(object entityId, string partitionKey = null)
        {
            if(entityId == null) throw new ArgumentNullException("entityId");
            var serializedEntityId = JsonSerializer.SerializeToString(entityId, entityId.GetType());
            if(partitionKey == null) partitionKey = DefaultSchema.PartitionKey;
            var tableEntity = TableAccessContext.Find(partitionKey, serializedEntityId);
            return tableEntity.DomainObjectInstance;
        }

        /// <summary>
        ///     Asynchronously gets a domain entity by the ID using the given entityId and based on the index defined by the given
        ///     partitionKey.
        ///     If the
        ///     <param name="partitionKey"></param>
        ///     parameter is left null then the DefaultSchema is used.
        /// </summary>
        /// <param name="entityId"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public async Task<TDomainEntity> GetByIdAsync(object entityId, string partitionKey = null)
        {
            if(entityId == null) return null;
            var serializedEntityId = JsonSerializer.SerializeToString(entityId, entityId.GetType());
            if(partitionKey == null) partitionKey = DefaultSchema.PartitionKey;
            var tableEntity = await TableAccessContext.FindAsync(partitionKey, serializedEntityId);
            return tableEntity.DomainObjectInstance;
        }

        /// <summary>
        ///     Retrieves all domain entities within a given Partition.
        /// </summary>
        /// <param name="partitionKey">
        ///     If the object being passed in is not a string, it gets serialized to a Jsv string (a la
        ///     ServiceStack.Text library) and that string gets used as a PartitionKey.
        /// </param>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetByPartitionKey(object partitionKey)
        {
            if(partitionKey is string)
            {
                return
                    TableAccessContext.GetByPartitionKey(partitionKey as string)
                                      .Select(tableEntity => tableEntity.DomainObjectInstance);
            }
            var serializedPartitionKey = JsonSerializer.SerializeToString(partitionKey, partitionKey.GetType());
            return
                TableAccessContext.GetByPartitionKey(serializedPartitionKey)
                                  .Select(azureTableEntity => azureTableEntity.DomainObjectInstance);
        }

        /// <summary>
        ///     Asynchronously retrieves all domain entities within a given Partition.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public async Task<List<TDomainEntity>> GetByPartitionKeyAsync(object partitionKey)
        {
            if(partitionKey is string)
            {
                var entities = await TableAccessContext.GetByPartitionKeyAsync(partitionKey as string);
                return entities.Select(tableEntity => tableEntity.DomainObjectInstance).ToList();
            }
            var serializedPartitionKey = JsonSerializer.SerializeToString(partitionKey, partitionKey.GetType());
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
        public IEnumerable<TDomainEntity> GetByPartitionKeyWithRowkeyRange(string partitionKey, string minRowKey = "",
                                                                           string maxRowKey = "")
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
        public async Task<List<TDomainEntity>> GetByPartitionKeyWithRowKeyRangeAsync(string partitionKey, string minRowKey = "", string maxRowKey = "")
        {
            var entites = await TableAccessContext.GetByPartitionKeyWithRowKeyRangeAsync(partitionKey, minRowKey, maxRowKey);
            return entites.Select(ent => ent.DomainObjectInstance).ToList();
        }

        /// <summary>
        ///     Gets a set of domain entities based on a given ParitionSchema with a filter based on the
        ///     <param name="indexedProperty"></param>
        ///     that
        ///     gets passed in.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="indexedProperty"></param>
        /// <returns></returns>
        public IEnumerable<TDomainEntity> GetByIndexedProperty(string partitionKey, object indexedProperty)
        {
            var tempCloudTableEntity = new CloudTableEntity<TDomainEntity>
            {
                IndexedProperty =
                {
                    ValueBeingIndexed = indexedProperty
                }
            };
            var serializedIndexedProperty = JsonSerializer.SerializeToString(tempCloudTableEntity.IndexedProperty,
                tempCloudTableEntity.IndexedProperty.GetType());
            return TableAccessContext.QueryWherePropertyEquals(partitionKey,
                CtConstants.PropNameIndexedProperty, serializedIndexedProperty).Select(cloudTableEntity => cloudTableEntity.DomainObjectInstance);
        }

        /// <summary>
        ///     Asynchronously gets a set of domain entities based on a given ParitionSchema with a filter based on the
        ///     <param name="indexedProperty"></param>
        ///     that
        ///     gets passed in.
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
            var serializedIndexedProperty = JsonSerializer.SerializeToString(tempCloudTableEntity.IndexedProperty,
                tempCloudTableEntity.IndexedProperty.GetType());
            var entities =
                await TableAccessContext.QueryWherePropertyEqualsAsync(partitionKey, CtConstants.PropNameIndexedProperty, serializedIndexedProperty);
            return entities.Select(cte => cte.DomainObjectInstance).ToList();
        }
        #endregion ---- Read Operations ----
    }
}