namespace AzureCloudTableContext.Api
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.Table.Queryable;
    using ServiceStack.Text;

    public class AtGatewayService<TAzureTableEntity> where TAzureTableEntity : ITableEntity, new()
    {
        private CloudStorageAccount _storageAccount;
        private CloudTable _table;

        public AtGatewayService(CloudStorageAccount storageAccount)
        {
            var tableName = string.Format("{0}Table", typeof(TAzureTableEntity).Name);
            InitTableAccess(storageAccount, tableName);
        }

        public AtGatewayService(CloudStorageAccount storageAccount, string tableName)
        {
            tableName = string.IsNullOrWhiteSpace(tableName) ? string.Format("{0}Table", typeof(TAzureTableEntity).Name) : tableName;
            InitTableAccess(storageAccount, tableName);
        }

        /// <summary>
        /// Provides direct access to the current CloudTable instance.
        /// </summary>
        public CloudTable Table { get { return _table; } }

        /// <summary>
        /// Provides access to the ServicePointManager for the table instance. This gives the ability to turn off the Nagle Algorithm,
        /// turn off Expect100Continue, increase the max connection limit, and others.
        /// </summary>
        public ServicePoint TableServicePoint { get; set; }

        private void InitTableAccess(CloudStorageAccount storageAccount, string tableName)
        {
            _storageAccount = storageAccount;
            TableServicePoint = ServicePointManager.FindServicePoint(_storageAccount.TableEndpoint);
            TableServicePoint.UseNagleAlgorithm = false;
            TableServicePoint.Expect100Continue = false;
            TableServicePoint.ConnectionLimit = 500;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
        }

        /// <summary>
        /// Calls the CreateIfNotExists() method. This insures that the table creation only happens once as some kind of an 
        /// application startup process so that there aren't calls to this on every instance of this class.
        /// </summary>
        public void InitializeTable()
        {
            _table.CreateIfNotExists();
        }

        /// <summary>
        /// Saves the entity based on the type of table operation specified.
        /// </summary>
        /// <param name="entities"></param>
        /// <param name="operation"></param>
        public void Save(IEnumerable<TAzureTableEntity> entities, SaveType operation)
        {
            ExecuteBatchOperation(entities, operation);
        }

        /// <summary>
        /// Asynchronously saves the entity based on the type of table operation specified.
        /// </summary>
        /// <param name="entities"></param>
        /// <param name="operation">Type of table operation (i.e. Insert, InsertOrMerge, Delete, etc.)</param>
        /// <returns></returns>
        public async Task SaveAsync(IEnumerable<TAzureTableEntity> entities, SaveType operation)
        {
            await ExecuteBatchOperationAsync(entities, operation);
        }

        #region ---- Queries ----

        /// <summary>
        /// Returns an IQueryable table query object.
        /// </summary>
        /// <returns></returns>
        public TableQuery<TAzureTableEntity> Query()
        {
            return Table.CreateQuery<TAzureTableEntity>();
        }

        /// <summary>
        /// Gets a table entity by its partition key and row key.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        public TAzureTableEntity Find(string partitionKey, string rowKey)
        {
            return (TAzureTableEntity)Table.Execute(TableOperation.Retrieve<TAzureTableEntity>(partitionKey, rowKey)).Result;
        }

        /// <summary>
        /// Asynchronously gets a table entity by its partition key and row key.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        public async Task<TAzureTableEntity> FindAsync(string partitionKey, string rowKey)
        {
            var retrieved = await Table.ExecuteAsync(TableOperation.Retrieve<TAzureTableEntity>(partitionKey, rowKey));
            return (TAzureTableEntity)retrieved.Result;
        }

        /// <summary>
        /// Gets collection of entities in a partition that reside within a specified row key range. 
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> GetRowKeyRange(string partitionKey, string minRowKey = "", string maxRowKey = "")
        {
            var pKFilter = GeneratePartitionKeyFilterCondition(partitionKey);
            var rKMinimum = TableQuery.GenerateFilterCondition(AtConstants.PropNameRowKey, QueryComparisons.GreaterThanOrEqual,
                minRowKey);
            var rKMaximum = TableQuery.GenerateFilterCondition(AtConstants.PropNameRowKey, QueryComparisons.LessThanOrEqual, maxRowKey);
            string combinedFilter;
            if (string.IsNullOrWhiteSpace(minRowKey))
            {
                combinedFilter = string.Format("({0}) {1} ({2})", pKFilter, TableOperators.And, rKMaximum);
            }
            else if (string.IsNullOrWhiteSpace(maxRowKey))
            {
                combinedFilter = string.Format("({0}) {1} ({2})", pKFilter, TableOperators.And, rKMinimum);
            }
            else
            {
                combinedFilter = string.Format("({0}) {1} ({2}) {3} ({4})", pKFilter, TableOperators.And, rKMaximum,
                    TableOperators.And, rKMinimum);
            }
            var query = new TableQuery<TAzureTableEntity>().Where(combinedFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Asynchronously gets a collection of entities in a partition which reside within a specified row key range.
        /// </summary>
        /// <param name="pK"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public async Task<IList<TAzureTableEntity>> GetRowKeyRangeAsync(string pK, string minRowKey = "", string maxRowKey = "")
        {
            var pKFilter = GeneratePartitionKeyFilterCondition(pK);
            var rKMinimum = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                minRowKey);
            var rKMaximum = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, maxRowKey);
            string combinedFilter;
            if (string.IsNullOrWhiteSpace(minRowKey))
            {
                combinedFilter = string.Format("({0}) {1} ({2})", pKFilter, TableOperators.And, rKMaximum);
            }
            else if (string.IsNullOrWhiteSpace(maxRowKey))
            {
                combinedFilter = string.Format("({0}) {1} ({2})", pKFilter, TableOperators.And, rKMinimum);
            }
            else
            {
                combinedFilter = string.Format("({0}) {1} ({2}) {3} ({4})", pKFilter, TableOperators.And, rKMaximum,
                    TableOperators.And, rKMinimum);
            }
            var query = _table.CreateQuery<TAzureTableEntity>().Where(combinedFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        private TableQuery<TAzureTableEntity> PartitionKeyAndPropertyFilterQuery(string partitionKey, string propertyFilter)
        {
            var pkFilter = GeneratePartitionKeyFilterCondition(partitionKey);
            var combinedFilter = string.Format("({0}) {1} ({2})", pkFilter, TableOperators.And, propertyFilter);
            var query = new TableQuery<TAzureTableEntity>().Where(combinedFilter);
            return query;
        }

        #region ---- WherePropertyEquals ----

        /// <summary>
        /// Queries the table based on the given partition key and property name and value.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> WherePropertyEquals(string partitionKey, string propertyName, string property)
        {
            var propertyFilter = TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        ///     Async shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> WherePropertyEqualsAsync(string partitionKey, string propertyName, string property)
        {
            var propertyFilter = TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> WherePropertyEquals(string partitionKey, string propertyName, byte[] property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBinary(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        ///     Async shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> WherePropertyEqualsAsync(string partitionKey, string propertyName, byte[] property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBinary(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> WherePropertyEquals(string partitionKey, string propertyName, bool property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBool(propertyName, QueryComparisons.Equal,
                property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> WherePropertyEqualsAsync(string partitionKey, string propertyName, bool property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBool(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> WherePropertyEquals(string partitionKey, string propertyName, DateTimeOffset property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDate(propertyName, QueryComparisons.Equal,
                property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> WherePropertyEqualsAsync(string partitionKey, string propertyName,
                                                                                 DateTimeOffset property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDate(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> WherePropertyEquals(string partitionKey, string propertyName, double property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDouble(propertyName, QueryComparisons.Equal,
                property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> WherePropertyEqualsAsync(string partitionKey, string propertyName, double property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDouble(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> WherePropertyEquals(string partitionKey, string propertyName, Guid property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForGuid(propertyName, QueryComparisons.Equal,
                property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> WherePropertyEqualsAsync(string partitionKey, string propertyName, Guid property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForGuid(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> WherePropertyEquals(string partitionKey, string propertyName, int property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForInt(propertyName, QueryComparisons.Equal,
                property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> WherePropertyEqualsAsync(string partitionKey, string propertyName, int property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForInt(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> WherePropertyEquals(string partitionKey, string propertyName, long property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForLong(propertyName, QueryComparisons.Equal,
                property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> WherePropertyEqualsAsync(string partitionKey, string propertyName, long property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForLong(propertyName, QueryComparisons.Equal, property);
            var query = PartitionKeyAndPropertyFilterQuery(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        #endregion ---- WherePropertyEquals ----

        private IEnumerable<TAzureTableEntity> RunQuerySegment(TableQuery<TAzureTableEntity> theQuery)
        {
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(theQuery,
                    currentQuerySegment != null ? currentQuerySegment.ContinuationToken : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
        }

        private async Task<List<TAzureTableEntity>> RunQuerySegmentAsync(TableQuery<TAzureTableEntity> tableQuery)
        {
            TableQuerySegment<TAzureTableEntity> querySegment = null;
            var returnList = new List<TAzureTableEntity>();
            while (querySegment == null || querySegment.ContinuationToken != null)
            {
                querySegment = await _table.ExecuteQuerySegmentedAsync(tableQuery, querySegment != null ? querySegment.ContinuationToken : null);
                returnList.AddRange(querySegment);
            }
            return returnList;
        }

        private string GeneratePartitionKeyFilterCondition(string partitionKey)
        {
            return TableQuery.GenerateFilterCondition(AtConstants.PropNamePartitionKey, QueryComparisons.Equal, partitionKey);
        }
        #endregion ---- Queries ----

        #region ---- Batch Operations ----

        private void ExecuteBatchOperation(IEnumerable<TAzureTableEntity> entities, SaveType batchMethodName)
        {
            if (entities == null)
            {
                throw new ArgumentNullException("entities");
            }
            // Creating a dictionary to group partitions together since a batch can only represent one partition.
            var batchPartitionPairs = new ConcurrentDictionary<string, List<TAzureTableEntity>>();
            foreach (var entity in entities)
            {
                var entity1 = entity;
                batchPartitionPairs.AddOrUpdate(entity.PartitionKey, new List<TAzureTableEntity> { entity }, (s, list) =>
                {
                    list.Add(entity1);
                    return list;
                });
            }
            // Iterating through the batch key-value pairs and executing the batch
            Parallel.ForEach(batchPartitionPairs, pair =>
            {
                var entityBatch = new EntityBatch(pair.Value.ToArray(), batchMethodName);
                entityBatch.BatchList.ForEach(batchOp => _table.ExecuteBatch(batchOp));
            });
        }

        private async Task ExecuteBatchOperationAsync(IEnumerable<TAzureTableEntity> entities, SaveType batchMethodName)
        {
            if (entities == null)
            {
                throw new ArgumentNullException("entities");
            }
            // Creating a dictionary to group partitions together since a batch can only represent one partition.
            var batchPartitionPairs = new ConcurrentDictionary<string, List<TAzureTableEntity>>();
            foreach (var entity in entities)
            {
                var entity1 = entity;
                batchPartitionPairs.AddOrUpdate(entity.PartitionKey, new List<TAzureTableEntity> { entity }, (s, list) =>
                {
                    list.Add(entity1);
                    return list;
                });
            }
            // Iterating through the batch key-value pairs and executing the batch one partition at a time.
            await Task.Run(() => Parallel.ForEach(batchPartitionPairs, async pair =>
            {
                var entityBatch = new EntityBatch(pair.Value.ToArray(), batchMethodName);
                var batchTasks = entityBatch.BatchList.Select(batchOp => _table.ExecuteBatchAsync(batchOp));
                await Task.WhenAll(batchTasks).ConfigureAwait(false);
            }));
        }

        #endregion ---- Batch Operations ----

        internal class EntityBatch
        {
            private readonly SaveType _operationName;

            public EntityBatch(IEnumerable<TAzureTableEntity> entities, SaveType operationName)
            {
                _operationName = operationName;
                EntitiesToBatch = new List<EntityBatchPair>();
                foreach (var azureTableEntity in entities)
                {
                    EntitiesToBatch.Add(new EntityBatchPair(azureTableEntity));
                }
                BatchList = new List<TableBatchOperation>();
                BatchArrayByteSize = BatchBytesSize();
                GroupEntitiesIntoBatches();
            }

            public List<EntityBatchPair> EntitiesToBatch { get; private set; }

            public Int64 BatchArrayByteSize { get; private set; }

            public List<TableBatchOperation> BatchList { get; set; }

            public void GroupEntitiesIntoBatches()
            {
                BatchList.Clear();
                EntitiesToBatch.ForEach(ent => ent.IsInBatch = false);
                const int maxBatchCount = 100;
                const int maxBatchSize = 4194304;
                var qtyRemaining = EntitiesToBatch.Count;
                while (qtyRemaining > 0)
                {
                    var batch = new TableBatchOperation();
                    var currentBatchQty = 0;
                    var currentBatchSize = 0;
                    foreach (var entityBatchPair in EntitiesToBatch.Where(etb => !etb.IsInBatch).Select(e => e))
                    {
                        var newSize = currentBatchSize + entityBatchPair.EntityByteSize;
                        if (newSize <= maxBatchSize && currentBatchQty < maxBatchCount)
                        {
                            AddOperationToBatch(ref batch, entityBatchPair.TableEntity, _operationName);
                            currentBatchQty++;
                            currentBatchSize += entityBatchPair.EntityByteSize;
                            qtyRemaining--;
                            entityBatchPair.IsInBatch = true;
                        }
                    }
                    BatchList.Add(batch);
                }
            }

            private void AddOperationToBatch(ref TableBatchOperation tableBatchOperation, TAzureTableEntity entity, SaveType batchMethodName)
            {
                switch (batchMethodName)
                {
                    case SaveType.Insert:
                        tableBatchOperation.Insert(entity);
                        break;
                    case SaveType.InsertOrMerge:
                        tableBatchOperation.InsertOrMerge(entity);
                        break;
                    case SaveType.InsertOrReplace:
                        tableBatchOperation.InsertOrReplace(entity);
                        break;
                    case SaveType.Merge:
                        tableBatchOperation.Merge(entity);
                        break;
                    case SaveType.Delete:
                        tableBatchOperation.Delete(entity);
                        break;
                    case SaveType.Replace:
                        tableBatchOperation.Replace(entity);
                        break;
                }
            }

            private long BatchBytesSize()
            {
                long entityBytes = 0;
                EntitiesToBatch.ForEach(ent => entityBytes += ent.EntityByteSize);
                return entityBytes;
            }
        }

        internal class EntityBatchPair
        {
            public EntityBatchPair(TAzureTableEntity entity)
            {
                TableEntity = entity;
                SerializedEntity = JsonSerializer.SerializeToString(entity);
                EntityByteSize = Encoding.UTF8.GetByteCount(SerializedEntity);
                IsInBatch = false;
            }

            public TAzureTableEntity TableEntity { get; private set; }
            public string SerializedEntity { get; private set; }

            public int EntityByteSize { get; private set; }
            public bool IsInBatch { get; set; }
        }
    }
}