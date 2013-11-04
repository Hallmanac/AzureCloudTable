namespace AzureCloudTableContext.Api
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.Table.Queryable;
    using ServiceStack.Text;

    /// <summary>
    ///     Class that provides direct interaction to the current Azure Table through commonly used techniques.
    ///     Uses a generic object that implements the ITableEntity interface. This class can be used in
    ///     conjunction with the CloudTableEntity class to wrap a POCO.
    /// </summary>
    /// <typeparam name="TAzureTableEntity"></typeparam>
    public class TableAccessContext<TAzureTableEntity> where TAzureTableEntity : ITableEntity, new()
    {
        private readonly CloudStorageAccount _storageAccount;
        private readonly CloudTable _table;

        public TableAccessContext(CloudStorageAccount storageAccount)
        {
            var tableName = string.Format("{0}Table", typeof(TAzureTableEntity).Name);
            _storageAccount = storageAccount;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
        }

        public TableAccessContext(CloudStorageAccount storageAccount, string tableName)
        {
            tableName = string.IsNullOrWhiteSpace(tableName) ? string.Format("{0}Table", typeof(TAzureTableEntity).Name) : tableName;
            _storageAccount = storageAccount;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
        }

        /// <summary>
        ///     Gets the current Azure Table being accessed.
        /// </summary>
        public CloudTable Table { get { return _table; } }

        #region ---Writes---

        /// <summary>
        ///     Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void InsertOrMerge(TAzureTableEntity tableEntity)
        {
            var updateOperation = TableOperation.InsertOrMerge(tableEntity);
            _table.Execute(updateOperation);
        }

        public async Task InsertOrMergeAsync(TAzureTableEntity tableEntity)
        {
            var updateOperation = TableOperation.InsertOrMerge(tableEntity);
            await _table.ExecuteAsync(updateOperation);
        }

        /// <summary>
        ///     Executes a batch table operation of the same name on an array of
        ///     <param name="entities"></param>
        ///     . Insures that
        ///     that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        ///     no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void InsertOrMerge(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, CtConstants.TableOpInsertOrMerge);
        }

        public async Task InsertOrMergeAsync(TAzureTableEntity[] entities)
        {
            await ExecuteBatchOperationAsync(entities, CtConstants.TableOpInsertOrMerge).ConfigureAwait(false);
        }

        /// <summary>
        ///     Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void InsertOrReplace(TAzureTableEntity tableEntity)
        {
            var updateOperation = TableOperation.InsertOrReplace(tableEntity);
            _table.Execute(updateOperation);
        }

        /// <summary>
        /// Executes a single InsertOrReplace table opertion asynchronously.
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task InsertOrReplaceAsync(TAzureTableEntity tableEntity)
        {
            var updateOperation = TableOperation.InsertOrReplace(tableEntity);
            await _table.ExecuteAsync(updateOperation);
        }

        /// <summary>
        ///     Executes a batch table operation of the same name on an array of
        ///     <param name="entities"></param>
        ///     . Insures that
        ///     that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        ///     no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void InsertOrReplace(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, CtConstants.TableOpInsertOrReplace);
        }

        /// <summary>
        /// Executes a batch InsertOrReplace asynchronously and groups the given entities into groups that meet the Azure Table requirements
        /// for Entity Group Transactions (i.e. batch no larger than 4MB or no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task InsertOrReplaceAsync(TAzureTableEntity[] entities)
        {
            await ExecuteBatchOperationAsync(entities, CtConstants.TableOpInsertOrReplace).ConfigureAwait(false);
        }

        /// <summary>
        ///     Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void Insert(TAzureTableEntity tableEntity)
        {
            var insertTableEntity = TableOperation.Insert(tableEntity);
            _table.Execute(insertTableEntity);
        }

        /// <summary>
        /// Executes a single Insert asynchronously.
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task InsertAsync(TAzureTableEntity tableEntity)
        {
            var insertTableEntity = TableOperation.Insert(tableEntity);
            await _table.ExecuteAsync(insertTableEntity);
        }

        /// <summary>
        ///     Executes a batch table operation of the same name on an array of
        ///     <param name="entities"></param>
        ///     . Insures that
        ///     that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        ///     no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void Insert(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, CtConstants.TableOpInsert);
        }

        /// <summary>
        /// Executes a batch InsertOrReplace asynchronously and groups the given entities into groups that meet the Azure Table requirements
        /// for Entity Group Transactions (i.e. batch no larger than 4MB or no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task InsertAsync(TAzureTableEntity[] entities)
        {
            await ExecuteBatchOperationAsync(entities, CtConstants.TableOpInsert).ConfigureAwait(false);
        }

        /// <summary>
        ///     Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void Delete(TAzureTableEntity tableEntity)
        {
            var deleteOperation = TableOperation.Delete(tableEntity);
            _table.Execute(deleteOperation);
        }

        /// <summary>
        /// Executes a single Delete table operation asynchronously.
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task DeleteAsync(TAzureTableEntity tableEntity)
        {
            var deleteOperation = TableOperation.Delete(tableEntity);
            await _table.ExecuteAsync(deleteOperation);
        }

        /// <summary>
        ///     Executes a batch table operation of the same name on an array of
        ///     <param name="entities"></param>
        ///     . Insures that
        ///     that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        ///     no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void Delete(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, CtConstants.TableOpDelete);
        }

        /// <summary>
        /// Executes a batch Delete asynchronously and groups the given entities into groups that meet the Azure Table requirements
        /// for Entity Group Transactions (i.e. batch no larger than 4MB or no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task DeleteAsync(TAzureTableEntity[] entities)
        {
            await ExecuteBatchOperationAsync(entities, CtConstants.TableOpDelete).ConfigureAwait(false);
        }

        /// <summary>
        ///     Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void Replace(TAzureTableEntity tableEntity)
        {
            var replaceOperation = TableOperation.Delete(tableEntity);
            _table.Execute(replaceOperation);
        }

        /// <summary>
        /// Executes a single Replace table operation asynchronously.
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task ReplaceAsync(TAzureTableEntity tableEntity)
        {
            var replaceOperation = TableOperation.Delete(tableEntity);
            await _table.ExecuteAsync(replaceOperation);
        }

        /// <summary>
        ///     Executes a batch table operation of the same name on an array of
        ///     <param name="entities"></param>
        ///     . Insures that
        ///     that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        ///     no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void Replace(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, CtConstants.TableOpReplace);
        }

        /// <summary>
        /// Executes a batch Replace asynchronously and groups the given entities into groups that meet the Azure Table requirements
        /// for Entity Group Transactions (i.e. batch no larger than 4MB or no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task ReplaceAsync(TAzureTableEntity[] entities)
        {
            await ExecuteBatchOperationAsync(entities, CtConstants.TableOpReplace).ConfigureAwait(false);
        }

        private void ExecuteBatchOperation(IEnumerable<TAzureTableEntity> entities, string batchMethodName)
        {
            if(entities == null)
            {
                throw new ArgumentNullException("entities");
            }
            if(string.IsNullOrEmpty(batchMethodName))
            {
                throw new ArgumentNullException("batchMethodName");
            }
            // Creating a dictionary to group partitions together since a batch can only represent one partition.
            var batchPartitionPairs = new ConcurrentDictionary<string, List<TAzureTableEntity>>();
            foreach(var entity in entities)
            {
                var entity1 = entity;
                batchPartitionPairs.AddOrUpdate(entity.PartitionKey, new List<TAzureTableEntity> {entity}, (s, list) =>
                {
                    list.Add(entity1);
                    return list;
                });
            }

            // Iterating through the batch key-value pairs and executing the batch
            foreach(var pair in batchPartitionPairs)
            {
                var entityBatch = new EntityBatch(pair.Value.ToArray(), batchMethodName);
                entityBatch.BatchList.ForEach(batchOp => _table.ExecuteBatch(batchOp));
            }
        }

        private async Task ExecuteBatchOperationAsync(IEnumerable<TAzureTableEntity> entities, string batchMethodName)
        {
            if(entities == null)
            {
                throw new ArgumentNullException("entities");
            }
            if(string.IsNullOrEmpty(batchMethodName))
            {
                throw new ArgumentNullException("batchMethodName");
            }

            // Creating a dictionary to group partitions together since a batch can only represent one partition.
            var batchPartitionPairs = new ConcurrentDictionary<string, List<TAzureTableEntity>>();
            foreach(var entity in entities)
            {
                var entity1 = entity;
                batchPartitionPairs.AddOrUpdate(entity.PartitionKey, new List<TAzureTableEntity> {entity}, (s, list) =>
                {
                    list.Add(entity1);
                    return list;
                });
            }
            // Iterating through the batch key-value pairs and executing the batch one partition at a time.
            foreach(var pair in batchPartitionPairs)
            {
                var entityBatch = new EntityBatch(pair.Value.ToArray(), batchMethodName);
                var batchTasks = entityBatch.BatchList.Select(batchOp => _table.ExecuteBatchAsync(batchOp));
                await Task.WhenAll(batchTasks);
            }
        }
        #endregion Writes

        #region Queries
        /// <summary>
        ///     Gives access to a raw TableQuery object to create custom queries against the Azure Table.
        /// </summary>
        /// <returns>TableQuery object</returns>
        public TableQuery<TAzureTableEntity> Query()
        {
            return _table.CreateQuery<TAzureTableEntity>();
        }

        /// <summary>
        ///     Gets all table entities that are in a given partition.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> GetByPartitionKey(string partitionKey)
        {
            return Query().Where(tEnt => tEnt.PartitionKey == partitionKey);
        }

        public async Task<List<TAzureTableEntity>> GetByPartitionKeyAsync(string partitionKey)
        {
            var theQuery = Query().Where(tEnt => tEnt.PartitionKey == partitionKey);
            return await RunQuerySegmentAsync(theQuery.AsTableQuery()).ConfigureAwait(false);
        }

        private IEnumerable<TAzureTableEntity> RunQuerySegment(TableQuery<TAzureTableEntity> theQuery)
        {
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = theQuery.ExecuteSegmented(currentQuerySegment != null ? currentQuerySegment.ContinuationToken : null);
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
            while(querySegment == null || querySegment.ContinuationToken != null)
            {
                querySegment = await _table.ExecuteQuerySegmentedAsync(tableQuery, querySegment != null ? querySegment.ContinuationToken : null);
                returnList.AddRange(querySegment);
            }
            return returnList;
        }

        private IEnumerable<TAzureTableEntity> RunQuerySegmentWithFilter(TableQuery<TAzureTableEntity> theQuery,
                                                                         Func<TAzureTableEntity, bool> customFilter)
        {
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = theQuery.ExecuteSegmented(currentQuerySegment != null ? currentQuerySegment.ContinuationToken : null);
                foreach (var entity in currentQuerySegment)
                {
                    if(customFilter(entity))
                    {
                        yield return entity;
                    }
                }
            }
        }

        private async Task<List<TAzureTableEntity>> RunQuerySegmentWithFilterAsync(TableQuery<TAzureTableEntity> tableQuery,
                                                                                   Func<TAzureTableEntity, bool> customFilter)
        {
            TableQuerySegment<TAzureTableEntity> querySegment = null;
            var returnList = new List<TAzureTableEntity>();
            while(querySegment == null || querySegment.ContinuationToken != null)
            {
                querySegment = await _table.ExecuteQuerySegmentedAsync(tableQuery, querySegment != null ? querySegment.ContinuationToken : null);
                foreach(var entity in querySegment)
                {
                    if(customFilter(entity))
                    {
                        returnList.Add(entity);
                    }
                }
            }
            return returnList;
        }

        /// <summary>
        ///     Returns a single table entity based on a given PartitionKey & RowKey combination.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        public TAzureTableEntity Find(string partitionKey, string rowKey)
        {
            var retrieve = _table.Execute(TableOperation.Retrieve<TAzureTableEntity>(partitionKey, rowKey));
            var result = retrieve.Result;
            return (TAzureTableEntity)result;
            //return (TAzureTableEntity)_table.Execute(TableOperation.Retrieve<TAzureTableEntity>(partitionKey, rowKey)).Result;
        }

        /// <summary>
        ///     Returns a single table entity based on a given PartitionKey & RowKey combination.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        public async Task<TAzureTableEntity> FindAsync(string partitionKey, string rowKey)
        {
            var query = _table.CreateQuery<TAzureTableEntity>().Where(tEnt => tEnt.PartitionKey == partitionKey && tEnt.RowKey == rowKey);
            var retrieved = await _table.ExecuteAsync(TableOperation.Retrieve<TAzureTableEntity>(partitionKey, rowKey));
            return (TAzureTableEntity)retrieved.Result;
        }

        /// <summary>
        ///     Gets a series of table entities based on a single PartitionKey combined with a range of RowKey values.
        /// </summary>
        /// <param name="pK"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> GetByPartitionKeyWithRowKeyRange(string pK, string minRowKey = "",
                                                                               string maxRowKey = "")
        {
            var pKFilter = GeneratePartitionKeyFilterCondition(pK);
            var rKMinimum = TableQuery.GenerateFilterCondition(CtConstants.PropNameRowKey, QueryComparisons.GreaterThanOrEqual,
                minRowKey);
            var rKMaximum = TableQuery.GenerateFilterCondition(CtConstants.PropNameRowKey, QueryComparisons.LessThanOrEqual, maxRowKey);
            string combinedFilter;
            if(string.IsNullOrWhiteSpace(minRowKey))
            {
                combinedFilter = string.Format("({0}) {1} ({2})", pKFilter, TableOperators.And, rKMaximum);
            }
            else if(string.IsNullOrWhiteSpace(maxRowKey))
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
        /// Gets a series of table entities based on a single PartitionKey combined with a range of RowKey values asynchronously.
        /// </summary>
        /// <param name="pK"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> GetByPartitionKeyWithRowKeyRangeAsync(string pK, string minRowKey = "",
                                                                                                string maxRowKey = "")
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

        /// <summary>
        ///     Shortcut method that returns a TableQuery.GenerateFilterCondition based on an equivalent to the given PartitionKey.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public string GeneratePartitionKeyFilterCondition(string partitionKey)
        {
            return TableQuery.GenerateFilterCondition(CtConstants.PropNamePartitionKey, QueryComparisons.Equal, partitionKey);
        }

        private TableQuery<TAzureTableEntity> CreateQueryWithPartitionKeyAndPropertyFilter(string partitionKey, string propertyFilter)
        {
            var pkFilter = GeneratePartitionKeyFilterCondition(partitionKey);
            var combinedFilter = string.Format("({0}) {1} ({2})", pkFilter, TableOperators.And, propertyFilter);
            var query = new TableQuery<TAzureTableEntity>().Where(combinedFilter);
            return query;
        }

        #region QueryWherePropertyEquals method with overloads
        /// <summary>
        ///     Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, string property)
        {
            var propertyFilter = TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Async shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, string property)
        {
            var propertyFilter = TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, byte[] property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBinary(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Async shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, byte[] property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBinary(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, bool property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBool(propertyName, QueryComparisons.Equal,
                property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, bool property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBool(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, DateTimeOffset property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDate(propertyName, QueryComparisons.Equal,
                property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName,
                                                                                        DateTimeOffset property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDate(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, double property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDouble(propertyName, QueryComparisons.Equal,
                property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, double property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDouble(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, Guid property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForGuid(propertyName, QueryComparisons.Equal,
                property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, Guid property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForGuid(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, int property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForInt(propertyName, QueryComparisons.Equal,
                property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, int property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForInt(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, long property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForLong(propertyName, QueryComparisons.Equal,
                property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return RunQuerySegment(query);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        ///     the same property name. Handles the continuation token scenario as well. Overloaded to accept
        ///     all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, long property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForLong(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        #endregion

        #endregion

        internal class EntityBatch
        {
            private readonly string _operationName;

            public EntityBatch(IEnumerable<TAzureTableEntity> entities, string operationName)
            {
                _operationName = operationName;
                EntitiesToBatch = new List<EntityBatchPair>();
                foreach(var azureTableEntity in entities)
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
                while(qtyRemaining > 0)
                {
                    var batch = new TableBatchOperation();
                    var currentBatchQty = 0;
                    var currentBatchSize = 0;
                    foreach(var entityBatchPair in EntitiesToBatch.Where(etb => !etb.IsInBatch).Select(e => e))
                    {
                        var newSize = currentBatchSize + entityBatchPair.EntityByteSize;
                        if(newSize <= maxBatchSize && currentBatchQty < maxBatchCount)
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

            private void AddOperationToBatch(ref TableBatchOperation tableBatchOperation, TAzureTableEntity entity, string batchMethodName)
            {
                switch(batchMethodName)
                {
                    case CtConstants.TableOpInsert:
                        tableBatchOperation.Insert(entity);
                        break;
                    case CtConstants.TableOpInsertOrMerge:
                        tableBatchOperation.InsertOrMerge(entity);
                        break;
                    case CtConstants.TableOpInsertOrReplace:
                        tableBatchOperation.InsertOrReplace(entity);
                        break;
                    case CtConstants.TableOpMerge:
                        tableBatchOperation.Merge(entity);
                        break;
                    case CtConstants.TableOpDelete:
                        tableBatchOperation.Delete(entity);
                        break;
                    case CtConstants.TableOpReplace:
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