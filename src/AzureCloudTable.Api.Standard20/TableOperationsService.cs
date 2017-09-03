using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json;

using Nito.AsyncEx.Synchronous;


namespace Hallmanac.AzureCloudTable.API
{
    /// <summary>
    /// Class that provides direct (lower level) interaction to the current Azure Table through commonly used techniques.
    /// Uses a generic object that implements the ITableEntity interface. This class can be used in
    /// conjunction with the TableEntityWrapper class to wrap a POCO.
    /// </summary>
    /// <typeparam name="TAzureTableEntity"></typeparam>
    public class TableOperationsService<TAzureTableEntity> where TAzureTableEntity : class, ITableEntity, new()
    {
        private readonly TableKeyEncoder _encoder;

        /// <summary>
        /// Constructor that takes in only the storage account for access. It determines a default table name by using the type name
        /// of the TAzureTableEntity
        /// </summary>
        /// <param name="storageAccount"></param>
        public TableOperationsService(CloudStorageAccount storageAccount)
        {
            _encoder = new TableKeyEncoder();
            InitConstructor(storageAccount);
        }

        /// <summary>
        /// Constructor that takes in only the storage account for access. It determines a default table name by using the given tableName property.
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="tableName"></param>
        public TableOperationsService(CloudStorageAccount storageAccount, string tableName)
        {
            _encoder = new TableKeyEncoder();
            InitConstructor(storageAccount, tableName);
        }

        /// <summary>
        /// Constructor that takes in only the storage account for access. It determines a default table name by using the given tableName property.
        /// </summary>
        public TableOperationsService(string connectionString)
        {
            _encoder = new TableKeyEncoder();
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            InitConstructor(storageAccount);
        }

        /// <summary>
        /// Constructor that takes in only the storage account for access. It determines a default table name by using the given tableName property.
        /// </summary>
        public TableOperationsService(string connectionString, string tableName)
        {
            _encoder = new TableKeyEncoder();
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            InitConstructor(storageAccount, tableName);
        }

        private void InitConstructor(CloudStorageAccount storageAccount, string tableName = null)
        {
            _tableName = tableName;
            TableServicePoint = ServicePointManager.FindServicePoint(storageAccount.TableEndpoint);
            TableServicePoint.UseNagleAlgorithm = false;
            TableServicePoint.Expect100Continue = false;
            TableServicePoint.ConnectionLimit = 1000;
            UseBackgroundTaskForIndexing = false;
            TableClient = storageAccount.CreateCloudTableClient();
        }

        /// <summary>
        /// Name of table in Azure Table Storage
        /// </summary>
        public string TableName
        {
            get
            {
                if (!string.IsNullOrWhiteSpace(_tableName))
                    return _tableName;
                _tableName = _encoder.CleanTableNameOfInvalidCharacters($"{typeof(TAzureTableEntity).Name}Table");
                return _tableName;
            }
        }
        private string _tableName;

        /// <summary>
        /// The CloudTableClient object that is used to connect to the current table.
        /// </summary>
        public CloudTableClient TableClient { get; private set; }

        /// <summary>
        /// Gets the current Azure Table being accessed. This is a property expression so it returns a new instance of the
        /// CloudTable class each time this property is accessed
        /// </summary>
        public CloudTable Table
        {
            get
            {
                if (_table != null)
                    return _table;
                _table = TableClient.GetTableReference(TableName);
                _table.CreateIfNotExistsAsync().WaitAndUnwrapException();
                return _table;
            }
        }
        private CloudTable _table;

        /// <summary>
        /// Provides connection management for HTTP connections. We use this to set connection properties to optimize the 
        /// communication to Azure Table Storage.
        /// </summary>
        public ServicePoint TableServicePoint { get; set; }

        /// <summary>
        /// When set to true, this enables the use of background threads (via Task.Factory.StartNew()) to run the indexing on saves which will allow for much faster
        /// save times. This defaults to false.
        /// </summary>
        public bool UseBackgroundTaskForIndexing { get; set; }

        #region ---Writes---
        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void InsertOrMerge(TAzureTableEntity tableEntity)
        {
            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            var updateOperation = TableOperation.InsertOrMerge(tableEntity);
            Table.ExecuteAsync(updateOperation).WaitAndUnwrapException();
        }

        /// <summary>
        /// Executes a single table operation for Insert Or Merge
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task InsertOrMergeAsync(TAzureTableEntity tableEntity)
        {
            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            var updateOperation = TableOperation.InsertOrMerge(tableEntity);
            await Table.ExecuteAsync(updateOperation).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of given entities. Insures that
        /// the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void InsertOrMerge(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperationAsync(entities, CtConstants.TableOpInsertOrMerge).WaitAndUnwrapException();
        }

        /// <summary>
        /// Executes a batch table operation for Insert Or Merge. Insures that the batch meets Azure Table requrirements for 
        /// Entity Group Transactions (i.e. batch no larger than 4MB or no more than 100 in a batch) 
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task InsertOrMergeAsync(TAzureTableEntity[] entities)
        {
            await ExecuteBatchOperationAsync(entities, CtConstants.TableOpInsertOrMerge).ConfigureAwait(false);
        }

        /// <summary>
        ///     Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void InsertOrReplace(TAzureTableEntity tableEntity)
        {
            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            var updateOperation = TableOperation.InsertOrReplace(tableEntity);
            Table.ExecuteAsync(updateOperation).WaitAndUnwrapException();
        }

        /// <summary>
        ///     Executes a single InsertOrReplace table opertion asynchronously.
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task InsertOrReplaceAsync(TAzureTableEntity tableEntity)
        {
            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            var updateOperation = TableOperation.InsertOrReplace(tableEntity);
            await Table.ExecuteAsync(updateOperation).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of given entities. Insures that the batch meets 
        /// Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void InsertOrReplace(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperationAsync(entities, CtConstants.TableOpInsertOrReplace).WaitAndUnwrapException();
        }

        /// <summary>
        ///     Executes a batch InsertOrReplace asynchronously and groups the given entities into groups that meet the Azure Table
        ///     requirements
        ///     for Entity Group Transactions (i.e. batch no larger than 4MB or no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task InsertOrReplaceAsync(TAzureTableEntity[] entities)
        {
           await ExecuteBatchOperationAsync(entities, CtConstants.TableOpInsertOrReplace).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Insert(TAzureTableEntity tableEntity)
        {
            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            var insertTableEntity = TableOperation.Insert(tableEntity);
            Table.ExecuteAsync(insertTableEntity).WaitAndUnwrapException();
        }

        /// <summary>
        /// Executes a single Insert asynchronously.
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task InsertAsync(TAzureTableEntity tableEntity)
        {
            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            var insertTableEntity = TableOperation.Insert(tableEntity);
            await Table.ExecuteAsync(insertTableEntity).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of given entities. Insures that the batch meets 
        /// Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Insert(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperationAsync(entities, CtConstants.TableOpInsert).WaitAndUnwrapException();
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
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Delete(TAzureTableEntity tableEntity)
        {
            DeleteAsync(tableEntity).WaitAndUnwrapException();
        }

        /// <summary>
        /// Executes a single Delete table operation asynchronously.
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task DeleteAsync(TAzureTableEntity tableEntity)
        {
            // Unfortunately Azure Table storage is not smart enough to handle deletes when the entity does not exist so we have to make sure it exists via a try/catch

            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            tableEntity.ETag = "*";
            try
            {
                var deleteOperation = TableOperation.Delete(tableEntity);
                await Table.ExecuteAsync(deleteOperation).ConfigureAwait(false);
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int) HttpStatusCode.NotFound)
                {
                    return;
                }
                throw;
            }
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of given entities. Insures that the batch meets Azure Table 
        /// requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Delete(TAzureTableEntity[] entities)
        {
            DeleteAsync(entities).WaitAndUnwrapException();
        }

        /// <summary>
        /// Executes a batch Delete asynchronously and groups the given entities into groups that meet the Azure Table requirements
        /// for Entity Group Transactions (i.e. batch no larger than 4MB or no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task DeleteAsync(TAzureTableEntity[] entities)
        {
            var batchedEntities = entities.ToList().ToBatch(100);
            foreach (var entityBatch in batchedEntities)
            {
                var allTasks = new List<Task>();
                foreach (var item in entityBatch)
                {
                    var task = DeleteAsync(item);
                    allTasks.Add(task);
                }
                await Task.WhenAll(allTasks).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Replace(TAzureTableEntity tableEntity)
        {
            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            var replaceOperation = TableOperation.Delete(tableEntity);
            Table.ExecuteAsync(replaceOperation).WaitAndUnwrapException();
        }

        /// <summary>
        /// Executes a single Replace table operation asynchronously.
        /// </summary>
        /// <param name="tableEntity"></param>
        /// <returns></returns>
        public async Task ReplaceAsync(TAzureTableEntity tableEntity)
        {
            tableEntity.PartitionKey = _encoder.EncodeTableKey(tableEntity.PartitionKey);
            tableEntity.RowKey = _encoder.EncodeTableKey(tableEntity.RowKey);
            var replaceOperation = TableOperation.Delete(tableEntity);
            await Table.ExecuteAsync(replaceOperation).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of given entiteis. Insures that the batch meets Azure Table requrirements 
        /// for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public void Replace(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperationAsync(entities, CtConstants.TableOpReplace).WaitAndUnwrapException();
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

        private async Task ExecuteBatchOperationAsync(IEnumerable<TAzureTableEntity> entities, string batchMethodName)
        {
            if(entities == null)
            {
                throw new ArgumentNullException(nameof(entities));
            }
            if(string.IsNullOrEmpty(batchMethodName))
            {
                throw new ArgumentNullException(nameof(batchMethodName));
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
            foreach (var pair in batchPartitionPairs)
            {
                try
                {
                    var entityBatch = new EntityBatch(pair.Value.ToArray(), batchMethodName, _encoder);
                    var batchTasks = entityBatch.BatchList.Select(batchOp => Table.ExecuteBatchAsync(batchOp));
                    await Task.WhenAll(batchTasks).ConfigureAwait(false);
                }
                catch(StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        continue;
                    }
                    throw;
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }
        #endregion Writes

        #region Queries
        /// <summary>
        /// Gives access to a raw TableQuery object to create custom queries against the Azure Table.
        /// </summary>
        /// <returns>TableQuery object</returns>
        public TableQuery<TAzureTableEntity> Query()
        {
            var query = new TableQuery<TAzureTableEntity>();
            return query;
        }

        /// <summary>
        /// Gets all table entities that are in a given partition.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> GetByPartitionKey(string partitionKey)
        {
            return GetByPartitionKeyAsync(partitionKey).Result;
        }

        /// <summary>
        /// Gets all table entities that are in a given partition
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> GetByPartitionKeyAsync(string partitionKey)
        {
            var theQuery = Query().Where(GeneratePartitionKeyFilterCondition(partitionKey));
            return await RunQuerySegmentAsync(theQuery).ConfigureAwait(false);
        }

        private async Task<List<TAzureTableEntity>> RunQuerySegmentAsync(TableQuery<TAzureTableEntity> tableQuery)
        {
            TableQuerySegment<TAzureTableEntity> querySegment = null;
            var resolvedList = new List<TAzureTableEntity>();
            while(querySegment == null || querySegment.ContinuationToken != null)
            {
                querySegment = await Table.ExecuteQuerySegmentedAsync(tableQuery, querySegment?.ContinuationToken).ConfigureAwait(false);
                resolvedList.AddRange(querySegment);
            }
            var returnList = new List<TAzureTableEntity>();
            for (var i = 0; i < resolvedList.Count; i++)
            {
                var entity = resolvedList[i];
                if (entity == null)
                    continue;
                entity.PartitionKey = _encoder.DecodeTableKey(entity.PartitionKey);
                entity.RowKey = _encoder.DecodeTableKey(entity.RowKey);
                returnList.Add(entity);
            }
            return returnList;
        }


        /// <summary>
        /// Returns a single table entity based on a given PartitionKey & RowKey combination.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public TAzureTableEntity Find(string partitionKey, string rowKey)
        {
            return FindAsync(partitionKey, rowKey).Result;
        }

        /// <summary>
        /// Returns a single table entity based on a given PartitionKey & RowKey combination.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        public async Task<TAzureTableEntity> FindAsync(string partitionKey, string rowKey)
        {
            var encodedPartitionKey = _encoder.EncodeTableKey(partitionKey);
            var encodedRowKey = _encoder.EncodeTableKey(rowKey);
            var retrieved = await Table.ExecuteAsync(TableOperation.Retrieve<TAzureTableEntity>(encodedPartitionKey, encodedRowKey)).ConfigureAwait(false);
            var entity = (TAzureTableEntity)retrieved.Result;
            if (entity == null)
                return null;
            entity.PartitionKey = _encoder.DecodeTableKey(entity.PartitionKey);
            entity.RowKey = _encoder.DecodeTableKey(entity.RowKey);
            return entity;
        }

        /// <summary>
        /// Gets a series of table entities based on a single PartitionKey combined with a range of RowKey values.
        /// </summary>
        /// <param name="pK"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> GetByPartitionKeyWithRowKeyRange(string pK, string minRowKey = "", string maxRowKey = "")
        {
            return GetByPartitionKeyWithRowKeyRangeAsync(pK, minRowKey, maxRowKey).Result;
        }

        /// <summary>
        /// Gets a series of table entities based on a single PartitionKey combined with a range of RowKey values
        /// asynchronously.
        /// </summary>
        /// <param name="pK"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> GetByPartitionKeyWithRowKeyRangeAsync(string pK, string minRowKey = "", string maxRowKey = "")
        {
            var encMinRowKey = _encoder.EncodeTableKey(minRowKey);
            var encMaxRowKey = _encoder.EncodeTableKey(maxRowKey);
            var pKFilter = GeneratePartitionKeyFilterCondition(pK);
            var rKMinimum = TableQuery.GenerateFilterCondition(CtConstants.PropNameRowKey, QueryComparisons.GreaterThanOrEqual, encMinRowKey);
            var rKMaximum = TableQuery.GenerateFilterCondition(CtConstants.PropNameRowKey, QueryComparisons.LessThanOrEqual, encMaxRowKey);
            string combinedFilter;
            if(string.IsNullOrWhiteSpace(encMinRowKey))
            {
                combinedFilter = $"({pKFilter}) {TableOperators.And} ({rKMaximum})";
            }
            else if(string.IsNullOrWhiteSpace(encMaxRowKey))
            {
                combinedFilter = $"({pKFilter}) {TableOperators.And} ({rKMinimum})";
            }
            else
            {
                combinedFilter = $"({pKFilter}) {TableOperators.And} ({rKMaximum}) {TableOperators.And} ({rKMinimum})";
            }
            var query = Query().Where(combinedFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        /// Shortcut method that returns a TableQuery.GenerateFilterCondition based on an equivalent to the given PartitionKey.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public string GeneratePartitionKeyFilterCondition(string partitionKey)
        {
            return TableQuery.GenerateFilterCondition(CtConstants.PropNamePartitionKey, QueryComparisons.Equal, _encoder.EncodeTableKey(partitionKey));
        }

        private TableQuery<TAzureTableEntity> CreateQueryWithPartitionKeyAndPropertyFilter(string partitionKey, string propertyFilter)
        {
            var pkFilter = GeneratePartitionKeyFilterCondition(partitionKey);
            var combinedFilter = $"({pkFilter}) {TableOperators.And} ({propertyFilter})";
            var query = new TableQuery<TAzureTableEntity>().Where(combinedFilter);
            return query;
        }

        #region QueryWherePropertyEquals method with overloads
        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, string property)
        {
            return QueryWherePropertyEqualsAsync(partitionKey, propertyName, property).Result;
        }

        /// <summary>
        /// Async shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, string property)
        {
            var propertyFilter = TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(_encoder.EncodeTableKey(partitionKey), propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, byte[] property)
        {
            return QueryWherePropertyEqualsAsync(partitionKey, propertyName, property).Result;
        }

        /// <summary>
        /// Async shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, byte[] property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBinary(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(_encoder.EncodeTableKey(partitionKey), propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, bool property)
        {
            return QueryWherePropertyEqualsAsync(partitionKey, propertyName, property).Result;
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, bool property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBool(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(_encoder.EncodeTableKey(partitionKey), propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, DateTimeOffset property)
        {
            return QueryWherePropertyEqualsAsync(partitionKey, propertyName, property).Result;
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
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, DateTimeOffset property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDate(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(_encoder.EncodeTableKey(partitionKey), propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, double property)
        {
            return QueryWherePropertyEqualsAsync(partitionKey, propertyName, property).Result;
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, double property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDouble(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(_encoder.EncodeTableKey(partitionKey), propertyFilter);
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
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, Guid property)
        {
            return QueryWherePropertyEqualsAsync(partitionKey, propertyName, property).Result;
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, Guid property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForGuid(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(_encoder.EncodeTableKey(partitionKey), propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, int property)
        {
            return QueryWherePropertyEqualsAsync(partitionKey, propertyName, property).Result;
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, int property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForInt(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(_encoder.EncodeTableKey(partitionKey), propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        [Obsolete("Be advised that the Azure Storage API for Net Standard and NET Core no longer supports regular synchronous methods. This method is merely wrapping an async call and blocking while waiting.")]
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, long property)
        {
            return QueryWherePropertyEqualsAsync(partitionKey, propertyName, property).Result;
        }

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        public async Task<List<TAzureTableEntity>> QueryWherePropertyEqualsAsync(string partitionKey, string propertyName, long property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForLong(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(_encoder.EncodeTableKey(partitionKey), propertyFilter);
            return await RunQuerySegmentAsync(query).ConfigureAwait(false);
        }
        #endregion

        #endregion

        internal class EntityBatch
        {
            private readonly string _operationName;

            public EntityBatch(IEnumerable<TAzureTableEntity> entities, string operationName, TableKeyEncoder encoder)
            {
                _operationName = operationName;
                EntitiesToBatch = new List<EntityBatchPair>();
                foreach(var azureTableEntity in entities)
                {
                    azureTableEntity.PartitionKey = encoder.EncodeTableKey(azureTableEntity.PartitionKey);
                    azureTableEntity.RowKey = encoder.EncodeTableKey(azureTableEntity.RowKey);
                    EntitiesToBatch.Add(new EntityBatchPair(azureTableEntity));
                }
                BatchList = new List<TableBatchOperation>();
                BatchArrayByteSize = BatchBytesSize();
                GroupEntitiesIntoBatches();
            }

            public List<EntityBatchPair> EntitiesToBatch { get; }

            public Int64 BatchArrayByteSize { get; }

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

            private static void AddOperationToBatch(ref TableBatchOperation tableBatchOperation, TAzureTableEntity entity, string batchMethodName)
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
                        entity.ETag = "*";
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
                SerializedEntity = JsonConvert.SerializeObject(entity);
                EntityByteSize = Encoding.UTF8.GetByteCount(SerializedEntity);
                IsInBatch = false;
            }

            public TAzureTableEntity TableEntity { get; }
            public string SerializedEntity { get; }

            public int EntityByteSize { get; }
            public bool IsInBatch { get; set; }
        }
    }
}