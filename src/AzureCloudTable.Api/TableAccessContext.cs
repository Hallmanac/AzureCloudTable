using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace AzureCloudTableContext.Api
{
    /// <summary>
    /// Class that provides direct interaction to the current Azure Table through commonly used techniques. 
    /// Uses a generic object that implements the ITableEntity interface. This class can be used in 
    /// conjunction with the CloudTableEntity class to wrap a POCO.
    /// </summary>
    /// <typeparam name="TAzureTableEntity"></typeparam>
    public class TableAccessContext<TAzureTableEntity> where TAzureTableEntity : ITableEntity, new()
    {
        private readonly CloudTable _table;
        private readonly CloudStorageAccount _storageAccount;

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
        /// Gets the current Azure Table being accessed.
        /// </summary>
        public CloudTable Table { get { return _table; } }

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void InsertOrMerge(TAzureTableEntity tableEntity)
        {
            TableOperation updateOperation = TableOperation.InsertOrMerge(tableEntity);
            _table.Execute(updateOperation);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void InsertOrMerge(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "InsertOrMerge");
        }

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void InsertOrReplace(TAzureTableEntity tableEntity)
        {
            TableOperation updateOperation = TableOperation.InsertOrReplace(tableEntity);
            _table.Execute(updateOperation);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void InsertOrReplace(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "InsertOrReplace");
        }

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void Insert(TAzureTableEntity tableEntity)
        {
            var insertTableEntity = TableOperation.Insert(tableEntity);
            _table.Execute(insertTableEntity);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void Insert(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "Insert");
        }

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void Delete(TAzureTableEntity tableEntity)
        {
            TableOperation deleteOperation = TableOperation.Delete(tableEntity);
            _table.Execute(deleteOperation);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void Delete(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "Delete");
        }

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        public void Replace(TAzureTableEntity tableEntity)
        {
            var replaceOperation = TableOperation.Delete(tableEntity);
            _table.Execute(replaceOperation);
        }

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        public void Replace(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "Replace");
        }

        #region Queries
        /// <summary>
        /// Gives access to a raw TableQuery object to create custom queries against the Azure Table.
        /// </summary>
        /// <returns>TableQuery object</returns>
        public TableQuery<TAzureTableEntity> Query()
        {
            return new TableQuery<TAzureTableEntity>();
        }

        /// <summary>
        /// Gets all table entities that are in a given partition.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> GetByPartitionKey(string partitionKey)
        {
            string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            TableQuery<TAzureTableEntity> query = new TableQuery<TAzureTableEntity>().Where(pkFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
        }

        /// <summary>
        /// Returns a single table entity based on a given PartitionKey & RowKey combination.
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
        /// Gets a series of table entities based on a single PartitionKey combined with a range of RowKey values.
        /// </summary>
        /// <param name="pK"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        public IEnumerable<TAzureTableEntity> GetByPartitionKeyWithRowKeyRange(string pK, string minRowKey = "",
                                                                     string maxRowKey = "")
        {
            string pKFilter = GeneratePartitionKeyFilterCondition(pK);
            string rKMinimum = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                                                                  minRowKey);
            string rKMaximum = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, maxRowKey);
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
            TableQuery<TAzureTableEntity> query = new TableQuery<TAzureTableEntity>().Where(combinedFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, string property)
        {
            var propertyFilter = TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);

            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                    currentQuerySegment != null
                        ? currentQuerySegment.ContinuationToken
                        : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, byte[] property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBinary(propertyName, QueryComparisons.Equal, property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach(var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, bool property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForBool(propertyName, QueryComparisons.Equal,
                                                                           property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, DateTimeOffset property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDate(propertyName, QueryComparisons.Equal,
                                                                           property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, double property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForDouble(propertyName, QueryComparisons.Equal,
                                                                           property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, Guid property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForGuid(propertyName, QueryComparisons.Equal,
                                                                           property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, int property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForInt(propertyName, QueryComparisons.Equal,
                                                                           property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach (var entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
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
        public IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, long property)
        {
            var propertyFilter = TableQuery.GenerateFilterConditionForLong(propertyName, QueryComparisons.Equal,
                                                                           property);
            var query = CreateQueryWithPartitionKeyAndPropertyFilter(partitionKey, propertyFilter);
            TableQuerySegment<TAzureTableEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                foreach (TAzureTableEntity entity in currentQuerySegment)
                {
                    yield return entity;
                }
            }
        }
        #endregion

        /// <summary>
        /// Shortcut method that returns a TableQuery.GenerateFilterCondition based on an equivalent to the given PartitionKey.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public string GeneratePartitionKeyFilterCondition(string partitionKey)
        {
            return TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
        }

        private TableQuery<TAzureTableEntity> CreateQueryWithPartitionKeyAndPropertyFilter(string partitionKey, string propertyFilter)
        {
            var pkFilter = GeneratePartitionKeyFilterCondition(partitionKey);
            var combinedFilter = string.Format("({0}) {1} ({2})", pkFilter, TableOperators.And, propertyFilter);
            TableQuery<TAzureTableEntity> query = new TableQuery<TAzureTableEntity>().Where(combinedFilter);
            return query;
        }
        #endregion

        private int GetRoughSizeOfEntitiesArray(TAzureTableEntity[] entities)
        {
            var entitesSerializedAsJsv = entities.ToJsv();
            return entitesSerializedAsJsv.Length;
        }

        private bool EntitiesAreOfTheSamePartition(TAzureTableEntity[] entities)
        {
            if(entities == null || entities.Length < 1)
                return false;
            var partitionName = entities[0].PartitionKey;
            return entities.All(azureTableEntity => azureTableEntity.PartitionKey == partitionName);
        }

        private void ExecuteBatchOperation(TAzureTableEntity[] entities, string batchMethodName)
        {
            if (!EntitiesAreOfTheSamePartition(entities))
                throw new Exception("Different Partition Keys detected. Entity Group Transactions (batching) have to be of the same PartitionKey");
            int sizeOfEntitiesArray = GetRoughSizeOfEntitiesArray(entities);
            TableBatchOperation theBatchOperation;
            if (sizeOfEntitiesArray <= 4000000)
            {
                theBatchOperation = new TableBatchOperation();
                if (entities.Length <= 100)
                {
                    foreach (TAzureTableEntity entity in entities)
                    {
                        AddOperationToBatch(ref theBatchOperation, entity, batchMethodName);
                    }
                    _table.ExecuteBatch(theBatchOperation);
                }
                else if (entities.Length > 100)
                {
                    var processIncrement = 0;
                    while (processIncrement <= entities.Length)
                    {
                        var batchOf100 = new TAzureTableEntity[100];
                        theBatchOperation = new TableBatchOperation();
                        for (int i = 0; i < 100; i++)
                        {
                            batchOf100[i] = entities[processIncrement];
                            processIncrement++;
                        }
                        foreach (TAzureTableEntity entity in batchOf100)
                        {
                            AddOperationToBatch(ref theBatchOperation, entity, batchMethodName);
                        }
                        _table.ExecuteBatch(theBatchOperation);
                    }
                }
            }
            //There might be a more efficient way to do this (below). For example, there might be 1500 entities that equal 3MB of space
            //and two entities that are 1MB each (which push the batch over the 4MB limit of Entity Group Transaction's), so it would be 
            //more efficient in that case to not break things into batches of four. The algorithm below just guarantees that things will 
            //work since a single entity cannot exceed 1MB in size.
            else if (sizeOfEntitiesArray > 4000000)
            {
                var incrementer = 0;
                theBatchOperation = new TableBatchOperation();
                while (incrementer <= entities.Length)
                {
                    var batchOf4 = new TAzureTableEntity[4];
                    for (int i = 0; i < 4; i++)
                    {
                        batchOf4[i] = entities[incrementer];
                        incrementer++;
                    }
                    foreach (var entity in batchOf4)
                    {
                        AddOperationToBatch(ref theBatchOperation, entity, batchMethodName);
                    }
                    _table.ExecuteBatch(theBatchOperation);
                }
            }
        }

        private void AddOperationToBatch(ref TableBatchOperation tableBatchOperation, TAzureTableEntity entity, string batchMethodName)
        {
            switch (batchMethodName)
            {
                case "Insert":
                    tableBatchOperation.Insert(entity);
                    break;
                case "InsertOrMerge":
                    tableBatchOperation.InsertOrMerge(entity);
                    break;
                case "InsertOrReplace":
                    tableBatchOperation.InsertOrReplace(entity);
                    break;
                case "Merge":
                    tableBatchOperation.Merge(entity);
                    break;
                case "Delete":
                    tableBatchOperation.Delete(entity);
                    break;
                case "Replace":
                    tableBatchOperation.Replace(entity);
                    break;
            }
        }
    }
}