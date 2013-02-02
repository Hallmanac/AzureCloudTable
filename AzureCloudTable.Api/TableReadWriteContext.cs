using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace AzureCloudTable.Api
{
    public class TableReadWriteContext<TAzureTableEntity> where TAzureTableEntity : ITableEntity, new()
    {
        private readonly CloudTable _table;
        private readonly CloudStorageAccount _storageAccount;

        public TableReadWriteContext(CloudStorageAccount storageAccount)
        {
            var tableName = string.Format("{0}Table", typeof(TAzureTableEntity).Name);
            _storageAccount = storageAccount;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
        }
        
        public TableReadWriteContext(CloudStorageAccount storageAccount, string tableName)
        {
            tableName = string.IsNullOrWhiteSpace(tableName) ? string.Format("{0}Table", typeof(TAzureTableEntity).Name) : tableName;
            _storageAccount = storageAccount;
            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
        }

        public void InsertOrMerge(TAzureTableEntity tableEntity)
        {
            TableOperation updateOperation = TableOperation.InsertOrMerge(tableEntity);
            _table.Execute(updateOperation);
        }

        public void InsertOrMerge(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "InsertOrMerge");
        }

        public void InsertOrReplace(TAzureTableEntity tableEntity)
        {
            TableOperation updateOperation = TableOperation.InsertOrReplace(tableEntity);
            _table.Execute(updateOperation);
        }

        public void InsertOrReplace(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "InsertOrReplace");
        }

        public void Insert(TAzureTableEntity tableEntity)
        {
            var insertTableEntity = TableOperation.Insert(tableEntity);
            _table.Execute(insertTableEntity);
        }

        public void Insert(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "Insert");
        }

        public void Delete(TAzureTableEntity tableEntity)
        {
            TableOperation deleteOperation = TableOperation.Delete(tableEntity);
            _table.Execute(deleteOperation);
        }

        public void Delete(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "Delete");
        }

        public void Replace(TAzureTableEntity tableEntity)
        {
            var replaceOperation = TableOperation.Delete(tableEntity);
            _table.Execute(replaceOperation);
        }

        public void Replace(TAzureTableEntity[] entities)
        {
            ExecuteBatchOperation(entities, "Replace");
        }

        #region Queries
        public TableQuery<TAzureTableEntity> Query()
        {
            return new TableQuery<TAzureTableEntity>();
        }

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

        public TAzureTableEntity Find(string partitionKey, string rowKey)
        {
            return (TAzureTableEntity)_table.Execute(TableOperation.Retrieve<TAzureTableEntity>(partitionKey, rowKey)).Result;
        }

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
            //and two entities that are 1MB each (which push the batch over the 4MB limit of EGT's), so it would be more efficient in 
            //that case to not break things into batches of four. The algorithm below just guarantees that things will work since a single
            //entity cannot exceed 1MB in size.
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