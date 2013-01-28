using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace HallmanacAzureTable.EventStore
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

        public void InsertTableEntity(TAzureTableEntity tableEntity)
        {
            var insertTableEntity = TableOperation.Insert(tableEntity);
            _table.Execute(insertTableEntity);
        }

        public void BatchInsertTableEntities(TAzureTableEntity[] entities)
        {
            if(entities.Length <= 100)
            {
                var batchOperationForInsert = new TableBatchOperation();
                foreach(TAzureTableEntity entity in entities)
                {
                    batchOperationForInsert.Insert(entity);
                }
                _table.ExecuteBatch(batchOperationForInsert);
            }
            else
            {
                var processIncrement = 0;
                while(processIncrement <= entities.Length)
                {
                    var batchOf100 = new TAzureTableEntity[100];
                    var batchOperationForInsert = new TableBatchOperation();
                    for(int i = 0;i < 100;i++)
                    {
                        batchOf100[i] = entities[processIncrement];
                        processIncrement++;
                    }
                    foreach(TAzureTableEntity entity in batchOf100)
                    {
                        batchOperationForInsert.Insert(entity);
                    }
                    _table.ExecuteBatch(batchOperationForInsert);
                }
            }
        }

        public void InsertOrMergeTableEntity(TAzureTableEntity tableEntity)
        {
            TableOperation updateOperation = TableOperation.InsertOrMerge(tableEntity);
            _table.Execute(updateOperation);
        }

        public void BatchInsertOrMergeTableEntity(TAzureTableEntity[] entities)
        {
            if(entities.Length <= 100)
            {
                var batchOperationForInsertOrMerge = new TableBatchOperation();
                foreach(TAzureTableEntity entity in entities)
                {
                    batchOperationForInsertOrMerge.InsertOrMerge(entity);
                }
                _table.ExecuteBatch(batchOperationForInsertOrMerge);
            }
            else
            {
                var processIncrement = 0;
                while(processIncrement <= entities.Length)
                {
                    var batchOf100 = new TAzureTableEntity[100];
                    var batchOperationForInsertOrMerge = new TableBatchOperation();
                    for(int i = 0;i < 100;i++)
                    {
                        batchOf100[i] = entities[processIncrement];
                        processIncrement++;
                    }
                    foreach(TAzureTableEntity entity in batchOf100)
                    {
                        batchOperationForInsertOrMerge.InsertOrMerge(entity);
                    }
                    _table.ExecuteBatch(batchOperationForInsertOrMerge);
                }
            }
        }

        public void InsertOrReplaceTableEntity(TAzureTableEntity tableEntity)
        {
            TableOperation updateOperation = TableOperation.InsertOrReplace(tableEntity);
            _table.Execute(updateOperation);
        }

        public void BatchInsertOrReplaceTableEntity(TAzureTableEntity[] entities)
        {
            if(entities.Length <= 100)
            {
                var batchOperationForInsertOrReplace = new TableBatchOperation();
                foreach(TAzureTableEntity entity in entities)
                {
                    batchOperationForInsertOrReplace.InsertOrReplace(entity);
                }
                _table.ExecuteBatch(batchOperationForInsertOrReplace);
            }
            else
            {
                var processIncrement = 0;
                while(processIncrement <= entities.Length)
                {
                    var batchOf100 = new TAzureTableEntity[100];
                    var batchOperationForInsertOrReplace = new TableBatchOperation();
                    for(int i = 0;i < 100;i++)
                    {
                        batchOf100[i] = entities[processIncrement];
                        processIncrement++;
                    }
                    foreach(TAzureTableEntity entity in batchOf100)
                    {
                        batchOperationForInsertOrReplace.InsertOrReplace(entity);
                    }
                    _table.ExecuteBatch(batchOperationForInsertOrReplace);
                }
            }
        }

        public void DeleteTableEntity(TAzureTableEntity tableEntity)
        {
            TableOperation deleteOperation = TableOperation.Delete(tableEntity);
            _table.Execute(deleteOperation);
        }

        public void BatchDeleteTableEntities(TAzureTableEntity[] entities)
        {
            if(entities.Length <= 100)
            {
                var batchOperationForDelete = new TableBatchOperation();
                foreach(TAzureTableEntity entity in entities)
                {
                    batchOperationForDelete.Delete(entity);
                }
                _table.ExecuteBatch(batchOperationForDelete);
            }
            else
            {
                int processIncrement = 0;
                while(processIncrement <= entities.Length)
                {
                    var batchOf100 = new TAzureTableEntity[100];
                    var batchOperationForDelete = new TableBatchOperation();
                    for(int i = 0;i < 100;i++)
                    {
                        batchOf100[i] = entities[processIncrement];
                        processIncrement++;
                    }
                    foreach(TAzureTableEntity entity in batchOf100)
                    {
                        batchOperationForDelete.Delete(entity);
                    }
                    _table.ExecuteBatch(batchOperationForDelete);
                }
            }
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
            var entities = new List<TAzureTableEntity>();
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
            var entities = new List<TAzureTableEntity>();
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
            var entities = new List<TAzureTableEntity>();
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
            var entities = new List<TAzureTableEntity>();
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
            var entities = new List<TAzureTableEntity>();
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
            var entities = new List<TAzureTableEntity>();
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
            var entities = new List<TAzureTableEntity>();
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
            var entities = new List<TAzureTableEntity>();
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
    }
}