using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace HallmanacAzureTable.EventStore
{
    public class AzureTableContext<TEntity> where TEntity : TableEntity, new()
    {
        private readonly CloudTable _table;

        public AzureTableContext(CloudStorageAccount storageAccount, string tableName)
        {
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
        }

        public void InsertTableEntity(TEntity tableEntity)
        {
            var insertTableEntity = TableOperation.Insert(tableEntity);
            _table.Execute(insertTableEntity);
        }

        public void BatchInsertTableEntities(TEntity[] entities)
        {
            if (entities.Length <= 100)
            {
                var batchOperationForInsert = new TableBatchOperation();
                foreach (TEntity entity in entities)
                {
                    batchOperationForInsert.Insert(entity);
                }
                _table.ExecuteBatch(batchOperationForInsert);
            }
            else
            {
                var processIncrement = 0;
                while (processIncrement <= entities.Length)
                {
                    var batchOf100 = new TEntity[100];
                    var batchOperationForInsert = new TableBatchOperation();
                    for (int i = 0; i < 100; i++)
                    {
                        batchOf100[i] = entities[processIncrement];
                        processIncrement++;
                    }
                    foreach (TEntity entity in batchOf100)
                    {
                        batchOperationForInsert.Insert(entity);
                    }
                    _table.ExecuteBatch(batchOperationForInsert);
                }
            }
        }

        public void UpsertTableEntity(TEntity tableEntity)
        {
            TableOperation updateOperation = TableOperation.InsertOrMerge(tableEntity);
            _table.Execute(updateOperation);
        }

        public void BatchUpsertTableEntity(TEntity[] entities)
        {
            if (entities.Length <= 100)
            {
                var batchOperationForInsertOrMerge = new TableBatchOperation();
                foreach (TEntity entity in entities)
                {
                    batchOperationForInsertOrMerge.InsertOrMerge(entity);
                }
                _table.ExecuteBatch(batchOperationForInsertOrMerge);
            }
            else
            {
                var processIncrement = 0;
                while (processIncrement <= entities.Length)
                {
                    var batchOf100 = new TEntity[100];
                    var batchOperationForInsertOrMerge = new TableBatchOperation();
                    for (int i = 0; i < 100; i++)
                    {
                        batchOf100[i] = entities[processIncrement];
                        processIncrement++;
                    }
                    foreach (TEntity entity in batchOf100)
                    {
                        batchOperationForInsertOrMerge.InsertOrMerge(entity);
                    }
                    _table.ExecuteBatch(batchOperationForInsertOrMerge);
                }
            }
        }

        public void InsertOrReplaceTableEntity(TEntity tableEntity)
        {
            TableOperation updateOperation = TableOperation.InsertOrReplace(tableEntity);
            _table.Execute(updateOperation);
        }

        public void BatchInsertOrReplaceTableEntity(TEntity[] entities)
        {
            if (entities.Length <= 100)
            {
                var batchOperationForInsertOrReplace = new TableBatchOperation();
                foreach (TEntity entity in entities)
                {
                    batchOperationForInsertOrReplace.InsertOrReplace(entity);
                }
                _table.ExecuteBatch(batchOperationForInsertOrReplace);
            }
            else
            {
                var processIncrement = 0;
                while (processIncrement <= entities.Length)
                {
                    var batchOf100 = new TEntity[100];
                    var batchOperationForInsertOrReplace = new TableBatchOperation();
                    for (int i = 0; i < 100; i++)
                    {
                        batchOf100[i] = entities[processIncrement];
                        processIncrement++;
                    }
                    foreach (TEntity entity in batchOf100)
                    {
                        batchOperationForInsertOrReplace.InsertOrReplace(entity);
                    }
                    _table.ExecuteBatch(batchOperationForInsertOrReplace);
                }
            }
        }

        public void DeleteTableEntity(TEntity tableEntity)
        {
            TableOperation deleteOperation = TableOperation.Delete(tableEntity);
            _table.Execute(deleteOperation);
        }

        public void BatchDeleteTableEntities(TEntity[] entities)
        {
            if (entities.Length <= 100)
            {
                var batchOperationForDelete = new TableBatchOperation();
                foreach (TEntity entity in entities)
                {
                    batchOperationForDelete.Delete(entity);
                }
                _table.ExecuteBatch(batchOperationForDelete);
            }
            else
            {
                int processIncrement = 0;
                while (processIncrement <= entities.Length)
                {
                    var batchOf100 = new TEntity[100];
                    var batchOperationForDelete = new TableBatchOperation();
                    for (int i = 0; i < 100; i++)
                    {
                        batchOf100[i] = entities[processIncrement];
                        processIncrement++;
                    }
                    foreach (TEntity entity in batchOf100)
                    {
                        batchOperationForDelete.Delete(entity);
                    }
                    _table.ExecuteBatch(batchOperationForDelete);
                }
            }
        }

        public TableQuery<TEntity> GetTableQuery()
        {
            return new TableQuery<TEntity>();
        }

        public TEntity GetTableEntity(string partitionKey, string rowKey)
        {
            var result = (TEntity)_table.Execute(TableOperation.Retrieve<TEntity>(partitionKey, rowKey)).Result;
            return result;
        }

        public IEnumerable<TEntity> GetByPartitionKey(string partitionKey)
        {
            string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            TableQuery<TEntity> query = new TableQuery<TEntity>().Where(pkFilter);
            IEnumerable<TEntity> entities = GetEntitesByCombinedQuerySegments(query);
            return entities;
        }

        public IEnumerable<TEntity> GetByPartitionKeyWithRowKeyRange(string pK, string minRowKey = "",
                                                                  string maxRowKey = "")
        {
            string pKFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pK);
            string rKMinimum = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                                                                  minRowKey);
            string rKMaximum = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, maxRowKey);
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
            TableQuery<TEntity> query = new TableQuery<TEntity>().Where(combinedFilter);
            IEnumerable<TEntity> entities = GetEntitesByCombinedQuerySegments(query);
            return entities;
        }

        public IEnumerable<TEntity> GetEntitesByCombinedQuerySegments(TableQuery<TEntity> query)
        {
            var entities = new List<TEntity>();
            TableQuerySegment<TEntity> currentQuerySegment = null;
            while (currentQuerySegment == null || currentQuerySegment.ContinuationToken != null)
            {
                currentQuerySegment = _table.ExecuteQuerySegmented(query,
                                                                   currentQuerySegment != null
                                                                           ? currentQuerySegment.ContinuationToken
                                                                           : null);
                entities.AddRange(currentQuerySegment.Results);
            }
            return entities;
        }
    }
}