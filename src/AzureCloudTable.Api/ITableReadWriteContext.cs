using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCloudTableContext.Api
{
    public interface ITableReadWriteContext<TAzureTableEntity> where TAzureTableEntity : ITableEntity, new() {
        /// <summary>
        /// Gets the current Azure Table being accessed.
        /// </summary>
        CloudTable Table { get; }

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        void InsertOrMerge(TAzureTableEntity tableEntity);

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        void InsertOrMerge(TAzureTableEntity[] entities);

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        void InsertOrReplace(TAzureTableEntity tableEntity);

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        void InsertOrReplace(TAzureTableEntity[] entities);

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        void Insert(TAzureTableEntity tableEntity);

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        void Insert(TAzureTableEntity[] entities);

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        void Delete(TAzureTableEntity tableEntity);

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        void Delete(TAzureTableEntity[] entities);

        /// <summary>
        /// Executes a single table operation of the same name.
        /// </summary>
        /// <param name="tableEntity">Single entity used in table operation.</param>
        void Replace(TAzureTableEntity tableEntity);

        /// <summary>
        /// Executes a batch table operation of the same name on an array of <param name="entities"></param>. Insures that
        /// that the batch meets Azure Table requrirements for Entity Group Transactions (i.e. batch no larger than 4MB or
        /// no more than 100 in a batch).
        /// </summary>
        /// <param name="entities"></param>
        void Replace(TAzureTableEntity[] entities);

        /// <summary>
        /// Gives access to a raw TableQuery object to create custom queries against the Azure Table.
        /// </summary>
        /// <returns>TableQuery object</returns>
        TableQuery<TAzureTableEntity> Query();

        /// <summary>
        /// Gets all table entities that are in a given partition.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> GetByPartitionKey(string partitionKey);

        /// <summary>
        /// Returns a single table entity based on a given PartitionKey & RowKey combination.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        TAzureTableEntity Find(string partitionKey, string rowKey);

        /// <summary>
        /// Gets a series of table entities based on a single PartitionKey combined with a range of RowKey values.
        /// </summary>
        /// <param name="pK"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> GetByPartitionKeyWithRowKeyRange(string pK, string minRowKey = "",
            string maxRowKey = "");

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with 
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, string property);

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with 
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, byte[] property);

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with 
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, bool property);

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with 
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, DateTimeOffset property);

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with 
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, double property);

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with 
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, Guid property);

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with 
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, int property);

        /// <summary>
        /// Shortcut method that queries the table based on a given PartitionKey and given property with 
        /// the same property name. Handles the continuation token scenario as well. Overloaded to accept
        /// all appropriate table entity types.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="propertyName"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        IEnumerable<TAzureTableEntity> QueryWherePropertyEquals(string partitionKey, string propertyName, long property);

        /// <summary>
        /// Shortcut method that returns a TableQuery.GenerateFilterCondition based on an equivalent to the given PartitionKey.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        string GeneratePartitionKeyFilterCondition(string partitionKey);
    }
}