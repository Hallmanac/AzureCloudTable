using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCloudTableContext.Api
{
    /// <summary>
    /// Interface used to wrap a domain entity for use with Azure Table Storage via using PartitionKey strategies (known as PartitionSchemas) 
    /// for grouping and filtering.
    /// </summary>
    /// <typeparam name="TDomainEntity"></typeparam>
    public interface ICloudTableContext<TDomainEntity> where TDomainEntity : class, new() {
        /// <summary>
        /// Gets a list of the PartitionKeys that are used in the table.
        /// </summary>
        List<string> PartitionKeysInTable { get; }

        /// <summary>
        /// This is the name of the property that is used to store the ID of the Domain Entity.
        /// <para>For example, if there is a domain entity of type User that has a property named "Id" then one would pass
        /// the name of that property ("Id") into the constructor of the CloudTableContext class.</para>
        /// <para>This could be done using the extension method (on Object) called "GetPropertyName"</para>
        /// </summary>
        string NameOfEntityIdProperty { get; set; }

        /// <summary>
        /// Gets the default partition partitionKey used for the table.
        /// </summary>
        PartitionSchema<TDomainEntity> DefaultSchema { get; }

        /// <summary>
        /// Returns a TableReadWriteContext class which allows for more options in constructing custom queries against the table.
        /// </summary>
        /// <returns></returns>
        TableQuery<CloudTableEntity<TDomainEntity>> TableQuery();

        /// <summary>
        /// Adds multiple PartitionSchema types to the current CloudTableContext. 
        /// </summary>
        /// <param name="partitionSchemas"></param>
        void AddMultiplePartitionSchemas(List<PartitionSchema<TDomainEntity>> partitionSchemas);

        /// <summary>
        /// Adds a single PartitionSchema to the current CloudTableContext.
        /// </summary>
        /// <param name="partitionSchema"></param>
        void AddPartitionSchema(PartitionSchema<TDomainEntity> partitionSchema);

        /// <summary>
        /// Executes a single "InsertOrMerge" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        void InsertOrMerge(TDomainEntity domainEntity);

        /// <summary>
        /// Executes a batch "InsertOrMerge" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        void InsertOrMerge(TDomainEntity[] domainEntities);

        /// <summary>
        /// Executes a single "InsertOrReplace" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        void InsertOrReplace(TDomainEntity domainEntity);

        /// <summary>
        /// Executes batch "InsertOrReplace" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        void InsertOrReplace(TDomainEntity[] domainEntities);

        /// <summary>
        /// Executes a single "Insert" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        void Insert(TDomainEntity domainEntity);

        /// <summary>
        /// Executes a batch "Insert" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        void Insert(TDomainEntity[] domainEntities);

        /// <summary>
        /// Executes a single "Delete" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        void Delete(TDomainEntity domainEntity);

        /// <summary>
        /// Executes a batch "Delete" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        void Delete(TDomainEntity[] domainEntities);

        /// <summary>
        /// Executes a single "Replace" table operation.
        /// </summary>
        /// <param name="domainEntity"></param>
        void Replace(TDomainEntity domainEntity);

        /// <summary>
        /// Executes a batch "Replace" table operation.
        /// </summary>
        /// <param name="domainEntities"></param>
        void Replace(TDomainEntity[] domainEntities);

        /// <summary>
        /// Gets all the entities via the DefaultSchema.
        /// </summary>
        /// <returns></returns>
        IEnumerable<TDomainEntity> GetAll();

        /// <summary>
        /// Gets a domain entity using the partition partitionKey's SchemaName (for the PartitionKey) and the entity's Id (for the RowKey).
        /// If the <param name="partitionKey"></param> parameter is left null then the DefaultSchema is used.
        /// </summary>
        /// <param name="entityId"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        TDomainEntity GetById(object entityId, string partitionKey = null);

        /// <summary>
        /// Retrieves all domain entities within a given PartitionKey.
        /// </summary>
        /// <param name="partitionKey">If the object being passed in is not a string, it gets serialized to a Jsv string (a la 
        /// ServiceStack.Text library) and that string gets used as a PartitionKey.</param>
        /// <returns></returns>
        IEnumerable<TDomainEntity> GetByPartitionKey(object partitionKey);

        /// <summary>
        /// Retrieves a set of domain entities based on a given PartitionScheme and an optional RowKey range.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns></returns>
        IEnumerable<TDomainEntity> GetByPartitionKeyWithRowkeyRange(string partitionKey, string minRowKey = "",
            string maxRowKey = "");

        /// <summary>
        /// Gets a set of domain entities based on a given ParitionSchema with a filter based on the <param name="indexedProperty"></param> that 
        /// gets passed in.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="indexedProperty"></param>
        /// <returns></returns>
        IEnumerable<TDomainEntity> QueryWhereIndexedPropertyEquals(string partitionKey, object indexedProperty);
    }
}