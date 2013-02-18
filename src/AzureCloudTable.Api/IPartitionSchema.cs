using System;
using System.Collections.Generic;

namespace AzureCloudTableContext.Api
{
    public interface IPartitionSchema<TDomainObject> where TDomainObject : class, new() {
        /// <summary>
        /// Unique name of PartitionSchema.
        /// </summary>
        string SchemaName { get; set; }

        /// <summary>
        /// Called to set the PartitionKey of the CloudTableEntity prior to saving to the table.
        /// </summary>
        Func<TDomainObject, string> SetPartitionKey { get; set; }

        /// <summary>
        /// Called to verify whether or not the given domain entity meets the requirements to be in the current PartitionSchema.
        /// </summary>
        Func<TDomainObject, bool> ValidateEntityForPartition { get; set; }

        /// <summary>
        /// Called to set the RowKey of the CloudTableEntity prior to saving to the table.
        /// </summary>
        Func<TDomainObject, string> SetRowKey { get; set; }

        /// <summary>
        /// Called to set the IndexedProperty of the CloudTableEntity prior to saving to the table.
        /// </summary>
        Func<TDomainObject, object> SetIndexedProperty { get; set; }

        /// <summary>
        /// Holds a list of CloudTableEntity objects that belong to a certain PartitionKey. This is used internally in 
        /// the CloudTableContext class and gets dynamically built up and destroyed.
        /// </summary>
        Dictionary<string, List<CloudTableEntity<TDomainObject>>> CloudTableEntities { get; set; }
    }
}