using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCloudTableContext.Api
{
    public interface ICloudTableEntity<TDomainObject> : ITableEntity where TDomainObject : class, new() {
        /// <summary>
        ///     Instance of the generic type TDomainObject.
        /// </summary>
        TDomainObject DomainObjectInstance { get; set; }

        string PartitionKey { get; set; }
        string RowKey { get; set; }
        DateTimeOffset Timestamp { get; set; }
        string ETag { get; set; }

        /// <summary>
        /// Property that is used to hold an indexed value. This type is a wrapper around the
        /// actual value due to serialization constraints.
        /// </summary>
        IndexedObject IndexedProperty { get; set; }

        void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext);
        IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext);

        /// <summary>
        /// Sets the value of the RowKey for the table entity as a padded integer based on the difference of
        /// the max value property of the DateTimeOffset and the DateTimeOffset.Now property, followed by an
        /// underscore and an random generated GUID.
        /// </summary>
        /// <returns></returns>
        string SetDefaultRowKey();

        /// <summary>
        /// Sets the default value of the PartitionKey for the table entity as a random generated GUID.
        /// </summary>
        /// <returns></returns>
        string SetDefaultPartitionKey();
    }
}