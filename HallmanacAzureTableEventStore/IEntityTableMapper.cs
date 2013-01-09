using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace HallmanacAzureTable.EventStore
{
    public interface IEntityTableMapper<TDomainObject, TTableEntity> where TTableEntity : TableEntity, new()
    {
        string RootEntityTableName { get; }
        AzureTableContext<TTableEntity> RootEntityContext { get; }
        Dictionary<string, AzureTableContext<PartitionedProperty>> IndexedProperties { get; } 
        TDomainObject MapFromTable(TTableEntity tableEntity);
        TTableEntity MapToTable(TDomainObject domainObject);
        string CreatePartitionKeyFromProperty(TDomainObject domainObject, string propertyName);
        string CreateRowKeyFromProperty(TDomainObject domainObject, string propertyName);   
        void CreateIndexedProperty(CloudStorageAccount storageAccount, TDomainObject domainObject, string propertyName);
    }
}