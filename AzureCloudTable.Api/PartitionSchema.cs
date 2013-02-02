using System;
using System.Collections.Generic;

namespace AzureCloudTable.Api
{
    public class PartitionSchema<TDomainObject> where TDomainObject : class, new()
    {
        public PartitionSchema()
        {
            CloudTableEntities = new List<CloudTableEntity<TDomainObject>>();
        }

        public PartitionSchema(string partitionName, Func<TDomainObject, bool> validationMethod,
            object partitionIndexedProperty = null)
        {
            PartitionName = partitionName;
            ValidationMethod = validationMethod;
            if(partitionIndexedProperty != null)
                PartitionIndexedProperty = partitionIndexedProperty;
            CloudTableEntities = new List<CloudTableEntity<TDomainObject>>();
        }
        
        public string PartitionName { get; set; }
        public Func<TDomainObject, bool> ValidationMethod { get; set; }
        public object PartitionIndexedProperty { get; set; }
        public List<CloudTableEntity<TDomainObject>> CloudTableEntities { get; set; }

        public void SetIndexedProperty(CloudTableEntity<TDomainObject> tableEntity)
        {
            if(PartitionIndexedProperty == null)
            {
                PartitionIndexedProperty = string.Empty;
            }
            tableEntity.IndexedProperty = PartitionIndexedProperty;
        }
    }
}