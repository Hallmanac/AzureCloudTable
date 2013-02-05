using System;
using System.Collections.Generic;
using System.Reflection;
using ServiceStack.Text;

namespace AzureCloudTable.Api
{
    public class PartitionSchema<TDomainObject> where TDomainObject : class, new()
    {
        public PartitionSchema(string partitionName, Func<TDomainObject, bool> validationMethod,
            string nameOfMappedPropertyToRowKey = null, string nameOfIndexedEntityProperty = null)
        {
            PartitionName = partitionName;
            ValidationMethod = validationMethod;
            if(nameOfIndexedEntityProperty != null)
                NameOfIndexedEntityProperty = nameOfIndexedEntityProperty;
            if(nameOfMappedPropertyToRowKey != null)
                NameOfMappedPropertyToRowKey = nameOfMappedPropertyToRowKey;
            CloudTableEntities = new List<CloudTableEntity<TDomainObject>>();
        }
        
        public string PartitionName { get; set; }
        public Func<TDomainObject, bool> ValidationMethod { get; set; }
        public string NameOfMappedPropertyToRowKey { get; set; }
        public string NameOfIndexedEntityProperty { get; set; }
        public List<CloudTableEntity<TDomainObject>> CloudTableEntities { get; set; }

        internal string SetIndexedProperty(CloudTableEntity<TDomainObject> tableEntity)
        {
            if(!string.IsNullOrWhiteSpace(NameOfIndexedEntityProperty))
            {
                var indexedPropertyInfo = typeof(TDomainObject).GetProperty(NameOfIndexedEntityProperty);
                if(indexedPropertyInfo != null)
                {
                    var propertyValue = indexedPropertyInfo.GetValue(tableEntity.DomainObjectInstance);
                    if(propertyValue != null)
                        return propertyValue.ToJsv();
                }
            }
            return string.Empty;
        }

        internal string SetRowKey(CloudTableEntity<TDomainObject> tableEntity)
        {
            if(NameOfMappedPropertyToRowKey != null)
            {
                var mappedProperty = typeof(TDomainObject).GetProperty(NameOfMappedPropertyToRowKey);
                var propertyValue = mappedProperty.GetValue(tableEntity.DomainObjectInstance);
                if(propertyValue != null)
                    return propertyValue.ToJsv();
            }
            return null;
        }
    }
}