using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace HallmanacAzureTable.EventStore
{
    public class TableRow<TDomainObject> : ITableEntity where TDomainObject : class, new()
    {
        public TableRow()
        {
            DomainObjectInstance = new TDomainObject();
            Metadata = new Dictionary<string, Object>();
        }

        public TableRow(string partitionKey, string rowKey, TDomainObject domainObject = null)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            DomainObjectInstance = domainObject ?? new TDomainObject();
            Metadata = new Dictionary<string, Object>();
        }

        /// <summary>
        ///     Instance of the generic type TDomainObject.
        /// </summary>
        public TDomainObject DomainObjectInstance { get; set; }

        /// <summary>
        ///     Dictionary property used to add additional metadata related to the storing of the TDomainObject
        ///     without having to modify the TDomainObject for storage purposes.
        /// </summary>
        public Dictionary<string, Object> Metadata { get; set; }

        public string NameOfPropertyMappedToPartitionKey { get; private set; }
        public string NameOfPropertyMappedToRowKey { get; private set; }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            foreach(PropertyInfo propertyInfo in typeof(TDomainObject).GetProperties())
            {
                if(IsNativeTableProperty(propertyInfo.Name) ||
                    propertyInfo.GetGetMethod() == null || !propertyInfo.GetGetMethod().IsPublic ||
                    !properties.ContainsKey(propertyInfo.Name)) return;
                EntityProperty entityProperty = properties[propertyInfo.Name];
                if(entityProperty == null)
                {
                    propertyInfo.SetValue(DomainObjectInstance, null, null);
                }
                else
                {
                    SetPropertyValueFromEntityProperty(entityProperty, propertyInfo);
                }
            }
            foreach(var entityProperty in properties)
            {
                bool propertyIsNotInDomainObject = true;
                foreach(PropertyInfo propertyInfo in typeof(TDomainObject).GetProperties())
                {
                    if(entityProperty.Key != propertyInfo.Name) continue;
                    propertyIsNotInDomainObject = false;
                }
                if(propertyIsNotInDomainObject && !entityProperty.Key.Equals("PartitionKey") &&
                    !entityProperty.Key.Equals("RowKey") && !entityProperty.Key.Equals("ETag") &&
                    !entityProperty.Key.Equals("Timestamp"))
                {
                    SetMetadataFromEntityProperty(entityProperty);
                }
            }
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dictionary = new Dictionary<string, EntityProperty>();
            foreach(PropertyInfo propertyInfo in typeof(TDomainObject).GetProperties())
            {
                if(IsNativeTableProperty(propertyInfo.Name) ||
                    propertyInfo.GetGetMethod() == null || !propertyInfo.GetGetMethod().IsPublic ||)
                    continue;
                EntityProperty propertyFromObject =
                    CreateEntityPropertyFromObject(propertyInfo.GetValue(DomainObjectInstance), true);
                if(propertyFromObject != null)
                    dictionary.Add(propertyInfo.Name, propertyFromObject);
            }
            foreach(var entityProperty in Metadata)
            {
                if(!dictionary.ContainsKey(entityProperty.Key))
                {
                    EntityProperty entityPropertyFromMetadata = CreateEntityPropertyFromObject(entityProperty.Value,
                        true);
                    dictionary.Add(entityProperty.Key, entityPropertyFromMetadata);
                }
            }
            return dictionary;
        }

        public void MapPropertyValueToPartitionKey(PropertyInfo givenPropertyInfo)
        {
            if(DomainObjectInstance != null)
                foreach(PropertyInfo propInfo in DomainObjectInstance.GetType().GetProperties())
                {
                    if(String.Equals(propInfo.Name, givenPropertyInfo.Name))
                    {
                        object propertyValue = propInfo.GetValue(DomainObjectInstance);
                        PartitionKey = JsonSerializer.SerializeToString(propertyValue, typeof(Object));
                        NameOfPropertyMappedToPartitionKey = givenPropertyInfo.Name;
                    }
                }
        }

        public void MapPropertyNameToPartitionKey(PropertyInfo givenPropertyInfo)
        {
            if(DomainObjectInstance != null)
            {
                foreach(PropertyInfo propertyInfo in DomainObjectInstance.GetType().GetProperties())
                {
                    if(String.Equals(propertyInfo.Name, givenPropertyInfo.Name))
                    {
                        PartitionKey = givenPropertyInfo.Name;
                        NameOfPropertyMappedToPartitionKey = givenPropertyInfo.Name;
                    }
                }
            }
        }

        public void MapPropertyValueToRowKey(PropertyInfo givenPropertyInfo)
        {
            if(DomainObjectInstance != null)
            {
                foreach(PropertyInfo propertyInfo in DomainObjectInstance.GetType().GetProperties())
                {
                    if(String.Equals(propertyInfo.Name, givenPropertyInfo.Name))
                    {
                        object propertyValue = propertyInfo.GetValue(DomainObjectInstance);
                        RowKey = JsonSerializer.SerializeToString(propertyValue, typeof(Object));
                        NameOfPropertyMappedToRowKey = givenPropertyInfo.Name;
                    }
                }
            }
        }

        public void MapPropertyNameToRowKey(PropertyInfo givenPropertyInfo)
        {
            if(DomainObjectInstance != null)
            {
                foreach(PropertyInfo propertyInfo in DomainObjectInstance.GetType().GetProperties())
                {
                    if(String.Equals(propertyInfo.Name, givenPropertyInfo.Name))
                    {
                        RowKey = givenPropertyInfo.Name;
                        NameOfPropertyMappedToRowKey = givenPropertyInfo.Name;
                    }
                }
            }
        }

        private bool IsNativeTableProperty(string propertyName)
        {
            return (propertyName == "PartitionKey" || propertyName == "RowKey" ||
                (propertyName == "Timestamp" || propertyName == "ETag"));
        }

        private void SetPropertyValueFromEntityProperty(EntityProperty entityProperty, PropertyInfo propertyInfo)
        {
            switch(entityProperty.PropertyType)
            {
                case EdmType.String:
                    if(propertyInfo.PropertyType == typeof(string))
                    {
                        propertyInfo.SetValue(obj: DomainObjectInstance, value: entityProperty.StringValue);
                        return;
                    }
                    else
                        return;
                case EdmType.Binary:
                    if(propertyInfo.PropertyType == typeof(byte[]))
                    {
                        propertyInfo.SetValue(obj: DomainObjectInstance, value: entityProperty.BinaryValue);
                        return;
                    }
                    else
                        return;
                case EdmType.Boolean:
                    if(propertyInfo.PropertyType == typeof(bool) ||
                        propertyInfo.PropertyType == typeof(bool?))
                    {
                        propertyInfo.SetValue(obj: DomainObjectInstance, value: entityProperty.BooleanValue);
                        return;
                    }
                    else
                        return;
                case EdmType.DateTime:
                    if(propertyInfo.PropertyType == typeof(DateTime))
                    {
                        if(entityProperty.DateTimeOffsetValue != null)
                            propertyInfo.SetValue(obj: DomainObjectInstance,
                                value: entityProperty.DateTimeOffsetValue.Value.UtcDateTime);
                        return;
                    }
                    else if(propertyInfo.PropertyType == typeof(DateTime?))
                    {
                        propertyInfo.SetValue(obj: DomainObjectInstance,
                            value: (entityProperty.DateTimeOffsetValue.HasValue
                                ? entityProperty.DateTimeOffsetValue.Value.UtcDateTime : new DateTime?()));
                        return;
                    }
                    else if(propertyInfo.PropertyType == typeof(DateTimeOffset))
                    {
                        if(entityProperty.DateTimeOffsetValue != null)
                            propertyInfo.SetValue(obj: DomainObjectInstance,
                                value: entityProperty.DateTimeOffsetValue.Value);
                        return;
                    }
                    else if(propertyInfo.PropertyType == typeof(DateTimeOffset?))
                    {
                        propertyInfo.SetValue(obj: DomainObjectInstance, value: entityProperty.DateTimeOffsetValue);
                        return;
                    }
                    else
                        return;
                case EdmType.Double:
                    if(propertyInfo.PropertyType == typeof(double) ||
                        propertyInfo.PropertyType == typeof(double?))
                    {
                        propertyInfo.SetValue(obj: DomainObjectInstance, value: entityProperty.DoubleValue);
                        return;
                    }
                    else
                        return;
                case EdmType.Guid:
                    if(propertyInfo.PropertyType == typeof(Guid) ||
                        propertyInfo.PropertyType == typeof(Guid?))
                    {
                        propertyInfo.SetValue(DomainObjectInstance, entityProperty.GuidValue);
                        return;
                    }
                    else
                        return;
                case EdmType.Int32:
                    if(propertyInfo.PropertyType == typeof(int) ||
                        propertyInfo.PropertyType == typeof(int?))
                    {
                        propertyInfo.SetValue(DomainObjectInstance, entityProperty.Int32Value);
                        return;
                    }
                    else
                        return;
                case EdmType.Int64:
                    if(propertyInfo.PropertyType == typeof(long) ||
                        propertyInfo.PropertyType == typeof(long?))
                    {
                        propertyInfo.SetValue(obj: DomainObjectInstance, value: entityProperty.Int64Value);
                        return;
                    }
                    else
                        return;
                default:
                    var deserializedFromString = JsonSerializer.DeserializeFromString<Object>(entityProperty.StringValue);
                    propertyInfo.SetValue(DomainObjectInstance,
                        Convert.ChangeType(deserializedFromString, propertyInfo.PropertyType));
                    return;
            }
        }

        private void SetMetadataFromEntityProperty(KeyValuePair<string, EntityProperty> entityProperty)
        {
            switch(entityProperty.Value.PropertyType)
            {
                case EdmType.String:
                    Metadata.Add(entityProperty.Key, entityProperty.Value);
                    return;
                case EdmType.Binary:
                    Metadata.Add(entityProperty.Key, entityProperty.Value);
                    return;
                case EdmType.Boolean:
                    Metadata.Add(entityProperty.Key, entityProperty.Value);
                    return;
                case EdmType.DateTime:
                    Metadata.Add(entityProperty.Key, entityProperty.Value);
                    return;
                case EdmType.Double:
                    Metadata.Add(entityProperty.Key, entityProperty.Value);
                    return;
                case EdmType.Guid:
                    Metadata.Add(entityProperty.Key, entityProperty.Value);
                    return;
                case EdmType.Int32:
                    Metadata.Add(entityProperty.Key, entityProperty.Value);
                    return;
                case EdmType.Int64:
                    Metadata.Add(entityProperty.Key, entityProperty.Value);
                    return;
                default:
                    var deserializedFromString =
                        JsonSerializer.DeserializeFromString<Object>(entityProperty.Value.StringValue);
                    Metadata.Add(entityProperty.Key, deserializedFromString);
                    return;
            }
        }

        private static EntityProperty CreateEntityPropertyFromObject(object value, bool allowUnknownTypes)
        {
            var stringValue = value as string;
            if(stringValue != null)
                return new EntityProperty(stringValue);
            var bytesValue = value as byte[];
            if(bytesValue != null)
                return new EntityProperty(bytesValue);
            if(value is bool)
                return new EntityProperty((bool)value);
            if(value is DateTime)
                return new EntityProperty((DateTime)value);
            if(value is DateTimeOffset)
                return new EntityProperty((DateTimeOffset)value);
            if(value is double)
                return new EntityProperty((double)value);
            var guidValue = value as Guid?;
            if(guidValue != null)
                return new EntityProperty(guidValue);
            if(value is int)
                return new EntityProperty((int)value);
            if(value is long)
                return new EntityProperty((long)value);
            if(allowUnknownTypes)
            {
                //TODO: Check if the string is longer than 63,500 characters
                string complexTypeSerialized = JsonSerializer.SerializeToString(value, typeof(Object));
                return new EntityProperty(complexTypeSerialized);
            }
            return new EntityProperty(string.Empty);
        }
    }
}