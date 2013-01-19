using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace HallmanacAzureTable.EventStore
{
    /// <summary>
    /// WARNING: This implementation has issues and is not complete. Use for research purposes or finish the implementation. --- 
    /// This class breaks apart an object into EntityProperties if possible. If the property is another Class type (i.e. reference object) then
    /// that property is serialized as JSON and stored as a string in Table storage as long as it fits within the 64KB limit.
    /// </summary>
    /// <typeparam name="TDomainObject"></typeparam>
    public class TableRow<TDomainObject> : ITableEntity where TDomainObject : class, new()
    {
        //CreatePartitionIndexByProperty(PropertyInfo propInfo, string rowKey = null) --> The PartitionKey would be the value of the property
        //(up to 1000 characters) and the default RowKey would be the AggregateID (unless that's the mapped partition key) then it would be a date-time ticks.

        //CreateCustomPartitionIndex(string customPartitionKey, string customRowKey = null) --> This might require some rethinking since you would 
        //essentially have to query each entity as it's being written to see if it meets the index parameters. That seems like it would be difficult.
        
        private readonly CloudStorageAccount _storageAccount;
        
        public TableRow()
        {
            PartitionKey = SetDefaultPartitionKey();
            RowKey = SetDefaultRowKey();
            DomainObjectInstance = new TDomainObject();
            IsMappedAsFatEntity = false;
            Metadata = new Dictionary<string, object>();
        }

        public TableRow(CloudStorageAccount storageAccount, string partitionKey = null, string rowKey = null, TDomainObject domainObject = null,
            bool isMappedAsFatEntity = false)
        {
            PartitionKey = partitionKey ?? SetDefaultPartitionKey();
            RowKey = rowKey ?? SetDefaultRowKey();
            DomainObjectInstance = domainObject ?? new TDomainObject();
            IsMappedAsFatEntity = isMappedAsFatEntity;
            Metadata = new Dictionary<string, object>();
            _storageAccount = storageAccount;
            QueryContext = new AzureTableContext<TableRow<TDomainObject>>(storageAccount);
        }

        /// <summary>
        ///     Instance of the generic type TDomainObject.
        /// </summary>
        public TDomainObject DomainObjectInstance { get; set; }

        public AzureTableContext<TableRow<TDomainObject>> QueryContext { get; set; }

        /// <summary>
        ///     Dictionary property used to add additional metadata related to the storing of the TDomainObject
        ///     without having to modify the TDomainObject for storage purposes.
        /// </summary>
        public Dictionary<string, Object> Metadata {get; private set; }

        public bool IsMappedAsFatEntity { get; set; }

        public string NameOfPropertyMappedToPartitionKey { get; private set; }
        public string NameOfPropertyMappedToRowKey { get; private set; }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public void AddMetadata(KeyValuePair<string, Object> keyValuePair)
        {
            //double check to make sure that the DomainObjectInstance doesn't have a property with the same name
            //If not, add the pair to _metadata
        }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            foreach(PropertyInfo propertyInfo in typeof(TDomainObject).GetProperties())
            {
                if(IsNativeTableProperty(propertyInfo.Name) || !PropertyIsValidForEntity(propertyInfo))
                    return;
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
                var domainObjectProperty = typeof(TDomainObject).GetProperty(entityProperty.Key);
                if(domainObjectProperty != null) continue;
                if(IsNativeTableProperty(entityProperty.Key)) continue;
                SetMetadataFromEntityProperty(entityProperty);
            }
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var regularEntityDictionary = new Dictionary<string, EntityProperty>();
            foreach(PropertyInfo propertyInfo in typeof(TDomainObject).GetProperties())
            {
                if(IsNativeTableProperty(propertyInfo.Name) || !PropertyIsValidForEntity(propertyInfo))
                    continue;
                EntityProperty entityFromProperty = null;
                try
                {
                    entityFromProperty =
                        CreateEntityPropertyFromObject(propertyInfo.GetValue(DomainObjectInstance), true);
                }
                catch(SerializedEntityPropertySizeException entityPropertySizeException)
                {
                    IsMappedAsFatEntity = true;
                    entityFromProperty = new EntityProperty(entityPropertySizeException.Entity);
                }
                finally
                {
                    if(entityFromProperty == null) {}
                    else
                    {
                        regularEntityDictionary.Add(propertyInfo.Name, entityFromProperty);
                    }
                }
            }
            foreach(var entityProperty in Metadata)
            {
                if(!regularEntityDictionary.ContainsKey(entityProperty.Key))
                {
                    EntityProperty entityPropertyFromMetadata = null;
                    try
                    {
                        entityPropertyFromMetadata = CreateEntityPropertyFromObject(entityProperty.Value,
                            true);
                    }
                    catch(SerializedEntityPropertySizeException entityPropertySizeException)
                    {
                        IsMappedAsFatEntity = true;
                        entityPropertyFromMetadata = new EntityProperty(entityPropertySizeException.Entity);
                    }
                    finally
                    {
                        if(entityPropertyFromMetadata == null) {}
                        else
                        {
                            regularEntityDictionary.Add(entityProperty.Key, entityPropertyFromMetadata);
                        }
                    }
                }
            }
            if(IsMappedAsFatEntity)
            {
                return WriteFatEntity(regularEntityDictionary);
            }
            return regularEntityDictionary;
        }

        private string SetDefaultRowKey()
        {
            string defaultRowKeyByTime = string.Format("{0:d19}",
                (DateTimeOffset.MaxValue.Ticks - DateTimeOffset.UtcNow.Ticks));
            return defaultRowKeyByTime + "_" + Guid.NewGuid().SerializeToString();
        }

        private string SetDefaultPartitionKey()
        {
            return typeof(TDomainObject).Name;
        }

        private IDictionary<string, EntityProperty> WriteFatEntity(Dictionary<string, EntityProperty> regularEntityDictionary)
        {
            var fatEntityDictionary = new Dictionary<string, EntityProperty>();
            string serializedDictionary = JsonSerializer.SerializeToString(regularEntityDictionary,
                typeof(Dictionary<string, EntityProperty>));
            int maxStringSize = 64000;
            int stringLength = serializedDictionary.Length;
            int dictionaryCount = fatEntityDictionary.Count;
            for(int i = 0;i < stringLength;i += maxStringSize)
            {
                if((i + maxStringSize) > stringLength)
                    maxStringSize = stringLength - i;
                string entityValue = serializedDictionary.Substring(i, maxStringSize);
                string entityKey = string.Format("{0:D2}", dictionaryCount);
                if(fatEntityDictionary.Count < 16)
                {
                    fatEntityDictionary.Add(entityKey, new EntityProperty(entityValue));
                }
                dictionaryCount++;
            }
            return fatEntityDictionary;
        }

        private bool PropertyIsValidForEntity(PropertyInfo propertyInfo)
        {
            return (propertyInfo.GetGetMethod() != null || propertyInfo.GetGetMethod().IsPublic ||
                propertyInfo.GetSetMethod() != null || propertyInfo.GetSetMethod().IsPublic);
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
                string complexTypeSerialized = JsonSerializer.SerializeToString(value, typeof(Object));
                if(!(complexTypeSerialized.Length > 63999))
                    return new EntityProperty(complexTypeSerialized);
                throw new SerializedEntityPropertySizeException(
                    "The string serialized object exceeds the 64KB limit for an EntityProperty.", complexTypeSerialized);
            }
            return new EntityProperty(string.Empty);
        }
    }

    public class SerializedEntityPropertySizeException : ApplicationException
    {
        public SerializedEntityPropertySizeException() {}

        public SerializedEntityPropertySizeException(string message, string entity)
            : base(message)
        {
            Entity = entity;
        }

        public string Entity { get; private set; }
    }
}