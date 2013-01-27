using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace HallmanacAzureTable.EventStore
{
    public class AzureTableEntity<TDomainObject> : ITableEntity where TDomainObject : class, new()
    {
        public AzureTableEntity()
        {
            PartitionKey = SetDefaultPartitionKey();
            RowKey = SetDefaultRowKey();
            DomainObjectInstance = new TDomainObject();
        }

        public AzureTableEntity(string partitionKey = null, string rowKey = null, TDomainObject domainObject = null)
        {
            PartitionKey = partitionKey ?? SetDefaultPartitionKey();
            RowKey = rowKey ?? SetDefaultRowKey();
            DomainObjectInstance = domainObject ?? new TDomainObject();
        }

        /// <summary>
        ///     Instance of the generic type TDomainObject.
        /// </summary>
        public TDomainObject DomainObjectInstance { get; set; }
        /// <summary>
        /// In the event that an object is too big to fit within the table entity the remainder of the serialized JSON object string
        /// would reside here. Used mostly for exceptions.
        /// </summary>
        public string IncompleteDomainObjectInstance { get; set; }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            #region Old Way
            /*foreach(PropertyInfo propertyInfo in typeof(TDomainObject).GetProperties())
            {
                if(IsNativeTableProperty(propertyInfo.Name) || !PropertyInfoIsValidForEntityProperty(propertyInfo))
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
            }*/
            #endregion

            ReadFatEntity(properties);
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            #region Old way
            /*var regularEntityDictionary = new Dictionary<string, EntityProperty>();
            foreach(PropertyInfo propertyInfo in typeof(TDomainObject).GetProperties())
            {
                if(IsNativeTableProperty(propertyInfo.Name) || !PropertyInfoIsValidForEntityProperty(propertyInfo))
                    continue;
                EntityProperty entityFromProperty = null;
                try
                {
                    entityFromProperty =
                        CreateEntityPropertyFromObject(propertyInfo.GetValue(DomainObjectInstance), true);
                }
                catch(SerializedEntityPropertySizeException entityPropertySizeException)
                {
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
            }*/
            #endregion

            var entityDictionary = WriteFatEntity(DomainObjectInstance);
            return entityDictionary;
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
                    }
                }
            }
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
                    }
                }
            }
        }

        /// <summary>
        /// Used to create an EntityProperty for use in the IndexedPropertyValue
        /// </summary>
        /// <param name="value"></param>
        /// <param name="allowUnknownTypes"></param>
        /// <returns></returns>
        public EntityProperty CreateEntityPropertyFromObject(object value, bool allowUnknownTypes)
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
                string complexTypeSerialized = value.ToJsv();
                if(!(complexTypeSerialized.Length > 63999))
                    return new EntityProperty(complexTypeSerialized);
                throw new SerializedEntityPropertySizeException(
                    "The string serialized object exceeds the 64KB limit for an EntityProperty.", complexTypeSerialized);
            }
            return new EntityProperty(string.Empty);
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

        private void ReadFatEntity(IEnumerable<KeyValuePair<string, EntityProperty>> entityProperties)
        {
            var combinedFatEntity = new StringBuilder();
            foreach(var entityProperty in entityProperties)
            {
                if(IsNativeTableProperty(entityProperty.Key))
                    continue;
                combinedFatEntity.Append(entityProperty.Value);
            }
            var fatEntityString = combinedFatEntity.ToString();
            var transitionObject = fatEntityString.FromJsv<Object>();
            DomainObjectInstance = transitionObject as TDomainObject;
            if(DomainObjectInstance == null)
            {
                DomainObjectInstance = new TDomainObject();
                IncompleteDomainObjectInstance = fatEntityString;
            }
        }

        private IDictionary<string, EntityProperty> WriteFatEntity(object givenObject)
        {
            var fatEntityDictionary = new Dictionary<string, EntityProperty>();

            string serializedObject = givenObject.ToJsv();
            int maxStringSize = 63997; //This is a "just in case". I found that when an object is serialized to a UTF-8 encoded 
                                       //string and is saved to a txt file it eats up an additional 3 Bytes. Probably over thinking
                                       //this but hey, that's how I roll.
            int stringLength = serializedObject.Length;
            int dictionaryCount = fatEntityDictionary.Count;
            for(int i = 0;i < stringLength;i += maxStringSize)
            {
                if((i + maxStringSize) > stringLength)
                    maxStringSize = stringLength - i;
                string entityValue = serializedObject.Substring(i, maxStringSize);
                string entityKey = string.Format("{0:D2}", dictionaryCount);
                if(fatEntityDictionary.Count < 16)
                {
                    fatEntityDictionary.Add(entityKey, new EntityProperty(entityValue));
                }
                dictionaryCount++;
            }
            return fatEntityDictionary;
        }

        private bool PropertyInfoIsValidForEntityProperty(PropertyInfo propertyInfo)
        {
            return (propertyInfo.GetGetMethod() != null || propertyInfo.GetGetMethod().IsPublic ||
                propertyInfo.GetSetMethod() != null || propertyInfo.GetSetMethod().IsPublic);
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
    }
}