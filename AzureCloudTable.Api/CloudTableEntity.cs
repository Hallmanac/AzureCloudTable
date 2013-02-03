using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceStack.Text;

namespace AzureCloudTable.Api
{
    public class CloudTableEntity<TDomainObject> : ITableEntity where TDomainObject : class, new()
    {
        public CloudTableEntity()
        {
            PartitionKey = SetDefaultPartitionKey();
            RowKey = SetDefaultRowKey();
            DomainObjectInstance = new TDomainObject();
        }

        public CloudTableEntity(string partitionKey = null, string rowKey = null, TDomainObject domainObject = null)
        {
            PartitionKey = partitionKey ?? SetDefaultPartitionKey();
            RowKey = rowKey ?? SetDefaultRowKey();
            DomainObjectInstance = domainObject ?? new TDomainObject();
        }

        /// <summary>
        ///     Instance of the generic type TDomainObject.
        /// </summary>
        public TDomainObject DomainObjectInstance { get; set; }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public object IndexedProperty { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            EntityProperty indexedEntityProperty;
            if(properties.TryGetValue(this.GetPropertyName(() => IndexedProperty), out indexedEntityProperty))
            {
                IndexedProperty = indexedEntityProperty.StringValue.FromJsv<object>();
            }
            ReadFatEntity(properties);
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var entityDictionary = WriteFatEntity(DomainObjectInstance);
            if(IndexedProperty != null)
            {
                string complexTypeSerialized = JsonSerializer.SerializeToString(IndexedProperty, typeof(Object));
                if((complexTypeSerialized.Length > 63997))
                {
                    string truncatedType = complexTypeSerialized.Substring(0, 63999);
                    entityDictionary.Add(this.GetPropertyName(() => IndexedProperty), new EntityProperty(truncatedType));
                }
                else
                {
                    entityDictionary.Add(this.GetPropertyName(() => IndexedProperty),
                        new EntityProperty(complexTypeSerialized));
                }
            }
            return entityDictionary;
        }

        private string SetDefaultRowKey()
        {
            string defaultRowKeyByTime = string.Format("{0:d19}",
                (DateTimeOffset.MaxValue.Ticks - DateTimeOffset.UtcNow.Ticks));
            return defaultRowKeyByTime + "_" + Guid.NewGuid().SerializeToString();
        }

        private string SetDefaultPartitionKey()
        {
            var defaultGuid = default(Guid);
            return defaultGuid.ToString();
        }

        private void ReadFatEntity(IEnumerable<KeyValuePair<string, EntityProperty>> entityProperties)
        {
            var combinedFatEntity = new StringBuilder();
            foreach(var entityProperty in entityProperties)
            {
                if(IsNativeTableProperty(entityProperty.Key) || entityProperty.Key == this.GetPropertyName(()=> IndexedProperty) || entityProperty.Value.PropertyType != EdmType.String)
                    continue;
                combinedFatEntity.Append(entityProperty.Value);
            }
            var fatEntityString = combinedFatEntity.ToString();
            var transitionObject = fatEntityString.FromJsv<Object>();
            DomainObjectInstance = transitionObject as TDomainObject;
            if(DomainObjectInstance == null)
            {
                DomainObjectInstance = new TDomainObject();
            }
        }

        private IDictionary<string, EntityProperty> WriteFatEntity(object givenObject)
        {
            var fatEntityDictionary = new Dictionary<string, EntityProperty>();

            string serializedObject = givenObject.ToJsv();
            int maxStringBlockSize = 63997; //This is a "just in case". I found that when an object is serialized to a UTF-8 encoded 
                                       //string and is saved to a txt file it eats up an additional 3 Bytes. Probably over thinking
                                       //this but hey, that's how I roll.
            int stringLength = serializedObject.Length;
            int dictionaryCount = fatEntityDictionary.Count;
            for(int i = 0;i <= stringLength;i += maxStringBlockSize)
            {
                if((i + maxStringBlockSize) > stringLength)
                    maxStringBlockSize = stringLength - i;
                string entityValue = serializedObject.Substring(i, maxStringBlockSize);
                string entityKey = string.Format("{0:D2}", dictionaryCount);
                if(fatEntityDictionary.Count < 15)
                {
                    fatEntityDictionary.Add(entityKey, new EntityProperty(entityValue));
                }
                else
                {
                    throw new ObjectToLargeForFatEntityException(
                        "Object is too large for serializing into a Fat Table Entity", givenObject);
                }
                dictionaryCount++;
            }
            return fatEntityDictionary;
        }

        private bool IsNativeTableProperty(string propertyName)
        {
            return (propertyName == "PartitionKey" || propertyName == "RowKey" ||
                (propertyName == "Timestamp" || propertyName == "ETag"));
        }
    }

    internal class ObjectToLargeForFatEntityException : Exception
    {
        public ObjectToLargeForFatEntityException(){}

        public ObjectToLargeForFatEntityException(string message, object givenObject)
            :base(message)
        {
            GivenObject = givenObject;
        }

        public object GivenObject { get; private set; }
    }
}