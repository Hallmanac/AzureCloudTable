﻿using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace AzureCloudTableContext.Api
{
    /// <summary>
    ///     Wraps a POCO so that it can be stored directly into Azure Table Storage.
    /// </summary>
    /// <typeparam name="TDomainObject"></typeparam>
    public class CloudTableEntity<TDomainObject> : ITableEntity where TDomainObject : class, new()
    {
        /// <summary>
        /// </summary>
        public CloudTableEntity()
        {
            PartitionKey = SetDefaultPartitionKey();
            RowKey = SetDefaultRowKey();
            DomainObjectInstance = new TDomainObject();
            IndexedProperty = new IndexedObject();
        }

        /// <summary>
        /// </summary>
        /// <param name="partitionKey">Sets the PartitionKey of the table entity.</param>
        /// <param name="rowKey">Sets the RowKey of the table entity.</param>
        /// <param name="domainObject">Sets the POCO that will be serialized to the table entity.</param>
        public CloudTableEntity(string partitionKey = null, string rowKey = null, TDomainObject domainObject = null)
        {
            PartitionKey = partitionKey ?? SetDefaultPartitionKey();
            RowKey = rowKey ?? SetDefaultRowKey();
            DomainObjectInstance = domainObject ?? new TDomainObject();
            IndexedProperty = new IndexedObject();
        }

        /// <summary>
        ///     Instance of the generic type TDomainObject.
        /// </summary>
        public TDomainObject DomainObjectInstance { get; set; }

        public string DomainObjectType { get; set; }

        /// <summary>
        ///     Property that is used to hold an indexed value. This type is a wrapper around the
        ///     actual value due to serialization constraints.
        /// </summary>
        public IndexedObject IndexedProperty { get; set; }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            EntityProperty indexedEntityProperty;
            EntityProperty domainObjType;
            if(properties.TryGetValue(CtConstants.PropNameIndexedProperty, out indexedEntityProperty))
            {
                IndexedProperty = JsonConvert.DeserializeObject<IndexedObject>(indexedEntityProperty.StringValue);
            }
            if(properties.TryGetValue(CtConstants.PropNameDomainObjectType, out domainObjType))
            {
                DomainObjectType = domainObjType.StringValue;
            }
            ReadFatEntity(properties);
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var entityDictionary = WriteFatEntity(DomainObjectInstance);
            if(IndexedProperty == null)
            {
                IndexedProperty = new IndexedObject();
            }
            DomainObjectType = GetAssemblyQualifiedName();
            entityDictionary.Add(CtConstants.PropNameDomainObjectType, new EntityProperty(DomainObjectType));
            var complexTypeSerialized = JsonConvert.SerializeObject(IndexedProperty);
            if((complexTypeSerialized.Length > 63997))
            {
                var truncatedType = complexTypeSerialized.Substring(0, 63999);
                entityDictionary.Add(CtConstants.PropNameIndexedProperty, new EntityProperty(truncatedType));
            }
            else
            {
                entityDictionary.Add(CtConstants.PropNameIndexedProperty,
                    new EntityProperty(complexTypeSerialized));
            }
            return entityDictionary;
        }

        /// <summary>
        ///     Sets the value of the RowKey for the table entity as a padded integer based on the difference of
        ///     the max value property of the DateTimeOffset and the DateTimeOffset.UtcNow property, followed by an
        ///     underscore and an random generated GUID.
        /// </summary>
        /// <returns></returns>
        public string SetDefaultRowKey()
        {
            var defaultRowKeyByTime = $"{(DateTimeOffset.MaxValue.Ticks - DateTimeOffset.UtcNow.Ticks):d19}";
            return defaultRowKeyByTime + "_" + JsonConvert.SerializeObject(Guid.NewGuid());
        }

        /// <summary>
        ///     Sets the default value of the PartitionKey for the table entity as a random generated GUID.
        /// </summary>
        /// <returns></returns>
        public string SetDefaultPartitionKey()
        {
            var defaultGuid = Guid.NewGuid();
            return defaultGuid.ToString();
        }

        private string GetAssemblyQualifiedName()
        {
            if(DomainObjectInstance != null)
            {
                var assemblyQualifiedName = DomainObjectInstance.GetType().AssemblyQualifiedName;
                if(assemblyQualifiedName != null)
                {
                    var typeArray = assemblyQualifiedName.Split(" ".ToCharArray());
                    return typeArray[0] + " " + typeArray[1].Replace(",", "");
                }
            }
            return "";
        }

        private void ReadFatEntity(IEnumerable<KeyValuePair<string, EntityProperty>> entityProperties)
        {
            var combinedFatEntity = new StringBuilder();
            foreach(var entityProperty in entityProperties)
            {
                if(IsNativeTableProperty(entityProperty.Key) || entityProperty.Key == CtConstants.PropNameIndexedProperty ||
                   entityProperty.Key == CtConstants.PropNameDomainObjectType ||
                   entityProperty.Value.PropertyType != EdmType.String)
                {
                    continue;
                }
                combinedFatEntity.Append(entityProperty.Value.StringValue);
            }
            var fatEntityString = combinedFatEntity.ToString();
            var transitionObject = JsonConvert.DeserializeObject<TDomainObject>(fatEntityString);
            //var transitionObject = (TDomainObject)JsonSerializer.DeserializeFromString(fatEntityString, Type.GetType(DomainObjectType));
            DomainObjectInstance = transitionObject;
            if(DomainObjectInstance == null)
            {
                DomainObjectInstance = new TDomainObject();
            }
        }

        private static IDictionary<string, EntityProperty> WriteFatEntity(TDomainObject givenObject)
        {
            var fatEntityDictionary = new Dictionary<string, EntityProperty>();
            var serializedObject = JsonConvert.SerializeObject(givenObject);
            var maxStringBlockSize = 63997; //This is a "just in case". I found that when an object is serialized to a UTF-8 encoded 
            //string and is saved to a txt file it eats up an additional 3 Bytes. Probably over thinking
            //this but hey, that's how I roll.
            var stringLength = serializedObject.Length;
            var dictionaryCount = fatEntityDictionary.Count;
            for(var i = 0; i < stringLength; i += maxStringBlockSize)
            {
                if((i + maxStringBlockSize) > stringLength)
                {
                    maxStringBlockSize = (stringLength - i);
                }
                var entityValue = serializedObject.Substring(i, maxStringBlockSize);
                var entityKey = $"E{(dictionaryCount + 1):D2}";
                if(fatEntityDictionary.Count < 14)
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

        private static bool IsNativeTableProperty(string propertyName)
        {
            return (propertyName == CtConstants.PropNamePartitionKey || propertyName == CtConstants.PropNameRowKey ||
                    (propertyName == CtConstants.PropNameTimeStamp || propertyName == CtConstants.PropNameEtag));
        }
    }

    /// <summary>
    ///     Exception that is thrown when a POCO is too large to be serialized and stored in Azure table storage.
    /// </summary>
    public class ObjectToLargeForFatEntityException : Exception
    {
        /// <summary>
        /// Object is too large
        /// </summary>
        public ObjectToLargeForFatEntityException() { }

        /// <summary>
        /// Object is too large
        /// </summary>
        /// <param name="message"></param>
        /// <param name="givenObject"></param>
        public ObjectToLargeForFatEntityException(string message, object givenObject)
            : base(message) { GivenObject = givenObject; }

        /// <summary>
        ///     The object that was too large to be stored.
        /// </summary>
        public object GivenObject { get; private set; }
    }
}