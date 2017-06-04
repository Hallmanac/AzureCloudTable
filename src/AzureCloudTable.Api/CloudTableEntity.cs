using System;
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
        /// Constructor will set a default partition key based on the type name of the TDomainObject and a default row
        /// key by subtracting the Max DateTimeOffset.Ticks from current DateTimeOffset.Ticks and appending a GUID.
        /// </summary>
        public CloudTableEntity()
        {
            PartitionKey = SetDefaultPartitionKey();
            RowKey = SetDefaultRowKey();
            DomainObjectInstance = new TDomainObject();
        }


        /// <summary>
        ///  If the parameters are null, the constructor will set a default partition key based on the type name of the TDomainObject and a default row
        /// key by subtracting the Max DateTimeOffset.Ticks from current DateTimeOffset.Ticks and appending a GUID.
        /// </summary>
        /// <param name="partitionKey">Sets the PartitionKey of the table entity.</param>
        /// <param name="rowKey">Sets the RowKey of the table entity.</param>
        /// <param name="domainObject">Sets the POCO that will be serialized to the table entity.</param>
        public CloudTableEntity(string partitionKey = null, string rowKey = null, TDomainObject domainObject = null)
        {
            PartitionKey = partitionKey ?? SetDefaultPartitionKey();
            RowKey = rowKey ?? SetDefaultRowKey();
            DomainObjectInstance = domainObject ?? new TDomainObject();
        }


        /// <summary>Gets or sets the entity's partition key.</summary>
        /// <value>The entity's partition key.</value>
        public string PartitionKey { get; set; }

        /// <summary>Gets or sets the entity's row key.</summary>
        /// <value>The entity's row key.</value>
        public string RowKey { get; set; }

        /// <summary>Gets or sets the entity's timestamp.</summary>
        /// <value>The entity's timestamp. The property is populated by the Microsoft Azure Table Service.</value>
        public DateTimeOffset Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the entity's current ETag.  Set this value to '*'
        /// in order to blindly overwrite an entity as part of an update
        /// operation.
        /// </summary>
        /// <value>The entity's timestamp.</value>
        public string ETag { get; set; }


        /// <summary>
        /// Populates the entity's properties from the <see cref="T:Microsoft.WindowsAzure.Storage.Table.EntityProperty" /> data values in the <paramref name="properties" /> dictionary.
        /// </summary>
        /// <param name="properties">The dictionary of string property names to <see cref="T:Microsoft.WindowsAzure.Storage.Table.EntityProperty" /> data values to deserialize and store in this table entity instance.</param>
        /// <param name="operationContext">An <see cref="T:Microsoft.WindowsAzure.Storage.OperationContext" /> object that represents the context for the current operation.</param>
        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            if (properties.TryGetValue(CtConstants.PropNameIndexedProperty, out EntityProperty indexedEntityProperty))
            {
                IndexedProperty = JsonConvert.DeserializeObject<IndexedObject>(indexedEntityProperty.StringValue);
            }
            if (properties.TryGetValue(CtConstants.PropNameDomainObjectType, out EntityProperty domainObjType))
            {
                DomainObjectType = domainObjType.StringValue;
            }
            ReadFatEntity(properties);
        }


        /// <summary>
        /// Serializes the <see cref="T:System.Collections.Generic.IDictionary`2" /> of property names mapped to <see cref="T:Microsoft.WindowsAzure.Storage.Table.EntityProperty" /> data values from the entity instance.
        /// </summary>
        /// <param name="operationContext">An <see cref="T:Microsoft.WindowsAzure.Storage.OperationContext" /> object that represents the context for the current operation.</param>
        /// <returns>An <see cref="T:System.Collections.Generic.IDictionary`2" /> object of property names to <see cref="T:Microsoft.WindowsAzure.Storage.Table.EntityProperty" /> data typed values created by serializing this table entity instance.</returns>
        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var entityDictionary = WriteFatEntity(DomainObjectInstance);
            if (IndexedProperty == null)
            {
                IndexedProperty = new IndexedObject();
            }
            DomainObjectType = GetAssemblyQualifiedName();
            entityDictionary.Add(CtConstants.PropNameDomainObjectType, new EntityProperty(DomainObjectType));
            var complexTypeSerialized = JsonConvert.SerializeObject(IndexedProperty);
            if (complexTypeSerialized.Length > 63997)
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
        ///     Instance of the object that is ultimately getting saved to Table Storage
        /// </summary>
        public TDomainObject DomainObjectInstance { get; set; }

        /// <summary>
        /// The type of the Domain Object Instance
        /// </summary>
        public string DomainObjectType { get; set; }

        /// <summary>
        /// Property that is used to hold an indexed value.
        /// <para>
        /// <para>
        /// Each index definition holds a complete copy of the Domain object and can only hold one indexed property value for that 
        /// index definition.
        /// </para>
        /// The DomainObjectInstance gets serialized and spliced across 
        /// all available Table Storage property columns except one. The remaining property column holds the JSON serialized
        /// value of the current index target. This property is what goes there.
        /// </para>
        /// </summary>
        public IndexedObject IndexedProperty { get; set; } = new IndexedObject();


        /// <summary>
        ///     Sets the value of the RowKey for the table entity as a padded integer based on the difference of
        ///     the max value property of the DateTimeOffset and the DateTimeOffset.UtcNow property, followed by an
        ///     underscore and an random generated GUID.
        /// </summary>
        /// <returns></returns>
        public string SetDefaultRowKey()
        {
            var defaultRowKeyByTime = $"{DateTimeOffset.MaxValue.Ticks - DateTimeOffset.UtcNow.Ticks:d19}";
            return defaultRowKeyByTime + "_" + JsonConvert.SerializeObject(Guid.NewGuid());
        }


        /// <summary>
        ///     Sets the default value of the PartitionKey for the table entity as a random generated GUID.
        /// </summary>
        /// <returns></returns>
        public string SetDefaultPartitionKey()
        {
            var typeName = typeof(TDomainObject).Name;
            return typeName;
        }


        private string GetAssemblyQualifiedName()
        {
            var assemblyQualifiedName = DomainObjectInstance?.GetType().AssemblyQualifiedName;
            if (assemblyQualifiedName == null)
                return "";
            var typeArray = assemblyQualifiedName.Split(" ".ToCharArray());
            return typeArray[0] + " " + typeArray[1].Replace(",", "");
        }


        private void ReadFatEntity(IEnumerable<KeyValuePair<string, EntityProperty>> entityProperties)
        {
            var combinedFatEntity = new StringBuilder();
            foreach (var entityProperty in entityProperties)
            {
                if (IsNativeTableProperty(entityProperty.Key) || entityProperty.Key == CtConstants.PropNameIndexedProperty ||
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
            if (DomainObjectInstance == null)
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
            for (var i = 0; i < stringLength; i += maxStringBlockSize)
            {
                if (i + maxStringBlockSize > stringLength)
                {
                    maxStringBlockSize = stringLength - i;
                }
                var entityValue = serializedObject.Substring(i, maxStringBlockSize);
                var entityKey = $"E{dictionaryCount + 1:D2}";
                if (fatEntityDictionary.Count < 14)
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
            return propertyName == CtConstants.PropNamePartitionKey || propertyName == CtConstants.PropNameRowKey ||
                   propertyName == CtConstants.PropNameTimeStamp || propertyName == CtConstants.PropNameEtag;
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
            : base(message)
        {
            GivenObject = givenObject;
        }


        /// <summary>
        ///     The object that was too large to be stored.
        /// </summary>
        public object GivenObject { get; }
    }
}