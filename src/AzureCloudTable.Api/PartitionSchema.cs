using System;
using System.Collections.Generic;

namespace AzureCloudTable.Api
{
    /// <summary>
    ///     Class that defines a partitioning strategy to store a domain entity in Azure Table Storage.
    /// </summary>
    /// <typeparam name="TDomainObject">The POCO class that is being written to Azure Table Storage</typeparam>
    public class PartitionSchema<TDomainObject> where TDomainObject : class, new()
    {
        public PartitionSchema()
        {
            SchemaName = "DefaultSchemaName";
            ValidationMethod = o => false;
        } 

        /// <summary>
        ///     Creates a new PartitionScheme object.
        /// </summary>
        /// <param name="schemaName">Value that will be used as the PartitionKey in the Azure TableEntity</param>
        /// <param name="validationMethod">
        ///     Lambda expression that determines if it meets the requirements of the
        ///     partition schema. If all entities need to be in this partition simply return true. Example:
        ///     <para>entity => entity.SomeProperty == SomeValue</para>
        ///     <para>entity => true</para>
        /// </param>
        /// <param name="setPartitionKey">
        ///     Lambda expression that sets the PartitionKey for the table entity. Gives ability to map the value of an
        ///     entity property to the PartitionKey. Make sure your property will conform to PartitionKey constraints 
        ///     (i.e. no bigger than 1K, etc). The default value is that of the SchemaName. Example:
        ///     <para>entity => entity.SomeProperty.ToString()</para>
        ///     <para>entity => entity.SomeProperty.ToJsv() -- ServiceStack.Text serializer --</para>
        /// </param>
        /// <param name="setIndexedPropValue">
        ///     Lambda expression that sets the value of the Indexed Property on the Table Entity. The default
        ///     returns an empty string. Example:
        ///     <para>
        ///         <example>entity => entity.SomeProperty</example>
        ///     </para>
        /// </param>
        /// <param name="setRowKeyValue">
        ///     Lambda expression that sets the RowKey value. Gives the ability to map the value of an entity property
        ///     to the RowKey. Make sure your property will conform to RowKey constraints (i.e. no bigger than 1K, etc) and is unique.
        ///     <para>The default value for the RowKey will be the value of the entity's Id property.</para>
        /// </param>
        public PartitionSchema(string schemaName, Func<TDomainObject, bool> validationMethod,
            Func<TDomainObject, string> setPartitionKey = null, Func<TDomainObject, object> setIndexedPropValue = null,
            Func<TDomainObject, string> setRowKeyValue = null)
        {
            if(setPartitionKey != null)
                SetPartitionKey = setPartitionKey;
            else
            {
                SetPartitionKey = entity => schemaName;
            }
            SchemaName = schemaName;
            Init(validationMethod, setIndexedPropValue, setRowKeyValue);
        }

        /// <summary>
        /// Unique name of PartitionSchema.
        /// </summary>
        public string SchemaName { get; set; }
        
        /// <summary>
        /// Called to set the PartitionKey of the CloudTableEntity prior to saving to the table.
        /// </summary>
        public Func<TDomainObject, string> SetPartitionKey { get; set; }

        /// <summary>
        /// Called to verify whether or not the given domain entity meets the requirements to be in the current PartitionSchema.
        /// </summary>
        public Func<TDomainObject, bool> ValidationMethod { get; set; }

        /// <summary>
        /// Called to set the RowKey of the CloudTableEntity prior to saving to the table.
        /// </summary>
        public Func<TDomainObject, string> SetRowKeyValue { get; set; }

        /// <summary>
        /// Called to set the IndexedProperty of the CloudTableEntity prior to saving to the table.
        /// </summary>
        public Func<TDomainObject, object> SetIndexedProperty { get; set; }

        public List<CloudTableEntity<TDomainObject>> CloudTableEntities { get; set; }

        private void Init(Func<TDomainObject, bool> validationMethod, Func<TDomainObject, object> setIndexedPropValue,
            Func<TDomainObject, string> setRowKeyValue)
        {
            ValidationMethod = validationMethod;
            if(setIndexedPropValue != null)
                SetIndexedProperty = setIndexedPropValue;
            else
            {
                SetIndexedProperty = entity => null;
            }
            if(setRowKeyValue != null)
                SetRowKeyValue = setRowKeyValue;
            else
            {
                SetRowKeyValue = entity => null;
            }
            CloudTableEntities = new List<CloudTableEntity<TDomainObject>>();
        }
    }
}