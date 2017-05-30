using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace AzureCloudTableContext.Api
{
    /// <summary>
    ///     Class that defines a partitioning strategy to store a domain entity in Azure Table Storage.
    /// </summary>
    /// <typeparam name="TDomainObject">The POCO class that is being written to Azure Table Storage</typeparam>
    public class AzureTableIndexDefinition<TDomainObject> where TDomainObject : class, new()
    {
        private Func<TDomainObject, object> _getIndexedPropertyFromCriteria;
        private Func<TDomainObject, string> _getRowKeyFromCriteria;
        private Func<TDomainObject, bool> _indexCriteriaMethod;
        private string _indexNameKey;

        /// <summary>
        /// Constructor of a new index definition object that takes in the string name of the property that defines
        /// the ID of the domain object.
        /// </summary>
        /// <param name="nameOfIdProperty"></param>
        public AzureTableIndexDefinition(string nameOfIdProperty)
        {
            CloudTableEntities = new List<CloudTableEntity<TDomainObject>>();
            NameOfIdProperty = nameOfIdProperty;
        }

        /// <summary>
        /// Partition Key.
        /// </summary>
        public string IndexNameKey
        {
            get
            {
                if(string.IsNullOrWhiteSpace(_indexNameKey))
                {
                    _indexNameKey = typeof(TDomainObject).Name;
                }
                return _indexNameKey;
            }
        }

        /// <summary>
        /// String name of the property that defines the ID of the domain object.
        /// </summary>
        public string NameOfIdProperty { get; }

        /// <summary>
        /// Called to verify whether or not the given domain entity meets the requirements to be in the current PartitionSchema.
        /// Default is to return true.
        /// </summary>
        public Func<TDomainObject, bool> DomainObjectMatchesIndexCriteria
        {
            get { return _indexCriteriaMethod ?? (_indexCriteriaMethod = givenEntity => true); }
        }

        /// <summary>
        /// Called to set the RowKey of the CloudTableEntity prior to saving to the table.
        /// </summary>
        public Func<TDomainObject, string> GetRowKeyFromCriteria
        {
            get
            {
                if (_getRowKeyFromCriteria != null)
                    return _getRowKeyFromCriteria;
                if(!string.IsNullOrWhiteSpace(NameOfIdProperty))
                {
                    _getRowKeyFromCriteria = entity =>
                    {
                        var propInfo = typeof(TDomainObject).GetProperty(NameOfIdProperty);
                        var propValue = propInfo?.GetValue(entity);
                        return propValue != null ? JsonConvert.SerializeObject(propValue) : "";
                    };
                }
                else
                {
                    _getRowKeyFromCriteria = entity => GetChronologicalBasedRowKey();
                }
                return _getRowKeyFromCriteria;
            }
        }

        /// <summary>
        /// Called to set the IndexedProperty of the CloudTableEntity prior to saving to the table.
        /// </summary>
        public Func<TDomainObject, object> GetIndexedPropertyFromCriteria { get { return _getIndexedPropertyFromCriteria ?? (givenObj => ""); } }

        internal List<CloudTableEntity<TDomainObject>> CloudTableEntities { get; set; }

        /// <summary>
        /// A string for a row key that provides a default ordering of oldest to newest.
        /// </summary>
        /// <returns></returns>
        private static string GetChronologicalBasedRowKey()
        {
            var now = DateTimeOffset.UtcNow;
            return $"{now.Ticks:D20}_{JsonConvert.SerializeObject(Guid.NewGuid())}";
        }

        private string GetReverseChronologicalBasedRowKey()
        {
            return $"{DateTimeOffset.MaxValue.Ticks - DateTimeOffset.UtcNow.Ticks:D20}_{Guid.NewGuid()}";
        }

        /// <summary>
        /// Sets the one and only partition key related to this index.
        /// </summary>
        /// <param name="givenPartitionKey"></param>
        /// <returns></returns>
        public AzureTableIndexDefinition<TDomainObject> SetIndexNameKey(string givenPartitionKey)
        {
            _indexNameKey = givenPartitionKey;
            return this;
        }

        /// <summary>
        /// Sets the criteria that is used to determine if a given object qualifies for this index.
        /// </summary>
        /// <param name="givenCriteria"></param>
        /// <returns></returns>
        public AzureTableIndexDefinition<TDomainObject> DefineIndexCriteria(Func<TDomainObject, bool> givenCriteria)
        {
            _indexCriteriaMethod = givenCriteria;
            return this;
        }

        /// <summary>
        /// Creates a custom definition for the indexed value. This is used rarely when you want to create a one-off instance of the same object.
        /// For example, when you want to keep a history of changes to an object instance over time you could use the func like so:
        /// </summary>
        /// <para>
        /// object => CloudTableContext.GetChronologicalBasedRowKey()
        /// </para>
        /// <param name="givenRowKeyCriteria">
        /// If the RowKey will be based on an object other than a string it is best to use 
        /// the JsonConvert.SerializeObject(...) serialization method from Json.Net.
        /// </param>
        /// <returns></returns>
        public AzureTableIndexDefinition<TDomainObject> SetCustomDefinitionForIndexedValue(Func<TDomainObject, string> givenRowKeyCriteria)
        {
            _getRowKeyFromCriteria = givenRowKeyCriteria;
            return this;
        }

        /// <summary>
        /// Sets the criteria that determines which property is used as an searchable index on the Table Entity.
        /// </summary>
        /// <param name="givenIndexedPropCriteria"></param>
        /// <returns></returns>
        public AzureTableIndexDefinition<TDomainObject> SetIndexedPropertyCriteria(Func<TDomainObject, object> givenIndexedPropCriteria)
        {
            _getIndexedPropertyFromCriteria = givenIndexedPropCriteria;
            return this;
        }
    }
}