using System;
using System.Collections.Generic;
using ServiceStack.Text;

namespace AzureCloudTableContext.Api
{
    /// <summary>
    ///     Class that defines a partitioning strategy to store a domain entity in Azure Table Storage.
    /// </summary>
    /// <typeparam name="TDomainObject">The POCO class that is being written to Azure Table Storage</typeparam>
    public class PartitionSchema<TDomainObject> where TDomainObject : class, new()
    {
        private Func<TDomainObject, bool> _partitionCriteriaMethod;
        private Func<TDomainObject, string> _getRowKeyFromCriteria;
        private Func<TDomainObject, object> _getIndexedPropertyFromCriteria;

        /// <summary>
        /// Partition Key.
        /// </summary>
        public string PartitionKey { get; private set; }
        
        /// <summary>
        /// Called to verify whether or not the given domain entity meets the requirements to be in the current PartitionSchema.
        /// Default is to return true.
        /// </summary>
        public Func<TDomainObject, bool> CheckAgainstPartitionCriteria
        {
            get
            {
                return _partitionCriteriaMethod ?? (_partitionCriteriaMethod = givenEntity => true);
            }
        }

        /// <summary>
        /// Called to set the RowKey of the CloudTableEntity prior to saving to the table.
        /// </summary>
        public Func<TDomainObject, string> GetRowKeyFromCriteria
        {
            get
            {
                return _getRowKeyFromCriteria ?? (_getRowKeyFromCriteria = givenObj => SetChronologicalBasedRowKey());
            }
        }

        /// <summary>
        /// Called to set the IndexedProperty of the CloudTableEntity prior to saving to the table.
        /// </summary>
        public Func<TDomainObject, object> GetIndexedPropertyFromCriteria
        {
            get
            {
                return _getIndexedPropertyFromCriteria ?? (givenObj => "");
            }
        }

        /// <summary>
        /// A string for a row key that provides a default ordering of oldest to newest.
        /// </summary>
        /// <returns></returns>
        public string SetChronologicalBasedRowKey()
        {
            return string.Format("{0:D20}_{1}", (DateTimeOffset.Now.Ticks), Guid.NewGuid().ToJsv());
        }

        /// <summary>
        /// A Row key that can be used for an ordering of newest to oldest.
        /// </summary>
        /// <returns></returns>
        public string SetReverseChronologicalBasedRowKey()
        {
            return string.Format("{0:D20}_{1}", (DateTimeOffset.MaxValue.Ticks - DateTimeOffset.Now.Ticks),
                                 Guid.NewGuid());
        }

        public PartitionSchema<TDomainObject> SetPartitionKey(string givenPartitionKey)
        {
            PartitionKey = givenPartitionKey;
            return this;
        }

        /// <summary>
        /// Sets the criteria that is used to determine if a given object qualifies for this partition scheme.
        /// </summary>
        /// <param name="givenCriteria"></param>
        /// <returns></returns>
        public PartitionSchema<TDomainObject> SetSchemaPredicateCriteria(Func<TDomainObject, bool> givenCriteria )
        {
            _partitionCriteriaMethod = givenCriteria;
            return this;
        }

        /// <summary>
        /// Sets the criteria that determines the RowKey.
        /// </summary>
        /// <param name="givenRowKeyCriteria">If the RowKey will be based on an object other than a string
        /// it is best to use the ToJsv() serialization method from ServiceStack.Text library.</param>
        /// <returns></returns>
        public PartitionSchema<TDomainObject> SetRowKeyCriteria(Func<TDomainObject, string> givenRowKeyCriteria )
        {
            // Need to convert the object provided in the Func to a JSV string.
            _getRowKeyFromCriteria = givenRowKeyCriteria;
            return this;
        }

        /// <summary>
        /// Sets the criteria that determines which property is used as an searchable index on the Table Entity.
        /// </summary>
        /// <param name="givenIndexedPropCriteria"></param>
        /// <returns></returns>
        public PartitionSchema<TDomainObject>  SetIndexedPropertyCriteria(Func<TDomainObject, object> givenIndexedPropCriteria )
        {
            _getIndexedPropertyFromCriteria = givenIndexedPropCriteria;
            return this;
        }
    }
}