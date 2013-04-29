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
        private Func<TDomainObject, string> _setRowKey;
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
                return _setRowKey ?? (_setRowKey = givenObj => SetChronologicalBasedRowKey());
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
 
        public PartitionSchema<TDomainObject> SetSchemaPredicateCriteria(Func<TDomainObject, bool> givenCriteria )
        {
            _partitionCriteriaMethod = givenCriteria;
            return this;
        }

        public PartitionSchema<TDomainObject> SetRowKeyCriteria(Func<TDomainObject, string> givenRowKeyCriteria )
        {
            _setRowKey = givenRowKeyCriteria;
            return this;
        }

        public PartitionSchema<TDomainObject>  SetIndexedPropertyCriteria(Func<TDomainObject, object> givenIndexedPropCriteria )
        {
            _getIndexedPropertyFromCriteria = givenIndexedPropCriteria;
            return this;
        }
    }
}