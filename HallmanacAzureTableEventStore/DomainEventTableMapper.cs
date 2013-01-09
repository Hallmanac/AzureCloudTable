using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using ServiceStack.Text;
using SimpleCqrs.Eventing;

namespace HallmanacAzureTable.EventStore
{
    public class DomainEventTableMapper : IEntityTableMapper<DomainEvent, DomainEventTableEntity>
    {
        private string _rootEntityTableName;
        
        public static int RowKeyPaddingValue
        {
            get
            {
                return 19;
            }
        }
        public static string RowKeyNumberPaddingFormatter
        {
            get { return string.Format("d{0}", RowKeyPaddingValue.ToString()); }
        }

        public string RootEntityTableName
        {
            get
            {
                if(!string.IsNullOrWhiteSpace(_rootEntityTableName))
                    _rootEntityTableName = string.Format("{0}s", typeof(DomainEvent).Name);
                return _rootEntityTableName;
            }
        }

        public AzureTableContext<DomainEventTableEntity> RootEntityContext { get; private set; }

        public Dictionary<string, AzureTableContext<PartitionedProperty>> IndexedProperties { get; private set; }

        public DomainEventTableMapper(CloudStorageAccount storageAccount)
        {
            RootEntityContext = new AzureTableContext<DomainEventTableEntity>(storageAccount, RootEntityTableName);
        }

        public DomainEvent MapFromTable(DomainEventTableEntity tableEntity)
        {
            var serializer = new JsonSerializer<DomainEvent>();
            DomainEvent domainEvent = serializer.DeserializeFromString(tableEntity.Data);
            domainEvent.EventDate = tableEntity.Timestamp.UtcDateTime;
            domainEvent.AggregateRootId = tableEntity.AggregateRootId;
            domainEvent.Sequence = GetSequenceFromRowKey(tableEntity);
            return domainEvent;
        }

        public DomainEventTableEntity MapToTable(DomainEvent domainObject)
        {
            var domainEventEntity = new DomainEventTableEntity
                                        {
                                                Data = domainObject.SerializeToString(),
                                                EventType = domainObject.GetType().Name,
                                                PartitionKey = domainObject.AggregateRootId.ToString(),
                                                RowKey = GetRowKeyFromSequence(domainObject),
                                                AggregateRootId = domainObject.AggregateRootId
                                        };
            return domainEventEntity;
        }

        public string CreatePartitionKeyFromProperty(DomainEvent domainObject, string propertyName)
        {
            string aggregateIdPropertyName = this.GetPropertyName(() => domainObject.AggregateRootId);
            if(propertyName != aggregateIdPropertyName)
            {
                return default(Guid).ToString();
            }
            string propertyValue = domainObject.GetType().GetProperty(propertyName).GetValue(domainObject).ToString();
            return propertyValue;
        }

        public string CreateRowKeyFromProperty(DomainEvent domainObject, string propertyName)
        {
            string domainPropertyName = this.GetPropertyName(() => domainObject.Sequence);
            if(domainPropertyName != propertyName)
                return default(int).ToString(RowKeyNumberPaddingFormatter);
            return domainObject.Sequence.ToString(RowKeyNumberPaddingFormatter);
        }
        
        public void CreateIndexedProperty(CloudStorageAccount storageAccount, DomainEvent domainObject, string propertyName)
        {
            var indexedProperty = new PartitionedProperty();
            var serializer = new JsonSerializer<DomainEvent>();
            var propInfo = domainObject.GetType().GetProperty(propertyName);
            var serializedPropValue = propInfo.GetValue(domainObject).ToJsv().Replace(" ", "_");
            indexedProperty.AggregateRootData = serializer.SerializeToString(domainObject);
            indexedProperty.JsvSerializedPropertyValue = serializedPropValue;
            indexedProperty.PartitionKey = serializedPropValue;
            indexedProperty.RowKey = domainObject.AggregateRootId.ToJsv();
            var tableName = string.Format("{0}_{1}s", typeof(DomainEvent).Name, propertyName);
            var azureTableContext = new AzureTableContext<PartitionedProperty>(storageAccount, tableName);
            IndexedProperties.Add(propertyName, azureTableContext);
        }

        /// <summary>
        ///     This method returns a string that contains the current time in "Ticks" with a Guid appended to it
        ///     to insure uniqueness of the RowKey.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        private string GetRowKeyFromSequence(DomainEvent entity)
        {
            if(entity == null)
                throw new ArgumentNullException(
                        string.Format("DomainEvent was null when trying to get the RowKey from the Sequence"));
            string rowKey;
            if(entity.Sequence == 0)
            {
                rowKey = DateTimeOffset.Now.Ticks.ToString(RowKeyNumberPaddingFormatter) + Guid.NewGuid().ToString();
                return rowKey;
            }
            rowKey = entity.Sequence.ToString(RowKeyNumberPaddingFormatter) + Guid.NewGuid().ToString();
            return rowKey;
        }

        /// <summary>
        ///     Returns an int that rips out the padded zeros and appended Guid of the RowKey.
        /// </summary>
        /// <param name="tableTableEntity"></param>
        /// <returns></returns>
        private int GetSequenceFromRowKey(DomainEventTableEntity tableTableEntity)
        {
            if(tableTableEntity == null) throw new ArgumentNullException("tableTableEntity");
            string rowKeyMinusGuid = tableTableEntity.RowKey.Substring(0, RowKeyPaddingValue);
            string trimmedRowKey = rowKeyMinusGuid.TrimStart('0');
            return Convert.ToInt32(trimmedRowKey);
        }
    }
}