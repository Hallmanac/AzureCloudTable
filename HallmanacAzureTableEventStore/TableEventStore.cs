using System;
using System.Collections.Generic;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using SimpleCqrs.Eventing;

namespace HallmanacAzureTable.EventStore
{
    public class TableEventStore : IEventStore
    {
        private readonly IEntityTableMapper<DomainEvent, DomainEventTableEntity> _domainEventMapper;

        public TableEventStore(string connectionString)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            _domainEventMapper = new DomainEventTableMapper(storageAccount);
        }

        public TableEventStore()
        {
            string connString = ConfigurationManager.ConnectionStrings["EventStoreConnectionString"].ConnectionString;
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connString);
            _domainEventMapper = new DomainEventTableMapper(storageAccount);
        }

        public IEnumerable<DomainEvent> GetEvents(Guid aggregateRootId = default(Guid), int startSequence = 0)
        {
            string aggregateRootToString = aggregateRootId.ToString();
            string startSequenceToString = string.Format("{0}_{1}",
                startSequence.ToString(
                                       DomainEventTableMapper
                                           .RowKeyNumberPaddingFormatter),
                default(Guid).ToString());
            IEnumerable<DomainEventTableEntity> tableEntities = startSequence == 0
                ? _domainEventMapper.RootEntityContext
                    .GetByPartitionKey(
                                       aggregateRootToString)
                : _domainEventMapper.RootEntityContext
                    .GetByPartitionKeyWithRowKeyRange
                    (aggregateRootToString,
                        startSequenceToString);
        }

        public void Insert(IEnumerable<DomainEvent> domainEvents) { throw new NotImplementedException(); }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes) { throw new NotImplementedException(); }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, Guid aggregateRootId) { throw new NotImplementedException(); }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, DateTime startDate,
            DateTime endDate) { throw new NotImplementedException(); }
    }
}