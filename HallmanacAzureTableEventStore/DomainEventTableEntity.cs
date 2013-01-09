using System;
using Microsoft.WindowsAzure.Storage.Table;
using SimpleCqrs.Eventing;

namespace HallmanacAzureTable.EventStore
{
    public class DomainEventTableEntity : TableEntity
    {
        public string EventType { get; set; }
        public Guid AggregateRootId { get; set; }
        public string Data { get; set; }
    }
}