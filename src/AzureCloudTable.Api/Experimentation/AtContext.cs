namespace AzureCloudTableContext.Api
{
    using System;
    using System.Collections.Concurrent;

    public class AtContext
    {
        public ConcurrentDictionary<Type, Type> MappedReferences { get; set; }
        public ConcurrentBag<PropertyItemIndex> PropertyItemIndices { get; set; }
        public ConcurrentBag<PropertyCollectionIndex> PropertyCollectionIndices { get; set; }
    }
}