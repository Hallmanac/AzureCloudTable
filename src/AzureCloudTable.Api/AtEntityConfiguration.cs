namespace AzureCloudTableContext.Api
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq.Expressions;

    public class AtEntityConfiguration<TDomainEntity> where TDomainEntity : class, new()
    {
        public List<string>  IndexedPropertyItemNames { get; set; }
        public List<string> IndexedPropertyCollectionNames { get; set; } 

        public List<string> CustomIndexNames { get; set; } 
        
        public List<string> PartitionKeysInTable { get; private set; }
        public List<UniqueValueIndex<TDomainEntity>> PartitionIndices { get; set; }
        public UniqueValueIndex<TDomainEntity> DefaultIndex { get; private set; } 
        public readonly string IdPropertyName;

        public void PropertyItemHasIndex<TPropertyItem>(Expression<Func<TDomainEntity, TPropertyItem>> propertyExpression)
        {
            var memberExpression = propertyExpression.Body as MemberExpression;
            if(memberExpression == null)
            {
                Trace.WriteLine(string.Format("PropertyItemHasIndex failed due to the memberExpression being null for {0}.", propertyExpression));
                return;
            }
            var propertyName = memberExpression.Member.Name;
            var propertyType = memberExpression.Member.ReflectedType;
        }

        public void PropertyCollectionHasIndex<TPropertyCollection>(Expression<Func<TDomainEntity, TPropertyCollection>> propertyExpression) where TPropertyCollection : IEnumerable<object> 
        {
            
        }

        public void PropertyIsEntityId<TPropertyItem>(Expression<Func<TDomainEntity, TPropertyItem>> propertyExpression)
        {
            var memberExpression = propertyExpression.Body as MemberExpression;
            if(memberExpression == null)
            {
                Trace.WriteLine(string.Format("PropertyIsEntityId failed due to the memberExpression being null for {0}", propertyExpression));
                return;
            }
            var propertyName = memberExpression.Member.Name;
        }

        public UniqueValueIndex<TDomainEntity> CreateCustomIndex(string indexName)
        {
            var partitionIndex = new UniqueValueIndex<TDomainEntity>(indexName);
            return partitionIndex;
        }
    }
}