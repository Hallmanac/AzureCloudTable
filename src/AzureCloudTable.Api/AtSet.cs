namespace AzureCloudTableContext.Api
{
    using System;
    using System.Diagnostics;
    using System.Linq.Expressions;
    using ServiceStack.Text;

    public class AtSet<TDomainEntity> where TDomainEntity : class, new()
    {
         public AtEntityConfiguration<TDomainEntity> Configuration { get; set; }

        public TableQueryBuilder WherePropertyItem<TProperty>(Expression<Func<TDomainEntity, TProperty>> propertyExpression)
        {
            var memberExpression = propertyExpression.Body as MemberExpression;
            if (memberExpression == null)
            {
                Trace.WriteLine(string.Format("PropertyIsEntityId failed due to the memberExpression being null for {0}", propertyExpression));
                throw new Exception(string.Format("PropertyIsEntityId failed due to the memberExpression being null for {0}", propertyExpression));
            }
            var propertyName = memberExpression.Member.Name;
            var tqb = new TableQueryBuilder(AtConstants.PropertyItemIndexPrefix);

        }
    }

    internal class TableQueryBuilder
    {
        private string _partitionKey;
        private string _indexOperator;
        private string _indexTypePrefix;

        public TableQueryBuilder(string indexTypePrefix)
        {
            _indexTypePrefix = indexTypePrefix;
        }

        public TableQueryBuilder IsLessThanOrEqualTo(object val)
        {
            _indexOperator = AtConstants.OperatorLessThanOrEqualTo;
            return this;
        }

        public TableQueryBuilder IsEqualTo(object val)
        {
            _indexOperator = AtConstants.OperatorEqualTo;
            return this;
        }

        public TableQueryBuilder CompileQuery()
        {
            _partitionKey = "";
            return this;
        }
    }
}