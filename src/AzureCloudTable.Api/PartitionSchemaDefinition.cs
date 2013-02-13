using System.Collections.Generic;

namespace AzureCloudTable.Api
{
    public class PartitionSchemaDefinition<TDomainEntity> where TDomainEntity : class, new()
    {
        public PartitionSchemaDefinition()
        {
            SchemaName = "DefaultSchemaName";
        } 
        
        public string SchemaName { get; set; }

        public string SetPartitionKey(TDomainEntity entity)
        {
            return SchemaName;
        }
        public virtual bool ValidationMethod(TDomainEntity entity)
        {
            return false;
        }
        public virtual string SetRowKeyValue(TDomainEntity entity)
        {
            return "DefaultRowKey";
        }
        protected virtual object SetIndexedProperty(TDomainEntity entity)
        {
            return "DefaultIndexedPropertyValue";
        }
    }
}