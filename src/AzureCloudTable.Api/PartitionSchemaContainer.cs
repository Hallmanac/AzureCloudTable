using System.Collections.Generic;

namespace AzureCloudTable.Api
{
    public class PartitionSchemaContainer<TDomainEntity> where TDomainEntity : class, new()
    {
        public PartitionSchemaContainer()
        {
            SchemaDefinition = new PartitionSchemaDefinition<TDomainEntity>();
        }
        
        public PartitionSchemaDefinition<TDomainEntity> SchemaDefinition { get; set; }  
        public List<CloudTableEntity<TDomainEntity>> CloudTableEntities { get; set; }
    }
}