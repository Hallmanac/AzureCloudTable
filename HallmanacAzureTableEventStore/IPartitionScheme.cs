namespace HallmanacAzureTable.EventStore
{
    public interface IPartitionScheme<TDomainEntity> where TDomainEntity : class, new()
    {
        string PartitionKeyName { get; }

        bool MeetsPartitionRequirements(TDomainEntity domainEntity, out AzureTableEntity<TDomainEntity> tableEntity );
    }
}