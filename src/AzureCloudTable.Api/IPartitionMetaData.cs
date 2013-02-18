using System.Collections.Generic;

namespace AzureCloudTableContext.Api
{
    public interface IPartitionMetaData {
        /// <summary>
        /// List of PartitionKey(s) that are in the current Azure Table
        /// </summary>
        List<string> PartitionKeys { get; set; }
    }
}