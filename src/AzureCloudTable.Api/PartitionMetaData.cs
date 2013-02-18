using System;
using System.Collections.Generic;

namespace AzureCloudTableContext.Api
{
    /// <summary>
    /// Used in the CloudTableContext class internally to wrap the list of PartitionKeys
    /// that are used in an Azure Table.
    /// </summary>
    public class PartitionMetaData 
    {
        /// <summary>
        /// List of PartitionKey(s) that are in the current Azure Table
        /// </summary>
        public List<String> PartitionKeys { get; set; } 

        public PartitionMetaData()
        {
            PartitionKeys = new List<String>();
        }
    }
}