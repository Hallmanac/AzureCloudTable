using System;
using System.Collections.Generic;

namespace AzureCloudTable.Api
{
    public class PartitionMetaData
    {
        public List<String> PartitionSchemaNames { get; set; } 

        public PartitionMetaData()
        {
            PartitionSchemaNames = new List<String>();
        }
    }
}