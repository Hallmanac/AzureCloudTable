namespace AzureCloudTableContext.Api
{
    /// <summary>
    /// Wraps the value being indexed in a CloudTableEntity.
    /// </summary>
    public class IndexedObject 
    {
        public IndexedObject()
        {
            ValueBeingIndexed = null;
        }
        
        /// <summary>
        /// Object being indexed.
        /// </summary>
        public object ValueBeingIndexed { get; set; }
    }
}