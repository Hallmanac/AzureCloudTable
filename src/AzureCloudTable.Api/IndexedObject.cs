namespace Hallmanac.AzureCloudTable.API
{
    /// <summary>
    /// Wraps the value being indexed in a CloudTableEntity.
    /// 
    /// <para>
    /// In order for the <see cref="CloudTableEntity{TDomainObject}"/> to always be able to properly serialize the 
    /// value of a property that is to be indexed, we have to wrap it inside a class. That's what this class is for.
    /// </para>
    /// </summary>
    public class IndexedObject 
    {
        /// <summary>
        /// Class to hold the indexed object.
        /// </summary>
        public IndexedObject()
        {
            ValueBeingIndexed = null;
        }


        /// <summary>
        /// Secondary constructor to allow for the <see cref="ValueBeingIndexed"/> property to be set 
        /// from the constructor
        /// </summary>
        /// <param name="valueBeingIndexed">The value being indexed</param>
        public IndexedObject(object valueBeingIndexed)
        {
            ValueBeingIndexed = valueBeingIndexed;
        }
        
        /// <summary>
        /// Object being indexed.
        /// </summary>
        public object ValueBeingIndexed { get; set; }
    }
}