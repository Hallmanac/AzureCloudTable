namespace AzureCloudTableContext.Api
{
    /// <summary>
    /// Enum for the different types of Table write operations.
    /// </summary>
    public enum SaveType
    {
        InsertOrReplace = 0,
        InsertOrMerge = 1,
        Insert = 2,
        Replace = 3,
        Delete = 4
    }
}