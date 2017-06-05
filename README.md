## Get It

```
	Install-Package Hallmanac.AzureCloudTable
```

## Description

This library is a wrapper around interacting with Azure Table Storage SDK. In short it enables the ability for POCOs to NOT have to inherit from TableEntity and allows for secondary indexes to be defined via a contextually managed set of partition keys. It handles batch operations automatically so there's no need to break up commits or saves.

This library allows one to use a POCO against table storage following the "Fat Entity" philosophy that Lokad.Cqrs developed (http://code.google.com/p/lokad-cloud/wiki/FatEntities) which, among other things, is a "table-per-type" approach to Azure Table Storage.

Basically, with the added concept of "Index Definitions", the object is getting validated against a set of criteria prior to being saved to the table. If the object matches the criteria of a single Index Definition (or set of them), then a duplicate object is created and saved with that particular Index Definition Name.

The name of the index is what eventually becomes the Azure Table `PartitionKey`. The `RowKey` is derived from the value of the declared `ID` property on the wrapped object type (i.e. `User.UserID` or `User.Id`). The `RowKey` can also be custom defined.

The best way to see these things in action is via the "Sample" console application that is in a folder of the same name.

## Usage

Using this library can be focused around the use of the `TableContext<TDomainObject>` class. If you want to do more than "search by ID" then you can add Indexes using the `TableIndexDefinition<TDomainObject>` class. You can add indexes directly from the `TableContext` class methods using a Fluent API style as seen below.

Inside some kind of Domain repository:
```
    public class UserRepository
    {
	  //Primary wrapper around saving and querying the User object against Azure Table Storage. This could be made a public
		//property to allow for consumers of this "repository" class to get more advanced querying capabilities
		private TableContext<User> UserContext;

		// An index on the FirstName property. This gets set below
       private TableIndexDefinition<User> _firstNameIndex;

		public UserRepository()
       {
           var connectionString = CloudConfigurationManager.GetSetting("AzureStorageConnectionString");
           var storageAccount = CloudStorageAccount.Parse(connectionString);
           UserContext = new TableContext<User>(storageAccount, "UserId");

           InitIndexDefinitions();
       }


		private void InitIndexDefinitions()
       {
           _firstNameIndex = UserContext.CreateIndexDefinition()
                                        .SetIndexNameKey("FirstNameIdx")
                                        .DefineIndexCriteria(user => user != null)
                                        .SetIndexedPropertyCriteria(user => user?.FirstName);

           // Now add the index definitions that were just created to the TableContext<User> instance (i.e. UserContext).
           UserContext.AddMultipleIndexDefinitions(new List<TableIndexDefinition<User>>
           {
               _firstNameIndex
           });
       }

		public List<User> GetAllUsers()
       {
           return UserContext.GetAll().ToList();
       }

		public List<User> GetUsersByFirstName(string firstName)
       {
           return UserContext.GetByIndexedProperty(_firstNameIndex.IndexNameKey, firstName).ToList();
       }

		
		public void Save(User user)
       {
           UserContext.InsertOrReplace(user);
       }
    }
```

## TableOperations<TAzureTableEntity> Class

This class is used at a deeper level of the API and isn't necessarily used "out of the box". None the less...

This class takes the most commonly used read/write commands against Azure Table Storage and wraps it up in a more consumable manner from client code. The *TAzureTableEntity* generic type is required to implement ITableEntity and have a parameterless constructor (like all TableEntity types). 

It handles batch table operations by insuring that the batch operation meets the "Entity Group Transaction" requirements set forth by the Azure Table Storage SDK (i.e. not larger than 4MB, no more than 100 in a transaction, etc.) It does this for you automagically so you can hand it any size array of *TAzureTableEntity* types and rest assured that it will "just work". It takes care of breaking up the array into manageable size batch operations as needed. 

## Extension Methods

There are three extension methods that are provided with this library. The first one is, perhaps, the most useful.
```
     public static string GetPropertyName<TProperty>(this object theObject, Expression<Func<TProperty>> propertyLambda)
    // This does just what it says: gets the name of a property. Use it like so:
               
    var myPropertyName = this.GetPropertyName(()=> someObjectInstance.SomeProperty);
    //OR
    var myPropertyName = this.GetPropertyName(()=> SomeStaticClass.SomeProperty);
```            
This allows the client code to leverage the name of a Property without having "magic strings" floating around.

The other two are less important in the context of this library but I thought they were useful none the less so I included them. They are as follows:

     public static Stream ToStream(this string theString)
          - Encodes a string to a UTF8 encoded Byte[] and converts it to a memory stream.
     
     public static string WriteToUtf8String(this MemoryStream stream)
          - Converts a MemoryStream to a UTF8 encoded string.
          
## Feedback

Hopefully this is useful to someone other than myself, otherwise this README is for naught. :-)

Please provide feedback via the issues on this repo. Or submit a pull request.

## Update (2017-06-04)

This library has been refactored to provide a more intuitive API. This was a result of dog-fooding this library a few times since it's creation and realizing each time that, even though I wrote it, it was difficult to remember/understand intent or some of the concepts behind the methods and classes. Mostly due to what I would say is the naming of everything.

Most of the refactoring is renaming classes and methods. Due to the large amount of breaking changes and the fact that this library was in Beta up until now, I decided to create a new NuGet package to align with the naming convention of the greater "Hallmanac" set of NuGet packages out on the NuGets.

The original library (Azure Cloud Table Context) will not be updated by me any longer. I have created a separate branch on this repo in case I or anyone else wants to continue to update it. The branch is called `legacy-azure-cloud-table-context`.

*To use the original Azure Cloud Table Context beta version through Nuget, type the following in the Package Manager Console:*

     Install-Package AzureCloudTableContext -Pre
