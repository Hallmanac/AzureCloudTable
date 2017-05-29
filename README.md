Update (2014-08-03)
===================
Okay, I just came back to this after 9 months and, wow, is this ever one of those "what were you thinking" moments of trying to be a consumer of an API you wrote. I will definitely be coming back to this in the future when I have a bit more time to refactor this into something a bit more usable. In the meantime, for those brave souls...

*To use the beta version through Nuget, type the following in the Package Manager Console:*

     Install-Package AzureCloudTableContext -Pre

AzureCloudTable
===============

A wrapper around interacting with Azure Table Storage SDK 2.0. In short it enables the ability for POCOs to NOT have to inherit from TableEntity and allows for secondary indexes to be defined via a contextually managed set of partition keys. It handles batch operations automatically so there's no need to break up commits or saves.

Inside some kind of Domain repository:

     public class UserRepository
    {
        private CloudTableContext<User> _userContext;
        private PartitionSchema<User> _userFirstNamePartition;
        
        private void SomeInitializeMethod()
        {
            _userContext = new CloudTableContext<User>(givenStorageAccount, "UserId", "SomeTableName");
            
            _userFirstNamePartition = _userContext.CreatePartitionSchema("FirstNamePartitionKey")
                .SetRowKeyCriteria(userInstance => userInstance.UserId)
                .SetIndexedPropertyCriteria(userInstance => userInstance.FirstName)
                .SetSchemaCriteria(userInstance => !(string.IsNullOrWhiteSpace(userInstance.FirstName)));

            _userContext.AddPartitionSchema(_userFirstNamePartition);
        }
        
        public void SaveUser(User givenUser)
        {
            _userContext.InsertOrReplace(givenUser);
        }

        public List<User> GetUsersByFirstName(string givenFirstName)
        {
            return _userContext.GetByIndexedProperty(_userFirstNamePartition.PartitionKey, givenFirstName).ToList();
        }
    }

This library allows one to use a POCO against table storage following the "Fat Entity" philosophy that Lokad.Cqrs developed (http://code.google.com/p/lokad-cloud/wiki/FatEntities) which, among other things, is a "table-per-type" approach to Azure Table Storage.

Basically, with the added concept of "Partition Schemas", the object is getting validated against a set of criteria prior to being saved to the table. If the object matches the criteria of the partition schema (or schemas), then a duplicate object is created and saved with that particular partition key.

The best way to see these things in action is via the "Sample" console application that is in a folder of the same name.

=================================
TableAccessContext <TAzureTableEntity> Class
=================================
This class is used at a deeper level of the API and isn't necessarily used "out of the box". None the less...

This class takes the most commonly used read/write commands against Azure Table Storage and wraps it up in a more easily consumable manner from client code. The *TAzureTableEntity* generic type is required to implement ITableEntity and have a parameterless constructor (like all TableEntity types). 

It handles batch table operations by insuring that the batch operation meets the "Entity Group Transaction" requirements set forth by the Azure Table Storage SDK (i.e. not larger than 4MB, no more than 100 in a transaction, etc.) It does this for you automagically so you can hand it any size array of *TAzureTableEntity* types and rest assured that it will "just work". It takes care of breaking up the array into manageable size batch operations as needed. 

==============================
Extension Methods
==============================
There are three extension methods that are provided with this library. The first one is, perhaps, the most important one.

     public static string GetPropertyName<TProperty>(this object theObject, Expression<Func<TProperty>> propertyLambda)
               // This does just what it says: gets the name of a property. Use it like so:
               
               var myPropertyName = this.GetPropertyName(()=> someObjectInstance.SomeProperty);
               //OR
               var myPropertyName = this.GetPropertyName(()=> SomeStaticClass.SomeProperty);
               
This allows the client code to leverage the name of a Property without having "magic strings" floating around.

The other two are less important in the context of this library but I thought they were useful none the less so I included them. They are as follows:

     public static Stream ToStream(this string theString)
          - Encodes a string to a UTF8 encoded Byte[] and converts it to a memory stream.
     
     public static string WriteToUtf8String(this MemoryStream stream)
          - Converts a MemoryStream to a UTF8 encoded string.
          
          
===========================
Feedback
===========================
Hopefully this is useful to someone other than myself, otherwise this README is for naught. :-)

Please provide feedback as you see fit. Or submit a pull request. I have yet to deal with those personally so bear with me if I struggle through that as it will most likely be my first time. :-)
               


