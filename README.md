*To use the beta (v1.0.0) version through Nuget, type the following in the Package Manager Console:*

     Install-Package AzureCloudTableStorage -Pre
     
*The Nuget package is in Pre-release so it won't be searchable from the "Managage Nuget Packages" dialog.*

AzureCloudTable
===============

A wrapper around interacting with Azure Table Storage. In short it enables the ability for POCOs to NOT have to inherit from TableEntity and allows for secondary indexes to be defined via a contextually managed set of partition keys.

This library is a wrapper around the Azure Storage SDK 2.0. It allows one to use a POCO against table storage following the "Fat Entity" philosophy that Lokad.Cqrs developed (http://code.google.com/p/lokad-cloud/wiki/FatEntities) which, among other things, is a "table-per-type" approach to Azure Table Storage. This makes a lot of sense for denormalized data structures or, even better, for a DDD (Domain Driven Design) architecture where each aggregate root type has a dedicated table. There are a few things this library does differently though.

There are two main parts that client code will be dealing with:

     1) CloudTableContext<TDomainObject> where TDomainObject : class, new()
        a) This class is used to maanage the wrapped POCOs as well as the basic CRUD operations against the Azure table.
        
     2) PartitionSchema<TDomainObject> where TDomainEntity : class, new()
          a) This class is used in client code to define a partitioning strategy
          b) It is created and then passed into the CloudTableContext instance.

=====================================
PartitionSchema<TDomainObject> Class
=====================================

This class is perhaps the most confusing aspect of this library, but it's essential for using this library as it was intended. In the client code (perhaps a repository of some kind - or not) there would be one or more instances defined. This class has 5 public properties that are crucial:
     
     string SchemaName --> Required - no defaults...
                           This defines the name of the SCHEMA only. Think of it as an instance ID.
     
     Func<TDomainObject, string> SetPartitionKey --> Optional - defaults to the SchemaName...
                                                     A delegate that defines how the PartitionKey is determined.
                                                     In most cases, it will simply refer to the SchemaName property
                                                     and, in fact, the default for this property is just that.
                                                     
     Func<TDomainObject, bool> ValidateEntityForPartition --> Optional - returns false by default...
                                                              Delegate that is used to determine if a given object
                                                              qualifies to be in this particular PartitionSchema.
                                                              
     Func<TDomainObject, string> SetRowKey --> Optional - defaults to the "Id" property of the POCO...
                                               Just like the "SetPartitionKey" this delegate defines how the 
                                               PartitionKey is determined. The default is to fall back on the 
                                               "Id" property of the given POCO (which is defined in the
                                               CloudTableContext class).
                                               
     Func<TDomainObject, object> SetIndexedProperty --> Optional - defaults to null...
                                                        A delegate that defines a value that will be indexed inside
                                                        the Azure table.
                                                        
The four Func delegates get called in the CloudTableContext class (under the hood) just prior to being saved to the table, using the POCO that gets passed in for saving as the "TDomainObject".

==================================
CloudTableContext<TDomainEntity> Class
==================================
This class is where the "magic happens" and where you'll be doing most of the interaction with this library. The main feature that this library has (which is located in this class) is that it allows for the client to create secondary indices (in a manner of speaking) by creating and managing additional PartitionKey(s)for the same POCO instance through the use of the *PartitionSchema* class. 

These additional PartitionKey based indices means that there is an extra copy of the POCO in the form of a Fat Entity. This means duplicated data but with the inherently cheap cost of Azure Table Storage as well as the ability to scale I believe that it's a good trade off. 

This class provides many of the typical ways to interact with Azure Table storage with the added benefit of being able to map this functionality right onto a POCO. 

When a write to an Azure Table occurs, this class is checking the object instance against the declared "PartitionSchema" instances and duplicating the object into the PartitionSchemas for which it is qualified. This keeps things consistent before the object actually goes into an Azure table. 

Since there are additional partition keys used for grouping/categorizing parts of the POCO together I created the additional "IndexedProperty" property to allow for one more level of filtering. The client code would map a property from the POCO to the "IndexedProperty" of the "CloudTableEntity<TDomainObject>" class. Then, when searching on that partition, the client would filter on the "IndexedProperty" using the *QueryWhereIndexedPropertyEquals* method.

The best way to see these things in action is via the "Sample" console application that is in a folder of the same name.

=====================================
Other Aspects of this Library
=====================================
This library provides a few other classes that are useful outside the *CloudTableContext* class which enable one to get a little closer to Azure Table Storage. All of these classes are used in the *CloudTableContext* class to one extent or another.

==================================
CloudTableEntity<TDomainObject> Class
==================================
This class does a couple of interesting things. First, it wraps a POCO into a "Fat Entity" by using the ServiceStack.Text library to serialize the object into a JSV string (JSON + CSV) and splits that across 14 Table EntityProperty types. 

This is different than the Lokad.Cqrs Fat Entity in that we're using one less Entity Property for sharing the object size. The reason for this is to leave the last Entity Property open for the "IndexedProperty" that was described above.

Basically, this class provides the mechanism to write a POCO to Azure Table Storage without having to inerit from TableEntity. This class implements it's own verion of ITableEntity under the hood to get this done.

=================================
TableReadWriteContext<TAzureTableEntity> Class
=================================
This class takes the most commonly used read/write commands against Azure Table Storage and wraps it up in a more easily consumable manner from client code. The *TAzureTableEntity* generic type is required to implement ITableEntity and have a parameterless constructor (like all TableEntity types). 

It handles batch table operations by insuring that the batch operation meets the "Entity Group Transaction" requirements set forth by the Azure Table Storage SDK (i.e. not larger than 4MB, no more than 100 in a transaction, etc.) It does this for you automagically so you can hand it any size array of *TAzureTableEntity* types and rest assured that it will "just work". It takes care of breaking up the array into manageable size batch operations as needed. 

==============================
Extension Methods
==============================
There are three extension methods that are provided with this library. The first one is, perhaps, the most important one.

     public static string GetPropertyName<TProperty>(this object theObject, Expression<Func<TProperty>> propertyLambda)
               - This does just what it says: gets the name of a property. Use it like so:
               --> var myPropertyName = this.GetPropertyName(()=> someObjectInstance.SomeProperty);
               --> var myPropertyName = this.GetPropertyName(()=> SomeStaticClass.SomeProperty);
               
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
               
I plan on putting this on Nuget very shortly (today is February 16, 2013) so be on the lookout for that if you want to use this.



