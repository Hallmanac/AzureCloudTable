**2012/02/02:**
**WARNING - This isn't even 0.1 yet so the documentation is probably confusing as hell and there might be bugs. I'm starting to use this code in production for a project and will work out the kinks as I discover them.**

AzureCloudTable
===============

Wrapper around interacting with Azure Table Storage. In short it enables the ability for POCOs to NOT have to inherit from TableEntity and allows for secondary indexes to be defined via a centrally managed set of partition keys.

This library is a wrapper around the Azure Storage SDK 2.0. It allows one to use a POCO against table storage following the "Fat Entity" philosophy that Lokad.Cqrs developed (http://code.google.com/p/lokad-cloud/wiki/FatEntities). There are a few things this library does differently though.

There are two main parts:

     1) CloudTableEntity<TDomainObject> where TDomainObject : class, new()
        a) This will be used to wrap a POCO into an Azure ITableEntity.
  
     2) CloudTableContext<TDomainObject> where TDomainObject : class, new()
        a) This class is used to maanage the wrapped POCOs as well as the basic CRUD operations against the Azure table.

==================================
CloudTableEntity<TDomainObject> Class
==================================
The CloudTableEntity class does a couple of interesting things. First, to wrap a POCO into a "Fat Entity" it uses the ServiceStack.Text library to serialize the object into a JSV string (JSON + CSV) and splits that across 14 Table EntityProperty types. 

This is different than the Lokad.Cqrs Fat Entity in that we're using one less Entity Property for sharing the object size. The reason for this is to leave the last Entity Property open for something called an "IndexedProperty". We'll cover that in a bit.

==================================
CloudTableContext<TDomainEntity> Class
==================================
This class is where the "magic happens". The main feature that this library has (which is located in this class) is that it allows for the client to create secondary indexes (in a manner of speaking) through the use of creating and managing additional PartitionKey(s). 

These additional PartitionKey based indexes keep an a copy of the POCO as a Fat Entity. This means duplicated data but with the inherently cheap cost of Azure Table Storage as well as the ability to scale I believe that it's a good trade off. 

Since there are additional partition keys used for grouping/categorizing parts of the POCO together I created the additional "IndexedProperty" property to allow for one more level of filtering. The client code (through a repository pattern) would map a property from the POCO to the "IndexedProperty" of the "CloudTableEntity<TDomainObject>" class. Then, when searching on that partition, the client would filter on the "IndexedProperty".

More documentation to come...






