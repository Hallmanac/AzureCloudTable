using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace HallmanacAzureTable.EventStore
{
    public class TableEntityContext<TDomainObject> where TDomainObject : class, new()
    {
        private CloudBlobContainer _blobContainer;
        private CloudBlockBlob _serializedJsonDomainObject;
        private readonly string _rootEntityTableName = string.Format("{0}Table", typeof(TDomainObject).Name);
        private readonly string _rootEntityBlobContainerName = string.Format("{0}Container", typeof(TDomainObject).Name);

        private Dictionary<string, AzureTableContext<PartitionedProperty>> _partitionedProperties;

        public string EntityContextId { get; set; }
        public TDomainObject DomainObject { get; set; }

        public TableEntityContext(string entityContextId, TDomainObject domainObject)
        {
            string connString = ConfigurationManager.ConnectionStrings["EventStoreConnectionString"].ConnectionString;
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connString);
            EntityContextId = entityContextId;
            DomainObject = domainObject;
            Init(storageAccount);
        }

        public TableEntityContext(string entityContextId, TDomainObject domainObject, string connectionString)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            EntityContextId = entityContextId;
            DomainObject = domainObject;
            Init(storageAccount);
        }

        private void Init(CloudStorageAccount storageAccount)
        {
            var blobClient = storageAccount.CreateCloudBlobClient();
            _blobContainer = blobClient.GetContainerReference(_rootEntityBlobContainerName);
            _blobContainer.CreateIfNotExists();
            _serializedJsonDomainObject = _blobContainer.GetBlockBlobReference(EntityContextId);
            var stringToUpload = "My String to get uploaded.";
            
            using(var memStream = stringToUpload.ToStream())
            {
                _serializedJsonDomainObject.UploadFromStream(memStream);

            }

            using(var memoryStream = new MemoryStream())
            {
                _serializedJsonDomainObject.DownloadToStream(memoryStream);
                string newString = memoryStream.WriteToUtf8String();

            }
        }
    }
}