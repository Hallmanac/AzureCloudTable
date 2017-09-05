using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hallmanac.AzureCloudTable.API
{
    /// <summary>
    /// This class provides methods for managing the table and all the metadata around it. Mostly things like
    /// indexes for now.
    /// </summary>
    public class TableManager<TDomainEntity> where TDomainEntity : class, new()
    {
        private readonly TableOperationsService<TableEntityWrapper<PartitionMetaData>> _tableMetaDataContext;
        private string _defaultIndexDefinitionName;
        private bool _needToRunTableIndices;
        private TableEntityWrapper<PartitionMetaData> _partitionMetaDataEntityWrapper;
        private readonly TableKeyEncoder _encoder = new TableKeyEncoder();


        /// <summary>
        /// Initializes a new <see cref="TableManager{TDomainEntity}"/> object.
        /// </summary>
        public TableManager(CloudStorageAccount storageAccount, string nameOfEntityIdProperty, string tableName)
        {
            if (string.IsNullOrWhiteSpace(nameOfEntityIdProperty))
                throw new ArgumentNullException(nameof(nameOfEntityIdProperty));
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));

            NameOfEntityIdProperty = nameOfEntityIdProperty;
            TableOperationsService = new TableOperationsService<TableEntityWrapper<TDomainEntity>>(storageAccount, tableName);
            _tableMetaDataContext = new TableOperationsService<TableEntityWrapper<PartitionMetaData>>(storageAccount, TableOperationsService.TableName);
        }

        /// <summary>
        /// Initializes a new CloudTableContext object.
        /// </summary>
        public TableManager(string connectionString, string nameOfEntityIdProperty, string tableName)
        {
            if (string.IsNullOrWhiteSpace(nameOfEntityIdProperty))
                throw new ArgumentNullException(nameof(nameOfEntityIdProperty));
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));

            NameOfEntityIdProperty = nameOfEntityIdProperty;
            TableOperationsService = new TableOperationsService<TableEntityWrapper<TDomainEntity>>(connectionString, tableName);
            _tableMetaDataContext = new TableOperationsService<TableEntityWrapper<PartitionMetaData>>(connectionString, TableOperationsService.TableName);
        }


        /// <summary>
        /// Gives direct access to the underlying TableOperationsService class that does the interaction with the Azure Table.
        /// </summary>
        public TableOperationsService<TableEntityWrapper<TDomainEntity>> TableOperationsService { get; }


        /// <summary>
        /// This is the name of the property that is used to store the ID of the Domain Entity.
        /// <para>
        /// For example, if there is a domain entity of type User that has a property named "Id" then one would pass
        /// the name of that property ("Id") into the constructor of the CloudTableContext class.
        /// </para>
        /// <para>This could be done using the extension method (on Object) called "GetPropertyName"</para>
        /// </summary>
        public string NameOfEntityIdProperty { get; }

        /// <summary>
        /// Gets a list of the index name keys that are used in the table.
        /// </summary>
        public List<string> IndexNameKeysInTable { get; } = new List<string>();

        /// <summary>
        /// Gets the default index definition used for the table.
        /// </summary>
        public TableIndexDefinition<TDomainEntity> DefaultIndex { get; private set; }

        /// <summary>
        /// Runtime list of active partition schemas.
        /// </summary>
        public List<TableIndexDefinition<TDomainEntity>> IndexDefinitions { get; set; } = new List<TableIndexDefinition<TDomainEntity>>();


        /// <summary>
        /// This will load the metadata for a given table. It makes sure that we have a default index, and it populates the index names
        /// for the table into the local <see cref="IndexNameKeysInTable"/> property
        /// </summary>
        /// <returns></returns>
        public async Task LoadMetaDataAsync()
        {
            // Try to load the partition meta data from the existing table (which contains a list of the partition keys in the table).
            _partitionMetaDataEntityWrapper = await _tableMetaDataContext.FindAsync(CtConstants.TableMetaDataPartitionKey, CtConstants.PartitionSchemasRowKey).ConfigureAwait(false);

            // Set the default PartitionKey using the combination below in case there are more than one CloudTableContext objects
            // on the same table.
            _defaultIndexDefinitionName = $"DefaultIndex_ofType_{typeof(TDomainEntity).Name}";
            if (_partitionMetaDataEntityWrapper != null)
            {
                /* This is going through and populating the local PartitionKeysInTable property with the list of keys retrieved
                 * from the Azure table.
                 * This also checks to see if there is a PartitionKey for the table meta data and the DefaultPartition
                 * and adds that if there isn't*/
                var metaDataPkIsInList = false;
                foreach (var partitionKeyString in _partitionMetaDataEntityWrapper.DomainObjectInstance.PartitionKeys)
                {
                    if (partitionKeyString == CtConstants.TableMetaDataPartitionKey)
                    {
                        metaDataPkIsInList = true;
                    }
                    var isInList = IndexNameKeysInTable.Contains(partitionKeyString);

                    if (!isInList)
                    {
                        IndexNameKeysInTable.Add(partitionKeyString);
                    }
                }
                if (!metaDataPkIsInList)
                {
                    IndexNameKeysInTable.Add(CtConstants.TableMetaDataPartitionKey);
                }

                // The RowKey for the DefaultSchema is set by the given ID property of the TDomainEntity object
                DefaultIndex = CreateIndexDefinition(_defaultIndexDefinitionName)
                    .DefineIndexCriteria(entity => true)
                    .SetIndexedPropertyCriteria(entity => entity.GetType().Name); // Enables searching directly on the type.
                if (IndexDefinitions.All(indexDefinition => indexDefinition.IndexNameKey != DefaultIndex.IndexNameKey))
                {
                    AddIndexDefinition(DefaultIndex);
                }
            }
            else
            {
                /* Creates a new partition meta data entity and adds the appropriate default partitions and metadata partitions*/
                _partitionMetaDataEntityWrapper = new TableEntityWrapper<PartitionMetaData>(CtConstants.TableMetaDataPartitionKey, CtConstants.PartitionSchemasRowKey);
                DefaultIndex = CreateIndexDefinition(_defaultIndexDefinitionName)
                    .DefineIndexCriteria(entity => true)
                    .SetIndexedPropertyCriteria(entity => entity.GetType().Name); // Enables searching directly on the type
                AddIndexDefinition(DefaultIndex);
            }
        }


        /// <summary>
        /// Creates a new index definition for the {TDomainEntity} based on the given "indexName".
        /// The index definition's indexed value will be set based on the ID property of the "TDomainEntity".
        /// </summary>
        /// <param name="indexName"></param>
        /// <returns></returns>
        public TableIndexDefinition<TDomainEntity> CreateIndexDefinition(string indexName)
        {
            var schema = new TableIndexDefinition<TDomainEntity>(NameOfEntityIdProperty).SetIndexNameKey(indexName);
            return schema;
        }


        /// <summary>
        /// Creates a new Index Definition object for the {TDomainEntity} with the index name key being set based on
        /// the name of the type by default. The index definition's indexed value will be set based on the ID property of the {TDomainEntity}.
        /// </summary>
        /// <returns></returns>
        public TableIndexDefinition<TDomainEntity> CreateIndexDefinition()
        {
            return CreateIndexDefinition(typeof(TDomainEntity).Name);
        }


        /// <summary>
        /// Adds multiple Index Definitions types to the current <see cref="TableContext{TDomainEntity}"/>.
        /// </summary>
        /// <param name="indexDefinitions"></param>
        public void AddMultipleIndexDefinitions(List<TableIndexDefinition<TDomainEntity>> indexDefinitions)
        {
            foreach (var indexDefinition in indexDefinitions)
            {
                if (IndexDefinitions.Any(indexDef => indexDef.IndexNameKey == indexDefinition.IndexNameKey))
                {
                    continue;
                }
                IndexDefinitions.Add(indexDefinition);
            }
        }


        /// <summary>
        /// Adds a single Index Definition to the current <see cref="TableContext{TDomainEntity}"/>.
        /// </summary>
        /// <param name="tableIndexDefinition"></param>
        public void AddIndexDefinition(TableIndexDefinition<TDomainEntity> tableIndexDefinition)
        {
            if (IndexDefinitions.Any(indexDef => indexDef.IndexNameKey == tableIndexDefinition.IndexNameKey))
            {
                return;
            }
            IndexDefinitions.Add(tableIndexDefinition);
        }
    }
}
