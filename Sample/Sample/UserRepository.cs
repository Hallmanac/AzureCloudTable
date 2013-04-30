using System;
using System.Collections.Generic;
using System.Linq;
using AzureCloudTableContext.Api;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using ServiceStack.Text;

namespace Sample
{
    public class UserRepository
    {
        private readonly string _accountKey = "[YourAccountKey]";

        private readonly string _accountName = "[YourAccountName]";

        /*This is used as a dummy object to give access to the property names of a User type.
         *If the User class was static, then there would be no need for this.
         *This gets used in the constructor with the extension method called "GetPropertyName".*/
        private readonly User _tempUserInstance = new User();

        private PartitionSchema<User> _usersInFloridaPartitionSchema;
        private PartitionSchema<User> _userIdPartitionSchema;
        private PartitionSchema<User> _userTypePartitionSchema;
        private PartitionSchema<User> _firstNamePartitionSchema;

        //One could possibly make this a public property to give direct query access. Of course
        //that sort of defeats the purpose of a repository but, hey, nobody's looking.
        private CloudTableContext<User> _userContext;

        public UserRepository()
        {
            var storageCredentials = new StorageCredentials(_accountName, _accountKey);
            var storageAccount = new CloudStorageAccount(storageCredentials, true);
            _userContext = new CloudTableContext<User>(storageAccount,
                this.GetPropertyName(() => _tempUserInstance.UserId));
            
            InitPartitionSchemas();
        }

        /// <summary>
        /// The partition schemas are how your domain object gets sorted/categorized/grouped inside Azure Table
        /// storage. You create them in your client code and then "add" them to the CloudTableContext class
        /// that you're using to interact with the Table (in this case _userContext). 
        /// Remember, these are just schema definitions for one particular PartitionKey.
        /// 
        /// There is a DefaultSchema that get set on the CloudTableContext class automatically (in this case _userContext)
        /// which sets the PartitionKey to be the name of the object Type and the RowKey based on the Id property of the object
        /// provided during intialization.
        /// </summary>
        private void InitPartitionSchemas()
        {
            _usersInFloridaPartitionSchema = new PartitionSchema<User>()
                .SetPartitionKey("UsersInFlorida")
                .SetSchemaPredicateCriteria(user => user.UserAddress.State == "FL")
                .SetIndexedPropertyCriteria(user => user.UserAddress.State);
                
            _firstNamePartitionSchema = new PartitionSchema<User>()
                .SetPartitionKey("FirstName")
                .SetSchemaPredicateCriteria(user => true)
                .SetIndexedPropertyCriteria(user => user.FirstName);

            _userTypePartitionSchema = new PartitionSchema<User>()
                .SetPartitionKey("UserTypePartition")
                .SetSchemaPredicateCriteria(user => true)
                .SetRowKeyCriteria(user => user.UserId.ToJsv())
                .SetIndexedPropertyCriteria(user => user.GetType().Name);

            _userIdPartitionSchema = new PartitionSchema<User>()
                .SetPartitionKey("UserIdPartition")
                .SetSchemaPredicateCriteria(user => true)
                .SetRowKeyCriteria(user => PartitionSchema<User>.GetChronologicalBasedRowKey())
                .SetIndexedPropertyCriteria(user => user.UserId);
                
            _userContext.AddMultiplePartitionSchemas(new List<PartitionSchema<User>>
                {
                    _usersInFloridaPartitionSchema,
                    _firstNamePartitionSchema,
                    _userTypePartitionSchema,
                    _userIdPartitionSchema
                });
        }

        public IEnumerable<User> GetAllUsers()
        {
            return _userContext.GetAll();
        }

        public void Save(User user)
        {
            _userContext.InsertOrReplace(user);
        }

        public void Save(User[] users)
        {
            _userContext.InsertOrReplace(users);
        }

        public IEnumerable<User> GetUsersThatLiveInFlorida()
        {
            return _userContext.GetByPartitionKey(_usersInFloridaPartitionSchema.PartitionKey);
        }

        public IEnumerable<User> GetUsersByFirstName(string firstName)
        {
            return _userContext.QueryWhereIndexedPropertyEquals(_firstNamePartitionSchema.PartitionKey, firstName).ToList();
        }

        public IEnumerable<User> GetAllVersions(User givenUser)
        {
            return _userContext.QueryWhereIndexedPropertyEquals(_userIdPartitionSchema.PartitionKey, givenUser.UserId);
        }
    }
}