using System.Collections.Generic;
using System.Linq;
using AzureCloudTableContext.Api;
using ServiceStack.Text;

namespace Sample
{
    using System;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;

    public class UserRepository
    {
        private readonly string _accountKey = "SomeLongAccountKey";

        private readonly string _accountName = "YourAzureStorageAccountName";

        /*This is used as a dummy object to give access to the property names of a User type.
         *If the User class was static, then there would be no need for this.
         *This gets used in the constructor with the extension method called "GetPropertyName".*/
        private readonly User _tempUserInstance = new User();

        private PartitionSchema<User> _usersInFloridaPartitionSchema;
        private PartitionSchema<User> _userVersionPartitionSchema;
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
            _usersInFloridaPartitionSchema = _userContext.CreatePartitionSchema()
                .SetPartitionKey("UsersInFlorida")
                .SetSchemaCriteria(user => user.UserAddress.State == "FL")
                /*The RowKey is set to the ID property by default, which in this case is the user.UserId*/
                .SetIndexedPropertyCriteria(user => user.UserAddress.State);
                
            _firstNamePartitionSchema = _userContext.CreatePartitionSchema()
                .SetPartitionKey("FirstName")
                .SetSchemaCriteria(user => true)
                /*The RowKey is set to the ID property by default, which in this case is the user.UserId*/
                .SetIndexedPropertyCriteria(user => user.FirstName);

            _userTypePartitionSchema = _userContext.CreatePartitionSchema()
                .SetPartitionKey("UserTypePartition")
                .SetSchemaCriteria(user => true)
                /*The RowKey is set to the ID property by default, which in this case is the user.UserId*/
                .SetIndexedPropertyCriteria(user => user.GetType().Name);

            _userVersionPartitionSchema = _userContext.CreatePartitionSchema()
                .SetPartitionKey("UserVersionPartition")
                .SetSchemaCriteria(user => true)
                .SetRowKeyCriteria(user => _userContext.GetChronologicalBasedRowKey())/*In this case we're keeping a version log so we want a new 
                                                                                       RowKey created upon each write to the Table*/
                .SetIndexedPropertyCriteria(user => user.UserId);
                
            // Now add the schemas that were just created to the CloudTableContext<User> instance (i.e. _userContext).
            _userContext.AddMultiplePartitionSchemas(new List<PartitionSchema<User>>
                {
                    _usersInFloridaPartitionSchema,
                    _firstNamePartitionSchema,
                    _userTypePartitionSchema,
                    _userVersionPartitionSchema
                });
        }

        public IEnumerable<User> GetAllUsers()
        {
            foreach(var obj in _userContext.GetByDefaultSchema())
            {
                var objInstance = (User)obj;
                yield return objInstance;
            }
            /*return _userContext.GetByDefaultSchema();*/
        }

        public void Save(User user)
        {
            _userContext.InsertOrReplace(user);
        }

        /*public void Save(Admin adminUser) { _userContext.InsertOrReplace(adminUser); }*/

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
            return _userContext.GetByIndexedProperty(_firstNamePartitionSchema.PartitionKey, firstName).ToList();
        }

        public IEnumerable<User> GetAllVersions(User givenUser)
        {
            return _userContext.GetByIndexedProperty(_userVersionPartitionSchema.PartitionKey, _userVersionPartitionSchema.GetIndexedPropertyFromCriteria(givenUser));
        }
    }
}
