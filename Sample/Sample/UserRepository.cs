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
        /// Remember, these are just schema definitions (if you will) and they have the ability to provide several
        /// different PartitionKey id's since you are using a Func delegate to define a PartitionKey naming strategy.
        /// For the most part, you will simply reference the "SchemaName" property on the PartitionSchema class but 
        /// there are times where a PartitionSchema (strategy) will yield several PartitionKey id's. One such example
        /// is the _userIdPartitionSchema in this class. You are creating a PartitionKey based on the unique ID of
        /// each User instance. This could be used to keep track of versions (such as would be used in an Event Sourcing
        /// type of architecture).
        /// </summary>
        private void InitPartitionSchemas()
        {
            _usersInFloridaPartitionSchema = new PartitionSchema<User>(schemaName: "UsersInFlorida", validateEntityForPartition: user => user.UserAddress.State == "FL", setPartitionKey: user => "UsersInFlorida", setIndexedPropValue: user => user.UserAddress.State);
            _firstNamePartitionSchema = new PartitionSchema<User>(schemaName: "FirstName", validateEntityForPartition: user => true, setPartitionKey: user => "FirstName", setIndexedPropValue: user => user.FirstName);
            _userTypePartitionSchema = new PartitionSchema<User>(schemaName: "UserTypePartition", validateEntityForPartition: user => true, setPartitionKey: user => user.GetType().Name);
            _userIdPartitionSchema = new PartitionSchema<User>(schemaName: "UserIdPartition", validateEntityForPartition: user => true, setPartitionKey: user => user.UserId.ToJsv(), setRowKey: SetReverseChronologicalBasedRowKey);
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
            return _userContext.GetByPartitionKey(_usersInFloridaPartitionSchema.SchemaName);
        }

        public IEnumerable<User> GetUsersByFirstName(string firstName)
        {
            return _userContext.QueryWhereIndexedPropertyEquals(_firstNamePartitionSchema.SchemaName, firstName).ToList();
        }

        public IEnumerable<User> GetAllVersions(User givenUser)
        {
            var givenUserPartitionKey = _userIdPartitionSchema.SetPartitionKey(givenUser);
            return _userContext.GetByPartitionKey(givenUserPartitionKey);
        }

        private string SetReverseChronologicalBasedRowKey(User arg)
        {
            return string.Format("{0:D20}_{1}", (DateTimeOffset.MaxValue.Ticks - DateTimeOffset.Now.Ticks),
                default(Guid).ToJsv());
        }
    }
}