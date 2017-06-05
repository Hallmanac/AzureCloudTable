using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Hallmanac.AzureCloudTable.API;

using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;


namespace Sample
{
    public class UserRepository
    {
        private TableIndexDefinition<User> _firstNameIndex;
        private TableIndexDefinition<User> _usersInFloridaIndex;
        private TableIndexDefinition<User> _userTypeIndex;
        private TableIndexDefinition<User> _userVersionIndex;

        //One could make this a public property to give direct query access
        public TableContext<User> UserContext;


        public UserRepository()
        {
            var connectionString = CloudConfigurationManager.GetSetting("AzureStorageConnectionString");
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            UserContext = new TableContext<User>(storageAccount, "UserId");

            InitIndexDefinitions();
        }


        public List<User> GetAllUsers()
        {
            return UserContext.GetAll().ToList();
        }


        public async Task<List<User>> GetAllUsersAsync()
        {
            var users = await UserContext.GetAllAsync();
            return users;
        }


        public void Save(User user)
        {
            UserContext.InsertOrReplace(user);
        }


        public void Save(User[] users)
        {
            UserContext.InsertOrReplace(users);
        }


        public List<User> GetUsersThatLiveInFlorida()
        {
            return UserContext.GetAllItemsFromIndex(_usersInFloridaIndex.IndexNameKey).ToList();
        }


        public List<User> GetUsersByFirstName(string firstName)
        {
            return UserContext.GetByIndexedProperty(_firstNameIndex.IndexNameKey, firstName).ToList();
        }


        public List<User> GetUsersByTypeOfUser(string userType)
        {
            return UserContext.GetByIndexedProperty(_userTypeIndex.IndexNameKey, userType).ToList();
        }


        public IEnumerable<User> GetAllVersions(User givenUser)
        {
            return UserContext.GetByIndexedProperty(_userVersionIndex.IndexNameKey,
                                                    _userVersionIndex.GetIndexedPropertyFromCriteria(givenUser));
        }


        /// <summary>
        /// The index definitions are how your domain object gets sorted/categorized/grouped inside Azure Table
        /// storage. You create them in your client code and then "add" them to the TableContext class that you're using 
        /// to interact with the Table (in this case _userContext). Remember, these are just index definitions for one 
        /// particular partition or model type.
        /// 
        /// There is a DefaultIndex that is set on the TableContext class automatically (in this case _userContext)
        /// which sets the IndexNameKey to be the name of the object Type and the Indexed value based on the Id property of the object
        /// provided during intialization.
        /// </summary>
        private void InitIndexDefinitions()
        {
            _usersInFloridaIndex = UserContext.CreateIndexDefinition()
                                                        .SetIndexNameKey("UsersInFloridaIdx")
                                                        .DefineIndexCriteria(user => user != null && user.UserAddress.State == "FL")
                                                        /*The RowKey is set to the ID property by default, which in this case is the user.UserId*/
                                                        .SetIndexedPropertyCriteria(user => user?.UserAddress.State);

            _firstNameIndex = UserContext.CreateIndexDefinition()
                                                   .SetIndexNameKey("FirstNameIdx")
                                                   .DefineIndexCriteria(user => user != null)
                                                   /*The RowKey is set to the ID property by default, which in this case is the user.UserId*/
                                                   .SetIndexedPropertyCriteria(user => user?.FirstName);

            _userTypeIndex = UserContext.CreateIndexDefinition()
                                                  .SetIndexNameKey("UserTypeIdx")
                                                  .DefineIndexCriteria(user => user != null)
                                                  /*The RowKey is set to the ID property by default, which in this case is the user.UserId*/
                                                  .SetIndexedPropertyCriteria(user => user.IsAdmin ? "Admin" : "Standard");

            _userVersionIndex = UserContext.CreateIndexDefinition()
                                                     .SetIndexNameKey("UserVersionIdx")
                                                     .DefineIndexCriteria(user => user != null)
                                                     /*In this case we're keeping a version log so we want a new RowKey created upon each write to the Table*/
                                                     .SetCustomDefinitionForRowKey(user => UserContext.GetChronologicalBasedRowKey())
                                                     .SetIndexedPropertyCriteria(user => user?.UserId);

            // Now add the index definitions that were just created to the TableContext<User> instance (i.e. UserContext).
            UserContext.AddMultipleIndexDefinitions(new List<TableIndexDefinition<User>>
            {
                _usersInFloridaIndex,
                _firstNameIndex,
                _userTypeIndex,
                _userVersionIndex
            });
        }
    }
}