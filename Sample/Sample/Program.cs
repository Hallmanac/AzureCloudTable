using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Text;

namespace Sample
{
    class Program
    {
        private static void Main(string[] args)
        {
            #region First Test
            var adminUser = new List<Admin>();

            Console.WriteLine("Initializing User Repo...");
            var startTime = DateTimeOffset.Now;
            var userRepo = new UserRepository();
            var endTime = DateTimeOffset.Now;
            var elapsedTime = (endTime - startTime).Milliseconds;

            Console.WriteLine("Repo took {0} milliseconds to complete.", elapsedTime.ToString());
            Console.ReadLine();

            startTime = DateTimeOffset.Now;
            var existingUsers = userRepo.GetAllUsers().ToList();
            endTime = DateTimeOffset.Now;
            elapsedTime = (endTime - startTime).Milliseconds;
            Console.WriteLine("Getting all Users took {0} milliseconds.", elapsedTime);
            Console.WriteLine("There were {0} users: ", existingUsers.Count.ToString());
            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();

            if(existingUsers.Count < 1)
            {
                var user1 = new Admin
                {
                    UserId = Guid.NewGuid(),
                    FirstName = "Brian",
                    LastName = "Hall",
                    EmailAddress = "Brian@Hallmanac.com",
                    UserAddress = new Address
                    {
                        StreetNumber = 1234,
                        StreetName = "Anywhere ST",
                        City = "Orlando",
                        State = "FL",
                        ZipCode = 55555
                    },
                    Employee = "Jennifer"
                };
                var user2 = new Admin
                {
                    UserId = Guid.NewGuid(),
                    FirstName = "Jennifer",
                    LastName = "Adamchek",
                    EmailAddress = "Jenn@somedomain.com",
                    UserAddress = new Address
                    {
                        StreetNumber = 4322,
                        StreetName = "Elm ST",
                        City = "Houston",
                        State = "TX",
                        ZipCode = 22222
                    },
                    Employee = "Brian"
                };

                var user3 = new Standard
                {
                    UserId = Guid.NewGuid(),
                    FirstName = "Daryl",
                    LastName = "Smith",
                    EmailAddress = "Daryl@someotherdomain.com",
                    UserAddress = new Address
                    {
                        StreetNumber = 9068,
                        StreetName = "Pine RD",
                        City = "Seattle",
                        State = "WA",
                        ZipCode = 99888
                    },
                    Manager = "Brian"
                };
                Console.WriteLine("Hit enter to see info for User 1.");
                Console.ReadLine();
                Console.WriteLine("User 1 is: \n{0}", user1.SerializeAndFormat());
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();

                Console.WriteLine("Saving Users...");
                startTime = DateTimeOffset.Now;
                userRepo.Save(user1);
                endTime = DateTimeOffset.Now;
                elapsedTime = (endTime - startTime).Milliseconds;
                Console.WriteLine("Users saved in {0} milliseconds.", elapsedTime.ToString());
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();

                Console.WriteLine("Getting all users...");
                startTime = DateTimeOffset.Now;
                
                var allUsersGotten = userRepo.GetAllUsers().ToList();
                endTime = DateTimeOffset.Now;
                elapsedTime = (endTime - startTime).Milliseconds;
                foreach(var user in allUsersGotten)
                {
                    var userItem = string.Format("{0}", JsonSerializer.SerializeToString(user, user.GetType()));
                    Console.WriteLine("{0}", userItem.SerializeAndFormat());
                    /*Console.WriteLine("{0}", JsonSerializer.SerializeToString(user, user.GetType()));*/
                }
                /*Console.WriteLine("All Users:\n{0}", allUsersGotten.SerializeAndFormat());*/
                Console.WriteLine("Time taken was {0} milliseconds.", elapsedTime);
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();

                var userList = new List<User>
                {
                    user2,
                    user3
                };

                Console.WriteLine("Saving the rest of the users...");
                startTime = DateTimeOffset.Now;
                userRepo.Save(userList.ToArray());
                endTime = DateTimeOffset.Now;
                elapsedTime = (endTime - startTime).Milliseconds;
                Console.WriteLine("Users List Saved in {0} milliseconds.", elapsedTime.ToString());
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            } else
            {
                foreach(var existingUser in existingUsers)
                {
                    Console.WriteLine("{0}", JsonSerializer.SerializeToString(existingUser, existingUser.GetType()));
                }
                /*Console.WriteLine("All Users:\n{0}", existingUsers.SerializeAndFormat());*/
                Console.WriteLine("All Users listed above.");
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();

                Console.WriteLine("Changing State property on users (except those in FL)...");
                foreach(var user in existingUsers)
                {
                    if(user.UserAddress.State == "FL") continue;
                    user.UserAddress.State = "NY";
                    user.Version++;
                }

                userRepo.Save(existingUsers.ToArray());
                Console.WriteLine("State property changed.");
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
                
                Console.WriteLine("Getting users with first name of Brian...");
                startTime = DateTimeOffset.Now;
                var usersWithFirstNameBrian = userRepo.GetUsersByFirstName("Brian").ToList();
                endTime = DateTimeOffset.Now;
                elapsedTime = (endTime - startTime).Milliseconds;
                Console.WriteLine("List of Users with First Name of Brian:\n{0}", usersWithFirstNameBrian.SerializeAndFormat());
                Console.WriteLine("\nTime taken was {0} milliseconds.", elapsedTime.ToString());
                Console.WriteLine("Press any key to continue...");

                Console.ReadLine();
            }

            Console.WriteLine("Gettin all Admin user types...");
            foreach(var user in userRepo.GetAllUsers())
            {
                if(user.GetType().Name == "Admin")
                {
                    adminUser.Add((Admin)user);
                }
            }
            Console.WriteLine("List of Admin users: \n{0}", adminUser.SerializeAndFormat());
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadLine();

            Console.WriteLine("Getting users that live in Florida...");
            startTime = DateTimeOffset.Now;
            var usersInFlorida = userRepo.GetUsersThatLiveInFlorida().ToList();
            endTime = DateTimeOffset.Now;
            elapsedTime = (endTime - startTime).Milliseconds;
            if(usersInFlorida.Count > 0)
            {
                Console.WriteLine("Users in Florida:\n{0}", usersInFlorida.SerializeAndFormat());
            } else
            {
                Console.WriteLine("No Users live in Florida.");
            }
            Console.WriteLine("Operation took {0} milliseconds.", elapsedTime.ToString());
            Console.WriteLine("Hit any key to continue...");
            Console.ReadLine();

            Console.WriteLine("Getting versions for Jennifer Admin...");
            var jenniferUser = userRepo.GetUsersByFirstName("Jennifer");
            var listOfJenniferVersions = new List<Admin>();
            foreach(var userVersion in userRepo.GetAllVersions(jenniferUser.FirstOrDefault()))
            {
                listOfJenniferVersions.Add((Admin)userVersion);
            }
            Console.WriteLine("All Versions of AdminUser...\n{0}", listOfJenniferVersions.SerializeAndFormat());
            Console.WriteLine("Hit any key to continue...");
            /*Console.WriteLine("Getting versions for users...");
            if(existingUsers.Count < 1) existingUsers = userRepo.GetAllUsers().ToList();
            foreach(var user in existingUsers)
            {
                startTime = DateTimeOffset.Now;
                var versionsForCurrentUser = userRepo.GetAllVersions(user).ToList();
                endTime = DateTimeOffset.Now;
                elapsedTime = (endTime - startTime).Milliseconds;
                var latestVersionNumber = versionsForCurrentUser.Max(usr => usr.Version);

                Console.WriteLine("The latest version number is {0}.", latestVersionNumber.ToString());
                Console.WriteLine("\nTime to complete query was {0} milliseconds.", elapsedTime.ToString());
                Console.WriteLine("Press enter to continue...");
                Console.ReadLine();
            }*/
            Console.ReadLine();
            #endregion

            
        }
    }
}
