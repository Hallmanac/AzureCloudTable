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
            Console.ReadLine();

            if(existingUsers.Count < 1)
            {
                var user1 = new User
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
                            }
                    };
                var user2 = new User
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
                            }
                    };

                var user3 = new User
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
                            }
                    };
                Console.WriteLine("Hit enter to see info for User 1.");
                Console.ReadLine();
                Console.WriteLine("User 1 is: \n{0}", user1.SerializeAndFormat());
                Console.ReadLine();

                Console.WriteLine("Saving Users...");
                startTime = DateTimeOffset.Now;
                userRepo.Save(user1);
                endTime = DateTimeOffset.Now;
                elapsedTime = (endTime - startTime).Milliseconds;
                Console.WriteLine("Users saved in {0} milliseconds.", elapsedTime.ToString());
                Console.ReadLine();

                Console.WriteLine("Getting all users...");
                startTime = DateTimeOffset.Now;
                var allUsersGotten = userRepo.GetAllUsers().ToList();
                endTime = DateTimeOffset.Now;
                elapsedTime = (endTime - startTime).Milliseconds;
                Console.WriteLine("All Users:\n{0}", allUsersGotten.SerializeAndFormat());
                Console.WriteLine("Time taken was {0} milliseconds.", elapsedTime);
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
                Console.ReadLine();
            }

            else
            {
                Console.WriteLine("All Users:\n{0}", existingUsers.SerializeAndFormat());
                Console.WriteLine("All Users listed above. Hit enter to proceed...");
                Console.ReadLine();
                
                Console.WriteLine("");
                foreach(var user in existingUsers)
                {
                    if(user.UserAddress.State == "FL")
                        continue;
                    user.UserAddress.State = "NY";
                    user.Version++;
                }

                userRepo.Save(existingUsers.ToArray());

                Console.WriteLine("Getting users with first name of Brian...");
                startTime = DateTimeOffset.Now;
                var usersWithFirstNameBrian = userRepo.GetUsersByFirstName("Brian");
                endTime = DateTimeOffset.Now;
                elapsedTime = (endTime - startTime).Milliseconds;
                Console.WriteLine("List of Users with First Name of Brian:\n{0}", usersWithFirstNameBrian.SerializeAndFormat());
                Console.WriteLine("\nTime taken was {0} milliseconds.", elapsedTime.ToString());

                Console.ReadLine();
            }

            Console.WriteLine("Getting users that live in Florida...");
            startTime = DateTimeOffset.Now;
            var usersInFlorida = userRepo.GetUsersThatLiveInFlorida().ToList();
            endTime = DateTimeOffset.Now;
            elapsedTime = (endTime - startTime).Milliseconds;
            if(usersInFlorida.Count > 0)
            {
                Console.WriteLine("Users in Florida:\n{0}", usersInFlorida.SerializeAndFormat());
            }
            else
            {
                Console.WriteLine("No Users live in Florida.");
            }
            Console.WriteLine("Operation took {0} milliseconds.", elapsedTime.ToString());
            Console.ReadLine();

            Console.WriteLine("Getting versions for users...");
            if(existingUsers.Count < 1)
                existingUsers = userRepo.GetAllUsers().ToList();
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
            }
            Console.ReadLine();
        }
    }
}
