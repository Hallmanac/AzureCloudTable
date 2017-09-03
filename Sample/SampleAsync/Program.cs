using Newtonsoft.Json;
using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace SampleAsync
{
    class Program
    {
        static void Main(string[] args)
        {
            AsyncContext.Run(() => MainAsync(args));
        }

        static async Task<int> MainAsync(string[] args)
        {
            var sw = new Stopwatch();
            var listOfUsers = new List<User>();

            #region First Test
            var adminUsers = new List<User>();
            Console.WriteLine("Initializing User Repo...");
            sw.Start();
            var userRepo = new UserRepository();
            sw.Stop();
            Console.WriteLine("Repo took {0} milliseconds to complete.\n", sw.ElapsedMilliseconds);
            Console.WriteLine("The nagle = {0}", userRepo.UserContext.TableOperationsService.TableServicePoint.Expect100Continue);
            Console.WriteLine("The Expect100 status is {0}", userRepo.UserContext.TableOperationsService.TableServicePoint.UseNagleAlgorithm);
            sw.Reset();
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadLine();
            Console.WriteLine("\nGetting All Users...");
            sw.Start();
            var existingUsers = await userRepo.GetAllUsersAsync();
            sw.Stop();
            Console.WriteLine("Getting all Users took {0} milliseconds.", sw.ElapsedMilliseconds);
            Console.WriteLine("There were {0} users: ", existingUsers.Count);
            sw.Reset();
            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
            if (existingUsers.Count < 1)
            {
                sw.Start();
                #region Initialize Users
                for (var i = 0; i < 400; i++)
                {
                    if (i % 6 == 0)
                    {
                        var adminUsr = new User
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
                            NameOfEmployeeMinion = "Jennifer",
                            IsAdmin = true
                        };
                        listOfUsers.Add(adminUsr);
                    }
                    else
                    {
                        var stdUsr = new User
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
                            NameOfManager = "Brian"
                        };
                        listOfUsers.Add(stdUsr);
                    }
                    sw.Stop();
                }

                #endregion --Initialize Users--

                Console.WriteLine("\nUsers created in {0} milliseconds.", sw.ElapsedMilliseconds);
                Console.WriteLine("Press enter to see info for User 1.");
                Console.ReadLine();
                Console.WriteLine("User 1 is: \n{0}", JsonConvert.SerializeObject(listOfUsers[0], Formatting.Indented));
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
                Console.WriteLine("Saving Users...");
                sw.Start();
                await userRepo.SaveAsync(listOfUsers.ToArray());
                sw.Stop();
                Console.WriteLine("Users saved in {0} milliseconds.", sw.ElapsedMilliseconds);
                sw.Reset();
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
                Console.WriteLine("Getting all users...");
                sw.Start();
                var allUsersGotten = await userRepo.GetAllUsersAsync();
                sw.Stop();
                /*foreach(var user in allUsersGotten)
                {
                    var userItem = string.Format("{0}", JsonSerializer.SerializeToString(user, user.GetType()));
                    Console.WriteLine("{0}", userItem.SerializeAndFormat());
                }*/
                Console.WriteLine("Number of users retrieved is {0}\n", allUsersGotten.Count);
                Console.WriteLine("Time taken was {0} milliseconds.", sw.ElapsedMilliseconds);
                sw.Reset();
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
            else
            {
                /*foreach(var existingUser in existingUsers)
                {
                    Console.WriteLine("{0}", JsonSerializer.SerializeToString(existingUser, existingUser.GetType()));
                }*/
                /*Console.WriteLine("All Users listed above.");*/

                Console.WriteLine("Changing State property on users (except those in FL)...");
                sw.Start();
                var counter = 0;
                foreach (var user in existingUsers)
                {
                    if (user.UserAddress.State == "FL") continue;
                    user.UserAddress.State = "NY";
                    user.Version++;
                    counter++;
                }
                await userRepo.SaveAsync(existingUsers.ToArray());
                sw.Stop();
                Console.WriteLine("State property changed.");
                Console.WriteLine("Time taken was {0} milliseconds.", sw.ElapsedMilliseconds);
                Console.WriteLine("\nThere were {0} users changed.", counter);
                sw.Reset();
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
                Console.WriteLine("Getting users with first name of Brian...");
                sw.Start();
                var usersWithFirstNameBrian = await userRepo.GetUsersByFirstNameAsync("Brian");
                sw.Stop();
                //Console.WriteLine("List of Users with First Name of Brian:\n{0}", usersWithFirstNameBrian.SerializeAndFormat());
                Console.WriteLine("\nTime taken was {0} milliseconds.", sw.ElapsedMilliseconds);
                Console.WriteLine("Number of Users = {0}", usersWithFirstNameBrian.Count);
                sw.Reset();
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
            Console.WriteLine("Getting all Admin user types...");
            sw.Start();
            /*foreach(var user in userRepo.GetAllUsers())
            {
                if(user.GetType().Name == "Admin")
                {
                    adminUser.Add((Admin)user);
                }
            }*/
            var retrievedAdmins = await userRepo.GetUsersByTypeOfUserAsync("Admin");
            adminUsers.AddRange(retrievedAdmins);
            sw.Stop();
            //Console.WriteLine("List of Admin users: \n{0}", adminUser.SerializeAndFormat());
            Console.WriteLine("\nTime taken was {0} milliseconds.", sw.ElapsedMilliseconds);
            Console.WriteLine("\nThere were {0} users retrieved.", adminUsers.Count);
            sw.Reset();
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadLine();
            Console.WriteLine("Getting users that live in Florida...");
            sw.Start();
            var usersInFlorida = await userRepo.GetUsersThatLiveInFloridaAsync();
            sw.Stop();
            if (usersInFlorida.Count > 0)
            {
                //Console.WriteLine("Users in Florida:\n{0}", usersInFlorida.SerializeAndFormat());
            }
            else
            {
                Console.WriteLine("No Users live in Florida.");
            }
            Console.WriteLine("Operation took {0} milliseconds.", sw.ElapsedMilliseconds);
            Console.WriteLine("\nThere were {0} users that live in Florida.", usersInFlorida.Count);
            sw.Reset();
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadLine();
            Console.WriteLine("Getting versions for Jennifer Admin...");
            sw.Start();
            var jenniferUser = await userRepo.GetUsersByFirstNameAsync("Jennifer");
            var listOfJenniferVersions = new List<User>();
            if (jenniferUser != null)
            {
                foreach (var userVersion in await userRepo.GetAllVersionsAsync(jenniferUser.FirstOrDefault()))
                {
                    listOfJenniferVersions.Add(userVersion);
                }
            }
            sw.Stop();
            Console.WriteLine("All Versions of Jenfer Admin...\n{0}", JsonConvert.SerializeObject(listOfJenniferVersions, Formatting.Indented));
            Console.WriteLine("Time taken was {0} milliseconds.", sw.ElapsedMilliseconds);
            sw.Reset();
            Console.WriteLine("Would you like to delete all the test data? -  y or n");
            var shouldDeleteAnswer = Console.ReadLine();
            if (string.Equals(shouldDeleteAnswer, "y", StringComparison.CurrentCultureIgnoreCase))
            {
                var allUsers = existingUsers.ToList();
                allUsers.AddRange(listOfUsers);
                sw.Start();
                await userRepo.DeleteAllUsersAsync();
                sw.Stop();
                Console.WriteLine("Deleted all users");
                Console.WriteLine($"Time taken was {sw.ElapsedMilliseconds}");
            }
            Console.WriteLine("\nPress any key to continue...");
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

            return 1;
        }
    }
}
