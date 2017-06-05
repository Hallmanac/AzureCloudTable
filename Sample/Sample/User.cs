using System;

namespace Sample
{
    public class User
    {
        public Guid UserId { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string EmailAddress { get; set; }
        public bool IsAdmin { get; set; }
        public string NameOfEmployeeMinion { get; set; }
        public string NameOfManager { get; set; }
        public Address UserAddress { get; set; }
        public int Version { get; set; }
    }

    public class Address
    {
        public int StreetNumber { get; set; }
        public string StreetName { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public int ZipCode { get; set; }
    }
}