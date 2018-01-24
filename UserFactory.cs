using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Security.Principal;

namespace ViewPointAPI
{
    public class UserFactory
    {
        public User Create(WindowsIdentity currentWindowsUser)
        {
            var user = new User();

            string name = currentWindowsUser.Name.Replace("IEA\\", "");

            // a much simplified case for example (better to retrieve by GUID)
            User userInDatabase = (from u in db.Users
                                   where u.FirstName.ToLower() + u.LastName.ToLower() == name.ToLower()
                                   select u).FirstOrDefault();

            if (userInDatabase != null)
            {
                user.UserId = userInDatabase.UserId;
                user.DisplayName = userInDatabase.DisplayName;
                user.IsAdmin = userInDatabase.IsAdmin;
                user.ADGuid = userInDatabase.ADGuid;
            }

            return user;
        }
    }
   
}