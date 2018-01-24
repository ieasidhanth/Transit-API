using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ViewPointAPI
{
    public class User
    {
        public int UserId { get; set; }
        public string DisplayName { get; set; }
        public string ADGuid { get; set; }
        public bool IsAdmin { get; set; }
    }
}