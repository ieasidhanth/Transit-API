using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace ViewPointAPI.Controllers
{
   
    public class TestController : ApiController
    {
        [HttpGet]
        public string testApi()
        {
            return "Hello World";
        }

    }
}
