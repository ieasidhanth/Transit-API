using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace ViewPointAPI.Controllers
{
    public class WinAuthController : ApiController
    {
        [HttpGet]
        public IHttpActionResult TestAutentication()
        {
          
            if (User.Identity.IsAuthenticated)
            {
                return Ok("Authenticated: " + User.Identity.Name);
            }
            else
            {
                return BadRequest("Not authenticated");
            }
        }

    }
}
