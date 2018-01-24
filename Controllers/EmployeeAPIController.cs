using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using ViewPointAPI.Models;

namespace ViewPointAPI.Controllers
{
    public class EmployeeAPIController : ApiController
    {
        [Authorize]
        [HttpGet]
        public List<Employee> Get()
        {
            return new EmployeeDatabase();
        }
    }
}
