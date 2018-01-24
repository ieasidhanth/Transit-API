using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Owin;
using Owin;
using System.Web;
using System.Web.Http;

[assembly: OwinStartup(typeof(ViewPointAPI.Startup))]

namespace ViewPointAPI
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            HttpConfiguration config = new HttpConfiguration();
            WebApiConfig.Register(config);
            app.UseWebApi(config);
            ConfigureAuth(app);
        }
    }
}
