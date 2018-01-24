using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using System.Web.Configuration;

namespace ViewPointAPI
{
    public class Connection
    {
        private SqlConnection sqlconn;
        private SqlConnection transitDBconn;
        public SqlConnection initiateConnection()
        {
            try
            {
                sqlconn = new SqlConnection(Convert.ToString(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"]));
                sqlconn.Open();
                return sqlconn;
            }
            catch (SqlException e)
            {
                return null;

            }
        }
        public void dispose(SqlConnection conn)
        {
            conn.Dispose();

        }
        public SqlConnection InitiateTransitDBConncetion()
        {
            try
            {
                transitDBconn = new SqlConnection(Convert.ToString(WebConfigurationManager.ConnectionStrings["TransitDBConnection"]));
                transitDBconn.Open();
                return transitDBconn;
            }
            catch (SqlException e)
            {
                return null;

            }

        }


    }
}
