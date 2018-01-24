using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Script.Serialization;
using System.Data;
using Newtonsoft.Json.Linq;
using ViewPointAPI.Models;
using System.IO;
using System.DirectoryServices.AccountManagement;
using System.Threading.Tasks;
using System.Text;
using System.Net.Http.Headers;
using System.Web;
using System.Configuration;

namespace ViewPointAPI.Controllers
{
   // [BasicAuthentication]
    public class IControlController : ApiController
    {
        private DSL obj;
        private List<Dictionary<string, object>> jobList;
        private List<Dictionary<string, object>> locationList;

        public IControlController()
        {
           // obj = new DSL();
        }
        //gets all jobs
        [HttpGet]
        public List<Dictionary<string, object>> getJobs()
        {
            obj = new DSL();
            DataTable dt;
            dt = obj.getAllJobs();
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            Dictionary<string, object> row;
            foreach (DataRow dr in dt.Rows)
            {
                row = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    row.Add(col.ColumnName, dr[col]);
                }
                rows.Add(row);
            }
            jobList = rows;
            return rows;

        }
        //gets all equipments associated with a job
        [HttpPost]
        public List<Dictionary<string, object>> getEquipments(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            //JArray v = JArray.Parse(postedString);
            dynamic d = JObject.Parse(Convert.ToString(postedString));
            string jobId = Convert.ToString(d.jobId);
            obj = new DSL();
            DataTable dt;
            dt = obj.getEquipments(jobId);
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            Dictionary<string, object> row;
            foreach (DataRow dr in dt.Rows)
            {
                row = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    row.Add(col.ColumnName, dr[col]);
                }
                rows.Add(row);
            }

            return rows;


        }
        //gets all locations
        [HttpGet]
        public List<Dictionary<string, object>> getLocations()
        {
            obj = new DSL();
            DataTable dt;
            dt = obj.getAllLocations();
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            Dictionary<string, object> row;
            foreach (DataRow dr in dt.Rows)
            {
                row = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    row.Add(col.ColumnName, dr[col]);
                }
                rows.Add(row);
            }
            locationList = rows;
            return rows;

        }
        [HttpGet]
        public List<Dictionary<string, object>> getTorqueTools()
        {
            obj = new DSL();
            DataTable dt;
            dt = obj.getAllEquipments();
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            Dictionary<string, object> row;
            foreach (DataRow dr in dt.Rows)
            {
                row = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    row.Add(col.ColumnName, dr[col]);
                }
                rows.Add(row);
            }

            return rows;

        }
        //Generate template
        [HttpPost]
        public string ScheduleBatch(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            dynamic v = JArray.Parse(postedString);
            //dynamic d = JObject.Parse(Convert.ToString(postedString));
            //string month = Convert.ToDateTime(d.month);
            JArray toCsv = new JArray();
            JArray pushJob = new JArray();
            foreach(var equipment in v )
            {
                JObject item = new JObject();
                JObject itemPushJob = new JObject();
                //for converting it to CSV
                item.Add(new JProperty("Mth", DateTime.Today.ToString("MM/dd/yyyy")));
                item.Add(new JProperty("Equipment",Convert.ToString( equipment.Equipment)));
                item.Add(new JProperty("ToJob", equipment.ToJob));
                item.Add(new JProperty("ToLocation", equipment.ToLocation));
                string indate = equipment.jobDate;
                string[] dateinarray = indate.Split(' ');
                item.Add(new JProperty("DateIn", DateTime.Today.ToString(dateinarray[0])));
                item.Add(new JProperty("TimeIn", DateTime.Now.ToString(dateinarray[1])));
                toCsv.Add(item);
                // for pushing into Transit Database
                itemPushJob.Add(new JProperty("SerialNo",equipment.SerialNo));
                itemPushJob.Add(new JProperty("EquipmentID", Convert.ToString(equipment.Equipment)));
                itemPushJob.Add(new JProperty("EquipmentDescription", Convert.ToString(equipment.EquipmentDescription)));
                if(equipment.ToJob=="")
                {
                    itemPushJob.Add(new JProperty("TransferLocID", equipment.ToLocation));

                }
                else if(equipment.ToLocation=="")
                {
                    itemPushJob.Add(new JProperty("TransferLocID", equipment.ToJob));

                }
                
                itemPushJob.Add(new JProperty("TransferLocName", equipment.TransferLocDescription));
                itemPushJob.Add(new JProperty("jobDate", equipment.jobDate));
                itemPushJob.Add(new JProperty("CreatedBY", equipment.CreatedBy));
                itemPushJob.Add(new JProperty("CreatedTime", DateTime.Now.ToString()));
                pushJob.Add(itemPushJob);



            }
            Console.WriteLine(toCsv);
            obj = new DSL();
            try
            {
                string csv = obj.convertToCSV(toCsv);
                
                int fcount = Directory.GetFiles("\\\\DEV-VPOIN\\Viewpoint_EMAutoImport", "*.*", SearchOption.TopDirectoryOnly).Length;
                string date = DateTime.Now.ToString().Replace(':', '_').Replace('/', '-');
                string filename = (fcount + 1) + "_" + date;
                File.WriteAllText("\\\\DEV-VPOIN\\Viewpoint_EMAutoImport\\Job_" + filename + ".csv", csv);
                int i=obj.SchdeuleJob(pushJob);
                if (i == 1)
                    return "Success";
                else
                    return "faliure";

            }
            catch(Exception ex)
            {
                return ("failed");

            }
            


        }
        //Transfer Batch - This is  Schedule Batch V2 for Viewpoint 6.13 where there is no Batch concept for Equipment Transfers.
        [HttpPost]
        public string TransferBatch(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            dynamic v = JArray.Parse(postedString);
            //dynamic d = JObject.Parse(Convert.ToString(postedString));
            //string month = Convert.ToDateTime(d.month);
            JArray toCsv = new JArray();
            JArray pushJob = new JArray();
            foreach (var equipment in v)
            {
                JObject item = new JObject();
                JObject itemPushJob = new JObject();

                string indate = equipment.jobDate;
                string[] dateinarray = indate.Split(' ');

                // for pushing into Transit Database
                itemPushJob.Add(new JProperty("SerialNo", equipment.SerialNo));
                itemPushJob.Add(new JProperty("EquipmentID", Convert.ToString(equipment.Equipment)));
                itemPushJob.Add(new JProperty("EquipmentDescription", Convert.ToString(equipment.EquipmentDescription)));
                if (equipment.ToJob == "")
                {
                    itemPushJob.Add(new JProperty("TransferLocID", equipment.ToLocation));

                }
                else if (equipment.ToLocation == "")
                {
                    itemPushJob.Add(new JProperty("TransferLocID", equipment.ToJob));

                }

                itemPushJob.Add(new JProperty("TransferLocName", equipment.TransferLocDescription));
                itemPushJob.Add(new JProperty("jobDate", equipment.jobDate));
                itemPushJob.Add(new JProperty("CreatedBY", equipment.CreatedBy));
                itemPushJob.Add(new JProperty("CreatedTime", DateTime.Now.ToString()));
                itemPushJob.Add(new JProperty("PhysicalDate", DateTime.Now.ToShortDateString()));
                pushJob.Add(itemPushJob);




            }
            obj = new DSL();
            try
            {
                //string updateStatus= obj.UpdatevEMLocationHistory(JArray.Parse(postedString));
               string updateStatus = obj.UpdatevEMLocationHistoryV2(JArray.Parse(postedString));
                
               if (updateStatus=="1")
                {
                    int TransitDBUpdate = obj.SchdeuleJob(pushJob);
                    if (TransitDBUpdate == 1)
                    {
                        int phyisycalDateUpdate = obj.updateEquipmentPhysicaDateViewpoint(pushJob);
                        if(phyisycalDateUpdate==1)
                        {
                            return "Success";

                        }
                        else
                        {
                            return "Date_update_failed";
                        }
                        

                    }
                       
                    else
                    {
                        return "failed";
                    }
                        


                }
               else if(updateStatus=="-1")
                {
                    return ("failed");

                }
               else
                {
                    //return faulty equipment status in batch
                    return updateStatus;
                }
                


            }
            catch (Exception ex)
            {
                return ("failed");

            }
        }

        //Test API
        [HttpGet]
        public string testApi()
        {
            return "API Working Fine!";
        }

        //Get Batches
        [HttpGet]
        public List<Dictionary<string, object>> getInProgressBatchIDs()
        {
            obj = new DSL();
            DataTable dt;
            dt = obj.getScheduledBatches();
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            Dictionary<string, object> row;
            foreach (DataRow dr in dt.Rows)
            {
                row = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    row.Add(col.ColumnName, dr[col]);
                }
                rows.Add(row);
            }

            return rows;


        }
        //get Batch Details
        [HttpPost]
        public List<Dictionary<string, object>> getBatchDetails(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            //JArray v = JArray.Parse(postedString);
            string batchId = Convert.ToString(postedString);
            obj = new DSL();
            
            DataTable dt;
            dt=obj.getBatchDetails(batchId);
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            Dictionary<string, object> row;
            foreach (DataRow dr in dt.Rows)
            {
                row = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    row.Add(col.ColumnName, dr[col]);
                }
                rows.Add(row);
            }

            return rows;

        }
        //update Batch status
        public string updateBatch(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            JArray v = JArray.Parse(postedString);
            //dynamic d = JObject.Parse(Convert.ToString(postedString));
            string jobId = Convert.ToString(v[0]["jobId"]);
            string message = Convert.ToString(v[0]["message"]);
            obj = new DSL();
            string status = obj.updateBatchStatus(jobId, message);
            return status;


        }

        //validate User
        [HttpPost]
        public List<Dictionary<string, object>> validateUser(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            string base64Encoded = postedString;
            string base64Decoded;
            byte[] data = System.Convert.FromBase64String(base64Encoded);
            base64Decoded = System.Text.ASCIIEncoding.ASCII.GetString(data);
            dynamic d = JObject.Parse(Convert.ToString(base64Decoded));
            
            string username = Convert.ToString(d.username);
            string pwd = Convert.ToString(d.password);
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            Dictionary<string, object> row;
            row = new Dictionary<string, object>();
            obj = new DSL();
            using (PrincipalContext pc = new PrincipalContext(ContextType.Domain, "IEA"))
            {
                bool isValid = pc.ValidateCredentials(username, pwd, ContextOptions.Negotiate);
                if (isValid)
                {
                    using (UserPrincipal user = UserPrincipal.FindByIdentity(pc, username))
                    {
                        if (user != null)
                        {
                            
                            //string employeeID = user.EmployeeId;
                            int get_session_id = obj.getSessionID(user.SamAccountName);
                            if(get_session_id>0)
                            {
                                string Name = user.DisplayName;
                                row.Add("UserName", username);
                                row.Add("Name", Name);
                                row.Add("UserValidated", "true");
                                row.Add("UserID", user.SamAccountName);
                                row.Add("SessionID", get_session_id);
                                rows.Add(row);

                            }
                            else
                            {
                                string Name = user.DisplayName;
                                row.Add("UserName", username);
                                row.Add("Name", Name);
                                row.Add("UserValidated", "true");
                                row.Add("UserID", user.SamAccountName);
                                row.Add("SessionID", -1);
                                rows.Add(row);

                            }
                            return rows;

                        }
                        else
                        {
                            
                            row.Add("UserName", username);
                            row.Add("Name", "Not Found");
                            row.Add("UserValidated", "true");
                            row.Add("SessionID", -1);
                            rows.Add(row);
                            //string employeeID = user.EmployeeId;
                            return rows;

                        }
                    }
                }
                else
                {
                    row.Add("UserName", username);
                    row.Add("Name", "Not Found");
                    row.Add("UserValidated", "false");
                    rows.Add(row);
                    //string employeeID = user.EmployeeId;
                    return rows;
                    

                }
                
                
            }
        }
        [HttpPost]
        public List<Dictionary<string, object>> TrackEquipment(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            dynamic d = JObject.Parse(Convert.ToString(postedString));
            //JArray v = JArray.Parse(postedString);
            //dynamic d = JObject.Parse(Convert.ToString(postedString));
            string equipmentId = Convert.ToString(d.EquipmentID);
            obj = new DSL();
            DataTable dt;
            dt = obj.getEquipmentHistory(equipmentId);
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            Dictionary<string, object> row;
            foreach (DataRow dr in dt.Rows)
            {
                row = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    row.Add(col.ColumnName, dr[col]);
                }
                rows.Add(row);
            }

            return rows;

        }
        [HttpPost]
        public string disconnectSession(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            dynamic d = JObject.Parse(Convert.ToString(postedString));
            //JArray v = JArray.Parse(postedString);
            //dynamic d = JObject.Parse(Convert.ToString(postedString));
            string sessionId = Convert.ToString(d.SessionId);
            string message= Convert.ToString(d.message);
            obj = new DSL();
            int logoutStatus=obj.updateSession(sessionId, message);
            if (logoutStatus > 0)
                return "success";
            else
                return "failed";

        }
        [HttpPost]
        public string updatePhysicalDateViewpoint(HttpRequestMessage req)
        {
            var postedString = req.Content.ReadAsStringAsync().Result;
            dynamic v = JArray.Parse(postedString);
            //dynamic d = JObject.Parse(Convert.ToString(postedString));
            //dynamic d = JObject.Parse(Convert.ToString(postedString));
            //string month = Convert.ToDateTime(d.month);
            JArray toCsv = new JArray();
            JArray pushJob = new JArray();
            foreach (var equipment in v)
            {
                JObject item = new JObject();
                JObject itemPushJob = new JObject();



                // for pushing into Transit Database
                itemPushJob.Add(new JProperty("EquipmentID", Convert.ToString(equipment.EquipmentID)));
                itemPushJob.Add(new JProperty("SerialNo", equipment.SerialNo));
                itemPushJob.Add(new JProperty("Description", Convert.ToString(equipment.Description)));
                
               
                itemPushJob.Add(new JProperty("PhysicalDate", Convert.ToString(equipment.PhysicalDate)));
                

                pushJob.Add(itemPushJob);




            }
            obj = new DSL();
            try
            {


                int ViewpointDbUpdate = obj.updateEquipmentPhysicaDateViewpoint(pushJob);
                if (ViewpointDbUpdate == 1)
                    return "Success";
                else
                    return "failed";







            }
            catch (Exception ex)
            {
                return ("failed");

            }
        }

        [HttpPost]
        public string Upload()
        {
            
            string status = "";
            
            if (HttpContext.Current.Request.Files.AllKeys.Any())
            {
                // Get the uploaded image from the Files collection
                var httpPostedFile =HttpContext.Current.Request.Files["file"];
                string keyID=HttpContext.Current.Request.Form["KeyID"];
                string UploadedBy = HttpContext.Current.Request.Form["UploadedBy"];
                UploadedBy=UploadedBy.Trim('"');
                string Description = HttpContext.Current.Request.Form["Description"];
                string rootpath = ConfigurationManager.AppSettings["ViewpointUploadRootPath"];
                string uploadPath = ConfigurationManager.AppSettings["UploadPath"];
                string completepath = @rootpath + uploadPath;
                string upload_directory = DateTime.Now.Month.ToString("00") + "-" + DateTime.Now.Year.ToString();
                CreateIfMissing(completepath + upload_directory);
                
                if (httpPostedFile != null)
                {
                    // Get the complete file path
                    String fileSavePath = @completepath + upload_directory+@"\"+ httpPostedFile.FileName.ToString();

                    // Save the uploaded file to "UploadedFiles" folder
                    httpPostedFile.SaveAs(fileSavePath);
                    DSL obj = new DSL();
                   status=obj.insertAttachmentViewpoint(keyID, "From Transit: "+Description, completepath + upload_directory+"\\"+httpPostedFile.FileName.ToString(), httpPostedFile.FileName.ToString(), UploadedBy);
                   
                }
                
            }
            return status;

        }
        private string CreateIfMissing(string path)
        {
            bool folderExists = Directory.Exists(path);
            if (!folderExists)
                Directory.CreateDirectory(path);
            return path;
        }











    }
    }
