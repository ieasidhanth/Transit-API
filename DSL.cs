using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Xml;
using System.Web.Script.Serialization;
using System.Text;
using System.Timers;
using System.Web.Configuration;

namespace ViewPointAPI
{
    //class to access Data from viewPoint database
    public class DSL
    {
        //connection object
        
        public DSL()
        {
            //sqlconn = null;
            

        }
       
       //gets all jobs in company 53  
        public DataTable getAllJobs()
        {
            //Gets all Job with equipments
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("Select Distinct([Job]), [Description] from [Viewpoint].dbo.JCJM where Job in (SELECT Distinct([Job])  FROM[Viewpoint].[dbo].[EMEM]   where EMCo = 53) and JCCo=53; ", viewpointDBConnection);
                
                DataTable dt = new DataTable();
                adap.Fill(dt);
                viewpointDBConnection.Dispose();
                return dt;
            }
                
            
            
            


        }
        //gets equipments with job id
        public DataTable getEquipments(string JobID)
        {
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("SELECT [Equipment],[Description],[Manufacturer] from Viewpoint.dbo.EMEM where Job='" + JobID + "' and EMCo='53'", viewpointDBConnection);
                DataTable dt = new DataTable();
                adap.Fill(dt);
                viewpointDBConnection.Dispose();
                return dt;
            }

        }
        //get all location locations in compnay 53
        public DataTable getAllLocations()
        {
            //Gets all Job with equipments
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("Select EMCo,EMLoc,[Description] from Viewpoint.dbo.EMLM where EMCo='53' and Active='Y' and EMLoc in ('00','1-CALL OFF','2-JOB YARD');", viewpointDBConnection);
                DataTable dt = new DataTable();
                adap.Fill(dt);
                viewpointDBConnection.Dispose();
                return dt;
            }
               



        }
        // get all equipments from viewpoint of dept 15 and 19;ownership status as owner and company 53
        public DataTable getAllEquipments()
        {
            //SqlDataAdapter adap = new SqlDataAdapter("select VINNumber as SerialNo,a.Equipment As EquipmentID,a.[Description] As [Description],b.[Description] as JobDescription,a.Job as JobID from Viewpoint.dbo.EMEM as a join Viewpoint.dbo.JCJM as b on a.[Job] =b.Job where a.Department in ('15', '19') and a.EMCo = '53' and b.JCCo = '53' and a.OwnershipStatus = 'O'", sqlconn);
            DataTable dt = new DataTable();
            DataTable attachmentdt = new DataTable();
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap1 = new SqlDataAdapter("select KeyID,VINNumber as SerialNo,a.Equipment As EquipmentID,a.[Description] As [Description],a.Job as Job,a.Location as Location,AttachToEquip,udPhysicalDate as PhysicalDate from Viewpoint.dbo.EMEM as a  where a.Department in ('15', '19', '16') and a.EMCo = '53' and a.OwnershipStatus = 'O' and a.VINNumber is not null and a.Status='A'", viewpointDBConnection);
                adap1.Fill(dt);
                viewpointDBConnection.Dispose();
            }
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap2 = new SqlDataAdapter("SELECT [EMCo],[Equipment],[Attachments],[Description]  FROM EMEMAttachToEquip where EMCo = 53 and Equipment is not null", viewpointDBConnection);
                adap2.Fill(attachmentdt);
                viewpointDBConnection.Close();
            }
            Dictionary<string, string> jobcodes = fetchJobCodes();
            Dictionary<string, string> locationcodes = fetchLocationCodes();
            dt.Columns.Add("JobID", typeof(string));
            dt.Columns.Add("JobDescription", typeof(string));
            dt.Columns.Add("Locked", typeof(string));
            dt.Columns.Add("Attachment", typeof(string));
            dt.Columns.Add("AttachmentDesc", typeof(string));
            dt.Columns.Add("AttachmentList", typeof(string));
            foreach (DataRow dr in dt.Rows)
            {
                if(Convert.ToString(dr["Location"])=="" && Convert.ToString(dr["Job"])!="")
                {
                    dr["JobID"] = dr["Job"];
                    string JobDesc = "";
                    jobcodes.TryGetValue(Convert.ToString(dr["Job"]), out JobDesc);
                    dr["JobDescription"] = JobDesc;

                }
                else if(Convert.ToString(dr["Job"]) == "" && Convert.ToString(dr["Location"]) != "")
                {
                    dr["JobID"] = dr["Location"];
                    string JobDesc = "";
                    locationcodes.TryGetValue(Convert.ToString(dr["Location"]), out JobDesc);
                    dr["JobDescription"] = JobDesc;

                }
                else
                {
                    dr["JobID"] = "";
                    dr["JobDescription"] = "";

                }
                dr["Locked"] = "false";
                //if(Convert.ToString(dr["AttachToEquip"])!="")
                //{
                //    dr["Attachment"] = "true";
                //    var tempAttachDesc = "";
                //    DataRow[] filteredRows = dt.Select("EquipmentID='" + dr["AttachToEquip"] + "'");
                //    foreach (DataRow updaterow in filteredRows)
                //    {
                //        //dt.Rows.Remove(deleterow);
                //        updaterow["AttachToEquip"] = dr["EquipmentID"];
                //        updaterow["Attachment"] = "true";
                //        updaterow["AttachmentDesc"] = dr["Description"];
                //        tempAttachDesc = Convert.ToString(updaterow["Description"]);

                //    }
                //    dr["AttachmentDesc"] = tempAttachDesc;

                //}
                //else
                //{
                //    dr["Attachment"] = "false";

                //}
                string equipmentID = Convert.ToString(dr["EquipmentID"]);
                DataRow[] foundrows= null;

                foundrows = attachmentdt.Select("Equipment='" + equipmentID + "'");
                if (foundrows.Length>0)
                {
                    
                    string attchList = "";
                    string attchDescList = "";
                    foreach(DataRow row in foundrows)
                    {
                        attchList = attchList + row["Attachments"] + "#";
                        attchDescList = attchDescList + row["Description"] + "$";
                        dr["Attachment"] = "true";

                    }
                    
                    dr["AttachmentList"] = attchList.Substring(0, attchList.LastIndexOf('#'));
                    
                    dr["AttachToEquip"] = attchList.Substring(0, attchList.LastIndexOf('#'));
                    dr["AttachmentDesc"] = attchDescList.Substring(0, attchDescList.LastIndexOf('$'));
                }
                else
                {
                    dr["Attachment"] = "false";
                    dr["AttachmentList"] = "";
                    dr["AttachToEquip"] = Convert.ToString("");
                    dr["AttachmentDesc"] = Convert.ToString("");

                }

            }
            //Uncomment below line for lock functionality
            //dt = filterInProcessRows(dt);
            return dt;
        }




        // get all equipments from viewpoint of dept 15 and 19;ownership status as owner and company 53 and also if they are in a batch fetches the details
        public DataTable getAllEquipmentsWBatchDetails()
        {
            DataTable dt = new DataTable();
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap1 = new SqlDataAdapter("SELECT SerialNo,EquipmentID ,[Description],Job,Location,BatchId as CurrentBatch FROM EMEM_wBatch", viewpointDBConnection);
                adap1.Fill(dt);
                viewpointDBConnection.Dispose();
            }
                
            Dictionary<string, string> jobcodes = fetchJobCodes();
            Dictionary<string, string> locationcodes = fetchLocationCodes();
            dt.Columns.Add("JobID", typeof(string));
            dt.Columns.Add("JobDescription", typeof(string));
            dt.Columns.Add("Locked", typeof(string));
            foreach (DataRow dr in dt.Rows)
            {
                if (Convert.ToString(dr["Location"]) == "" && Convert.ToString(dr["Job"]) != "")
                {
                    dr["JobID"] = dr["Job"];
                    string JobDesc = "";
                    jobcodes.TryGetValue(Convert.ToString(dr["Job"]), out JobDesc);
                    dr["JobDescription"] = JobDesc;

                }
                else if (Convert.ToString(dr["Job"]) == "" && Convert.ToString(dr["Location"]) != "")
                {
                    dr["JobID"] = dr["Location"];
                    string JobDesc = "";
                    locationcodes.TryGetValue(Convert.ToString(dr["Location"]), out JobDesc);
                    dr["JobDescription"] = JobDesc;

                }
                else
                {
                    dr["JobID"] = "";
                    dr["JobDescription"] = "";

                }
                if(dr["CurrentBatch"]==DBNull.Value)
                {
                    dr["Locked"] = "false";

                }
                else
                {
                    dr["Locked"] = "true";

                }
                

            }

           // dt = filterInProcessRowsV2(dt);
            return dt;
        }

        public DataTable filterInProcessRowsV2(DataTable dt)
        {
            DataTable transactTable = new DataTable();
            using (SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("select * from dbo.TransitTransactions where Locked='true'", transitDBConnection);
                adap.Fill(transactTable);
                transitDBConnection.Dispose();
            }
            Console.WriteLine(transactTable);
            foreach (DataRow row in transactTable.Rows)
            {
                string serialNo = Convert.ToString(row["SerialNO"]);
                string equipmentId = Convert.ToString(row["EquipmentID"]);
                string TransactLoc = Convert.ToString(row["TransferLocID"]);
                //DataRow[] filteredRows=dt.Select("EquipmentID='"+equipmentId+ "' AND JobID <> '"+TransactLoc+"'");
                DataRow[] filteredRows = dt.Select("EquipmentID='" + equipmentId + "'");

                // DataTable temp = dt.Select("EquipmentID='" + equipmentId + "' AND JobID <>'" + TransactLoc + "'").CopyToDataTable();
                // Console.WriteLine(temp);



                foreach (DataRow updaterow in filteredRows)
                {
                    //dt.Rows.Remove(deleterow);
                    if(updaterow["CurrentBatch"]== DBNull.Value)
                    {
                        updaterow["Locked"] = "false";
                        row["Status"] = "Completed";
                        row["Locked"] = "false";


                    }
                    else
                    { 
                        
                      updaterow["Locked"] = "true";
                        row["Status"] = "InProgress";
                        row["Locked"] = "true";
                    }


                }


            }
            UpdateTransit(transactTable);
            return dt;
        }


        public void UpdateTransit(DataTable Transact)
        {
            using (SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                SqlBulkCopy bulkCopy = new SqlBulkCopy(transitDBConnection);
                bulkCopy.DestinationTableName = "dbo.TransitTransactions";
                try
                {
                    bulkCopy.WriteToServer(Transact);

                }
                catch (SqlException ex)
                {
                    transitDBConnection.Dispose();

                }
                finally
                {
                    transitDBConnection.Dispose();
                }
            }
                


        }



        //to filter out rowsV1 lock checking in WebAPI

        public DataTable filterInProcessRows(DataTable dt)
        {
            
            DataTable transactTable = new DataTable();
            using (SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("select * from dbo.TransitTransactions where Locked='true'", transitDBConnection);
                adap.Fill(transactTable);
                Console.WriteLine(transactTable);
                transitDBConnection.Dispose();
            }
               
            foreach(DataRow row in transactTable.Rows)
            {
                string serialNo = Convert.ToString(row["SerialNO"]);
                string equipmentId = Convert.ToString(row["EquipmentID"]);
                string TransactLoc = Convert.ToString(row["TransferLocID"]);
                //DataRow[] filteredRows=dt.Select("EquipmentID='"+equipmentId+ "' AND JobID <> '"+TransactLoc+"'");
                DataRow[] filteredRows = dt.Select("SerialNo='" + serialNo +"'");

                // DataTable temp = dt.Select("EquipmentID='" + equipmentId + "' AND JobID <>'" + TransactLoc + "'").CopyToDataTable();
                // Console.WriteLine(temp);



                foreach (DataRow updaterow in filteredRows)
                {
                    //dt.Rows.Remove(deleterow);
                    updaterow["Locked"] = "true";


                }
                
                
            }
            return dt;
        }
        public Dictionary<string, string> fetchLocationCodes()
        {
            DataTable dt = new DataTable();
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("select EMLoc as LocCode, Description as LocDesc from EMLM where EMCo='53'and Active='Y'", viewpointDBConnection);
                adap.Fill(dt);
                viewpointDBConnection.Dispose();

            }
                
            
            Dictionary<string, string> d = new Dictionary<string, string>();
            
            foreach(DataRow dr in dt.Rows)
            {
                string key = dr["LocCode"].ToString();
                string value = dr["LocDesc"].ToString();
                if(!d.ContainsKey(key))
                {
                    d.Add(key, value);
                }
               

            }
            return d;


        }

        public Dictionary<string, string> fetchJobCodes()
        {
            DataTable dt = new DataTable();
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("select Job as JobCode,Description as JobDesc from JCJM where JCCo='53'", viewpointDBConnection);
                adap.Fill(dt);
                viewpointDBConnection.Dispose();

            }
               
           
            Dictionary<string, string> d = new Dictionary<string, string>();
           
            foreach (DataRow dr in dt.Rows)
            {
                string key = dr["JobCode"].ToString();
                string value = dr["JobDesc"].ToString();
                if (!d.ContainsKey(key))
                {
                    d.Add(key, value);
                }


            }
            return d;

        }




        public string convertToCSV(JArray json)
        {
            
            XmlNode xml = JsonConvert.DeserializeXmlNode("{records:{record:" + json + "}}");
            
            XmlDocument xmldoc = new XmlDocument();
            //Create XmlDoc Object
            xmldoc.LoadXml(xml.InnerXml);
            //Create XML Steam 
            var xmlReader = new XmlNodeReader(xmldoc);
            DataSet dataSet = new DataSet();
            //Load Dataset with Xml
            dataSet.ReadXml(xmlReader);
            //return single table inside of dataset

            //string csv = ToCSV(dataSet.Tables[0],",");
            string csv = table_to_csv(dataSet.Tables[0]);
            return csv;

        }

        public string ToCSV(DataTable table, string delimator)
        {
            var result = new StringBuilder();
            for (int i = 0; i < table.Columns.Count; i++)
            {
                result.Append(table.Columns[i].ColumnName);
                result.Append(i == table.Columns.Count - 1 ? "\n" : delimator);
            }
            foreach (DataRow row in table.Rows)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    result.Append(row[i].ToString());
                    result.Append(i == table.Columns.Count - 1 ? "\n" : delimator);
                }
            }
            return result.ToString().TrimEnd(new char[] { '\r', '\n' });
            //return result.ToString();
        }


        public string table_to_csv(DataTable table)
        {
            string file = "";

            //foreach (DataColumn col in table.Columns)
            //    file = string.Concat(file, col.ColumnName, ",");

            //file = file.Remove(file.LastIndexOf(','), 1);
            //file = string.Concat(file, "\r\n");

            foreach (DataRow row in table.Rows)
            {
                foreach (object item in row.ItemArray)
                    file = string.Concat(file, item.ToString(), ",");

                file = file.Remove(file.LastIndexOf(','), 1);
                file = string.Concat(file, "\r\n");
            }

            return file;
        }
        public string UpdatevEMLocationHistory(JArray equipmentList)
        {
            string check_faulty_equipments=validateEquipments(equipmentList, Convert.ToString(equipmentList[0]["jobDate"]));
            if (check_faulty_equipments.Length == 0)
            {
                dynamic v = equipmentList;
                DataTable dt = new DataTable();
                using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
                {
                    viewpointDBConnection.Open();
                    SqlDataAdapter adap = new SqlDataAdapter("Select * from vEMLocationHistory", viewpointDBConnection);
                    adap.Fill(dt);




                    SqlCommandBuilder builder = new SqlCommandBuilder(adap);

                    // add rows to dataset

                    builder.GetInsertCommand();
                    foreach (var equipment in v)
                    {
                        DataRow row = dt.NewRow();
                        try
                        {
                            row["EMCo"] = 53;
                            row["Equipment"] = equipment.Equipment;
                            row["Sequence"] = GetSequenceNo(Convert.ToString(equipment.Equipment));
                            row["DateIn"] = Convert.ToDateTime(equipment.jobDate);
                            row["TimeIn"] = Convert.ToDateTime(equipment.jobDate);
                            row["ToJCCo"] = 53;
                            if (equipment.ToJob == "")
                            {
                                row["ToLocation"] = equipment.ToLocation;
                                row["ToJob"] = null;

                            }
                            else if (equipment.ToLocation == "")
                            {
                                row["ToJob"] = equipment.ToJob;
                                row["ToLocation"] = null;

                            }
                            row["Memo"] = "";
                            row["EstDateOut"] = DBNull.Value;
                            row["DateTimeIn"] = Convert.ToDateTime(equipment.jobDate);
                            row["Notes"] = "";
                            row["UniqueAttchID"] = DBNull.Value;
                            row["CreatedBy"] = equipment.CreatedByUserID;
                            row["CreatedDate"] = DateTime.Now;
                            row["ModifiedBy"] = equipment.CreatedByUserID;
                            row["ModifiedDate"] = DBNull.Value;
                            dt.Rows.Add(row);


                        }
                        catch (Exception ex)
                        {

                        }





                    }
                    try
                    {
                        adap.Update(dt);
                        return "1";

                    }
                    catch (SqlException ex)
                    {
                        return "-1";



                    }
                    finally
                    {
                        viewpointDBConnection.Dispose();
                    }
                }
            }
            else
            {
                return check_faulty_equipments;
            }
            
            

        }



        //Updated after 
        public string UpdatevEMLocationHistoryV2(JArray equipmentList)
        {
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                string check_faulty_equipments = validateEquipments(equipmentList, Convert.ToString(equipmentList[0]["jobDate"]));
                if (check_faulty_equipments.Length == 0)
                {
                    dynamic v = equipmentList;
                    bool error = false;
                    foreach (var equipment in v)
                    {

                        SqlCommand cmd = new SqlCommand();
                        cmd.Connection = viewpointDBConnection;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = "Insert into vEMLocationHistory(EMCo,Equipment,Sequence,DateIn,TimeIn,ToJCCo,ToJob,ToLocation,Memo,EstDateOut,DateTimeIn,Notes,UniqueAttchID,CreatedBy,CreatedDate,ModifiedBy,ModifiedDate) VALUES (@EMCo,@Equipment,@Sequence,@DateIn,@TimeIn,@ToJCCo,@ToJob,@ToLocation,@Memo,@EstDateOut,@DateTimeIn,@Notes,@UniqueAttchID,@CreatedBy,@CreatedDate,@ModifiedBy,@ModifiedDate)";
                        cmd.Parameters.AddWithValue("@EMCo", 53);
                        cmd.Parameters.AddWithValue("@Equipment", Convert.ToString(equipment.Equipment));
                        UInt32 SeqNo = GetSequenceNo(Convert.ToString(equipment.Equipment));
                        cmd.Parameters.AddWithValue("@Sequence", Convert.ToInt32(SeqNo));
                        cmd.Parameters.AddWithValue("@DateIn", Convert.ToDateTime(equipment.jobDate));
                        cmd.Parameters.AddWithValue("@TimeIn", Convert.ToDateTime(equipment.jobDate));
                        cmd.Parameters.AddWithValue("@ToJCCo", 53);
                        if (equipment.ToJob == "")
                        {
                            cmd.Parameters.AddWithValue("@ToLocation", Convert.ToString(equipment.ToLocation));
                            cmd.Parameters.AddWithValue("@ToJob", DBNull.Value);

                        }
                        else if (equipment.ToLocation == "")
                        {
                            cmd.Parameters.AddWithValue("@ToLocation", DBNull.Value);
                            cmd.Parameters.AddWithValue("@ToJob", Convert.ToString(equipment.ToJob));

                        }
                        cmd.Parameters.AddWithValue("@Memo", "");
                        cmd.Parameters.AddWithValue("@EstDateOut", DBNull.Value);
                        cmd.Parameters.AddWithValue("@DateTimeIn", Convert.ToDateTime(equipment.jobDate));
                        cmd.Parameters.AddWithValue("@Notes", "");
                        cmd.Parameters.AddWithValue("@UniqueAttchID", DBNull.Value);
                        cmd.Parameters.AddWithValue("@CreatedBy", Convert.ToString(equipment.CreatedByUserID));
                        DateTime Ctime = DateTime.Now;
                        cmd.Parameters.AddWithValue("@CreatedDate", Ctime);
                        cmd.Parameters.AddWithValue("@ModifiedBy", Convert.ToString(equipment.CreatedByUserID));
                        cmd.Parameters.AddWithValue("@ModifiedDate", DBNull.Value);
                        try
                        {
                            int recordsAffected = cmd.ExecuteNonQuery();
                            if (recordsAffected > 0)
                            {
                                if (Convert.ToString(equipment.HasAttachment) == "true")

                                {
                                    string alist = Convert.ToString(equipment.AttachmentList);
                                    string[] list = null;
                                    if (alist.Contains('#'))
                                    {

                                        list = alist.Split('#');
                                    }
                                    else
                                    {
                                        list = new string[1];
                                        list[0] = alist;
                                    }
                                    foreach (string attachment in list)
                                    {
                                        int recentSequenceNo = Convert.ToInt32(SeqNo);
                                        UInt32 LocationHistoryID = GetLocationHistoryID(Convert.ToString(equipment.Equipment), recentSequenceNo);
                                        int AttachmentInsertStatus = InsertAttachedEquipment(Convert.ToInt32(LocationHistoryID), Convert.ToInt32(recentSequenceNo), Convert.ToString(equipment.Equipment), attachment, Convert.ToString(equipment.CreatedByUserID), Ctime);
                                        if (!(AttachmentInsertStatus > 0))
                                        {
                                            error = true;
                                            break;
                                        }
                                        else
                                        {
                                            error = false;

                                        }
                                    }
                                }


                            }
                            else
                            {
                                error = true;
                                break;
                            }
                        }
                        catch (SqlException ex)
                        {
                            viewpointDBConnection.Dispose();
                            return "-1";

                        }
                        catch (Exception ex)
                        {
                            viewpointDBConnection.Dispose();
                            return "-1";

                        }









                    }
                    if (error == true)
                    {
                        viewpointDBConnection.Dispose();
                        return "-1";
                    }
                       
                    else
                    {
                        viewpointDBConnection.Dispose();
                        return "1";

                    }
                        

                }
                else
                {
                    viewpointDBConnection.Dispose();
                    return check_faulty_equipments;
                }
            }


        }

        public UInt32 GetLocationHistoryID(string equipmentID, int SequenceNo)
        {
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlCommand cmd = new SqlCommand("select LocationHistoryId from vEMLocationHistory where Equipment='" + equipmentID + "' and Sequence=" + SequenceNo + ";", viewpointDBConnection);
                try
                {
                    UInt32 LocationHistoryID = Convert.ToUInt32(cmd.ExecuteScalar());
                    viewpointDBConnection.Dispose();
                    return LocationHistoryID;

                }
                catch (SqlException ex)
                {
                    viewpointDBConnection.Dispose();
                    return Convert.ToUInt32(-1);
                }

            }


        }
        public int InsertAttachedEquipment(Int32 LocHistoryID, Int32 SeqNo, string equipmentID, string attachedEquip,string createdBy, DateTime createDate)
        {
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = viewpointDBConnection;
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = "Insert into vEMLocationHistoryAttach(LocationHistoryId,EMCo,Equipment,Sequence,AttachedEquipment,AttachedSequence,Memo,OverrideDateTime,DateIn,TimeIn,CreatedBy,CreatedDate,ModifiedBy,ModifiedDate,UniqueAttchID) VALUES (@LocationHistoryId,@EMCo,@Equipment,@Sequence,@AttachedEquipment,@AttachedSequence,@Memo,@OverrideDateTime,@DateIn,@TimeIn,@CreatedBy,@CreatedDate,@ModifiedBy,@ModifiedDate,@UniqueAttchID)";
                cmd.Parameters.AddWithValue("@LocationHistoryId", LocHistoryID);
                cmd.Parameters.AddWithValue("@EMCo", 53);
                cmd.Parameters.AddWithValue("@Equipment", equipmentID);
                cmd.Parameters.AddWithValue("@Sequence", SeqNo);
                cmd.Parameters.AddWithValue("@AttachedEquipment", attachedEquip);
                cmd.Parameters.AddWithValue("@AttachedSequence", 1);
                cmd.Parameters.AddWithValue("@Memo", "");
                cmd.Parameters.AddWithValue("@OverrideDateTime", 'N');
                cmd.Parameters.AddWithValue("@DateIn", DBNull.Value);
                cmd.Parameters.AddWithValue("@TimeIn", DBNull.Value);
                cmd.Parameters.AddWithValue("@CreatedBy", createdBy);
                cmd.Parameters.AddWithValue("@CreatedDate", createDate);
                cmd.Parameters.AddWithValue("@ModifiedBy", DBNull.Value);
                cmd.Parameters.AddWithValue("@ModifiedDate", DBNull.Value);
                cmd.Parameters.AddWithValue("@UniqueAttchID", DBNull.Value);
                try
                {
                    int recordsAffected = cmd.ExecuteNonQuery();
                    viewpointDBConnection.Dispose();
                    return recordsAffected;

                }
                catch (SqlException ex)
                {
                    viewpointDBConnection.Dispose();
                    return -1;
                }
            }

        }


        






        public string validateEquipments(JArray equipmentList, string DateIn)
        {
            string eList = "";
            dynamic v = equipmentList;
            foreach (var equipment in v)
            {
                eList = eList+"'" + equipment.Equipment + "',";
                




            }
            string temp=eList.Substring(0, eList.LastIndexOf(','));
            DataTable dt = new DataTable();
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                string query = "select t1.Equipment,t1.DateTimeIn,t2.[Description] From (select a.Equipment,a.DateTimeIn from  Viewpoint.dbo.vEMLocationHistory a INNER JOIN (select Equipment, MAX(Sequence) AS Seq from Viewpoint.dbo.vEMLocationHistory  where Equipment in (" + temp + ") group by Equipment) b ON a.Equipment = b.Equipment AND a.Sequence = b.Seq where DateTimeIn >= '" + Convert.ToDateTime(DateIn) + "') t1 JOIN Viewpoint.dbo.EMEM t2 on t1.Equipment=t2.Equipment where t2.EMCo='53'";
                SqlDataAdapter adap = new SqlDataAdapter(query, viewpointDBConnection);
                try
                {
                    adap.Fill(dt);
                    viewpointDBConnection.Dispose();

                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Error");
                    viewpointDBConnection.Dispose();
                    return "failed";

                }
            }
               
           
            

            
            string faultyEquipList = "";
            foreach(DataRow dr in dt.Rows)
            {
                faultyEquipList=faultyEquipList+Convert.ToString(dr["Equipment"])+"#"+Convert.ToString(dr["Description"])+"#"+ Convert.ToString(dr["DateTimeIn"]) + "$";

            }
            if(faultyEquipList.Length>0)
            {
                return faultyEquipList.Substring(0, faultyEquipList.LastIndexOf('$'));

            }
            else
            {
                return faultyEquipList;
            }
            
                
                
           }



        //get each equipment next Sequence no;
        public UInt32 GetSequenceNo(string equipmentID)
        {
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlCommand cmd = new SqlCommand("select max(Sequence) from vEMLocationHistory where Equipment='" + equipmentID + "';", viewpointDBConnection);
                UInt32 nextSequence;
                try
                {
                    var returnedVal = cmd.ExecuteScalar();
                    if (returnedVal == DBNull.Value)
                    {
                        nextSequence = 1;

                    }
                    else
                    {
                        nextSequence = Convert.ToUInt32(cmd.ExecuteScalar()) + 1;

                    }
                    viewpointDBConnection.Dispose();
                    return nextSequence;

                }
                catch (SqlException ex)
                {
                    viewpointDBConnection.Dispose();
                    return Convert.ToUInt32(-1);
                }
                catch (Exception ex)
                {
                    viewpointDBConnection.Dispose();
                    return Convert.ToUInt32(-1);

                }
            }
            

        }
        public int SchdeuleJob(JArray json)
        {
            using (SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                SqlBulkCopy bulkCopy = new SqlBulkCopy(transitDBConnection);
                DataTable dt = new DataTable();
                DataColumn ID = new DataColumn("ID", typeof(Int32));
                DataColumn JobID = new DataColumn("JobID", typeof(Int32));
                DataColumn SerialNo = new DataColumn("SerialNo", typeof(string));
                DataColumn EquipmentID = new DataColumn("EquipmentID", typeof(string));
                DataColumn EquipmentName = new DataColumn("EquipmentName", typeof(string));
                DataColumn TransferLocID = new DataColumn("TransferLocID", typeof(string));
                DataColumn TransferLocName = new DataColumn("TransferLocName", typeof(string));
                DataColumn jobDate = new DataColumn("jobDate", typeof(string));
                DataColumn CreatedBY = new DataColumn("CreatedBY", typeof(string));
                DataColumn CreatedTime = new DataColumn("CreatedTime", typeof(string));
                DataColumn Status = new DataColumn("Status", typeof(string));
                DataColumn Locked = new DataColumn("Locked", typeof(string));
                dt.Columns.Add(ID);
                dt.Columns.Add(JobID);
                dt.Columns.Add(SerialNo);
                dt.Columns.Add(EquipmentID);
                dt.Columns.Add(EquipmentName);
                dt.Columns.Add(TransferLocID);
                dt.Columns.Add(TransferLocName);
                dt.Columns.Add(jobDate);
                dt.Columns.Add(CreatedBY);
                dt.Columns.Add(CreatedTime);
                dt.Columns.Add(Status);
                dt.Columns.Add(Locked);
                SqlCommand cmd = new SqlCommand("select max(JobID) from dbo.TransitTransactions;", transitDBConnection);
                int nextJobId = ((int)cmd.ExecuteScalar()) + 1;
                foreach (var equipment in json)
                {

                    DataRow row1 = dt.NewRow();
                    row1["JobID"] = nextJobId;
                    row1["SerialNo"] = equipment["SerialNo"];
                    row1["EquipmentID"] = equipment["EquipmentID"];
                    row1["EquipmentName"] = equipment["EquipmentDescription"];
                    row1["TransferLocID"] = equipment["TransferLocID"];
                    row1["TransferLocName"] = equipment["TransferLocName"];
                    row1["jobDate"] = equipment["jobDate"];
                    row1["CreatedBY"] = equipment["CreatedBY"];
                    row1["CreatedTime"] = equipment["CreatedTime"];
                    row1["Status"] = "Processed";
                    row1["Locked"] = "";
                    dt.Rows.Add(row1);
                    //nextJobId = nextJobId + 1;

                }




                bulkCopy.DestinationTableName = "dbo.TransitTransactions";
                try
                {
                    bulkCopy.WriteToServer(dt);

                }
                catch (SqlException ex)
                {
                    transitDBConnection.Dispose();
                    return -1;

                }
                transitDBConnection.Dispose();
                return 1;
            }
            
        }


        public DataTable getScheduledBatches()
        {
            using(SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("select distinct(JobID),createdTime,CreatedBy from [dbo].[TransitTransactions] where JobID<>1000 order by JobID desc", transitDBConnection);
                DataTable dt = new DataTable();
                adap.Fill(dt);
                transitDBConnection.Dispose();
                return dt;
            }
            
        }
        public DataTable getBatchDetails(string batchId)
        {
            using (SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("Select * from [dbo].[TransitTransactions] where JobID='" + batchId + "'", transitDBConnection);
                DataTable dt = new DataTable();
                adap.Fill(dt);
                transitDBConnection.Dispose();
                return dt;
            }

        }
        public string updateBatchStatus(string jobID, string message)
        {
            using (SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("Select * from [dbo].[TransitTransactions] where JobID='" + jobID + "'", transitDBConnection);
                DataTable dt = new DataTable();
                adap.Fill(dt);
                foreach (DataRow row in dt.Rows)
                {
                    if (message == "Cancel")
                    {
                        row["Status"] = "Cancelled";
                        row["Locked"] = "false";

                    }
                    else if (message == "Completed")
                    {
                        row["Status"] = "Processed";
                        row["Locked"] = "false";
                    }
                }
                SqlCommandBuilder builder = new SqlCommandBuilder(adap);
                adap.UpdateCommand = builder.GetUpdateCommand();
                try
                {
                    adap.Update(dt);
                    dt.AcceptChanges();
                    transitDBConnection.Dispose();
                    return "Success";

                }
                catch (SqlException ex)
                {
                    transitDBConnection.Dispose();
                    return "failure";

                }
            }
            

        }
        //Fetch equipment transfer history
        public DataTable getEquipmentHistory(string equipmentID)
        {
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlDataAdapter adap = new SqlDataAdapter("select Top(10)* from EMLocationHistory where Equipment='" + equipmentID + "' and EMCo=53 order by Sequence desc", viewpointDBConnection);
                DataTable dt = new DataTable();
                adap.Fill(dt);
                adap.Dispose();
                Dictionary<string, string> jobList = fetchJobCodes();
                Dictionary<string, string> locationList = fetchLocationCodes();
                dt.Columns.Add("JobDesc", typeof(string));
                dt.Columns.Add("LocationDesc", typeof(string));
                foreach (DataRow dr in dt.Rows)
                {
                    if (Convert.ToString(dr["ToLocation"]) == "" && Convert.ToString(dr["ToJob"]) != "")
                    {

                        string JobDesc = "";
                        jobList.TryGetValue(Convert.ToString(dr["ToJob"]), out JobDesc);
                        dr["JobDesc"] = JobDesc;
                        dr["LocationDesc"] = "";

                    }
                    else if (Convert.ToString(dr["ToJob"]) == "" && Convert.ToString(dr["ToLocation"]) != "")
                    {
                        string LocDesc = "";
                        locationList.TryGetValue(Convert.ToString(dr["ToLocation"]), out LocDesc);
                        dr["LocationDesc"] = LocDesc;
                        dr["JobDesc"] = "";

                    }
                    else
                    {
                        dr["JobDesc"] = "";
                        dr["LocationDesc"] = "";

                    }


                }
                viewpointDBConnection.Dispose();
                return dt;
            }
        }

        public int getSessionID(string userID)
        {
            using (SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                string query = "Insert into User_Activity Values('" + userID + "','" + DateTime.Now + "','','Active');SELECT SCOPE_IDENTITY();";
                SqlCommand cmd = new SqlCommand(query, transitDBConnection);
                int status = Convert.ToInt32(cmd.ExecuteScalar());
                if (status > 0)
                {
                    transitDBConnection.Dispose();
                    return status;

                }
                else
                {
                    transitDBConnection.Dispose();
                    return -1;

                }
            }
            
        }

        public int updateSession(string sessionID,string message)
        {
            using (SqlConnection transitDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["TransitDBConnection"].ToString()))
            {
                transitDBConnection.Open();
                string query = "Update User_Activity SET User_Status='" + message + "', log_out_time='" + DateTime.Now + "' where session_ID=" + sessionID + ";";
                SqlCommand cmd = new SqlCommand(query, transitDBConnection);
                int status = cmd.ExecuteNonQuery();
                if (status > 0)
                {
                    transitDBConnection.Dispose();
                    return status;

                }
                else
                {
                    transitDBConnection.Dispose();
                    return -1;

                }
            }

        }
        public int updateEquipmentPhysicaDateViewpoint(JArray json)
        {
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                Boolean status = false;
                foreach (JObject eq in json)
                {
                    status = updatePhysicalDate(eq,viewpointDBConnection);
                    if (status == false)
                    {
                        viewpointDBConnection.Dispose();
                        return -1;

                    }

                }
                if (status)
                {
                    viewpointDBConnection.Dispose();
                    return 1;
                }
                    
                else
                {
                    viewpointDBConnection.Dispose();
                    return -1;
                }
                    
            }


        }
        public Boolean updatePhysicalDate(JObject equipment, SqlConnection viewpointDBConnection)
        {

            string equipmentID = Convert.ToString(equipment["EquipmentID"]);
            string PhysicalDate = Convert.ToString(equipment["PhysicalDate"]);
            
                
                SqlCommand cmd = new SqlCommand("update EMEM set [udPhysicalDate]= @PhyiscalDate from EMEM where [EMCo]=53 and [Equipment]=@EquipmentID", viewpointDBConnection);
                if (PhysicalDate.Length == 0)
                {
                    cmd.Parameters.AddWithValue("@PhyiscalDate", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@PhyiscalDate", PhysicalDate);

                }

                cmd.Parameters.AddWithValue("@EquipmentID", equipmentID);
                try
                {
                    if ((cmd.ExecuteNonQuery()) > 0)
                    {
                        
                        return true;

                    }
                    else
                    {
                        
                        return false;
                    }

                }
                catch (SqlException ex)
                {
                    
                    return false;
                }

            

        }
        public string insertAttachmentViewpoint(string KeyID, string desc,string uploadedFilePath, string fileName, string uploaded_by)
        {
            
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                SqlCommand cmd = new SqlCommand("[dbo].[vspHQATInsert]", viewpointDBConnection);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@hqco", 53);
                cmd.Parameters.AddWithValue("@formname", "EMEquipment");
                cmd.Parameters.AddWithValue("@keyfield", "KeyID="+KeyID);
                cmd.Parameters.AddWithValue("@description", desc);
                cmd.Parameters.AddWithValue("@addedby", uploaded_by);
                DateTime adddate = DateTime.Now;
                string sqlFormattedDate = adddate.ToString("yyyy-MM-dd HH:mm:ss.fff");
                cmd.Parameters.AddWithValue("@adddate", sqlFormattedDate);
                cmd.Parameters.AddWithValue("@docname", @uploadedFilePath);
                cmd.Parameters.AddWithValue("@tablename", "EMEM");
                cmd.Parameters.AddWithValue("@origfilename", fileName);
                var attid = cmd.Parameters.Add("@attid", SqlDbType.Int);
                attid.Direction = ParameterDirection.Output;
                var uniqueattchid = cmd.Parameters.Add("@uniqueattchid", SqlDbType.UniqueIdentifier);
                uniqueattchid.Direction = ParameterDirection.Output;
                cmd.Parameters.AddWithValue("@docattchyn", 'N');
                cmd.Parameters.AddWithValue("@createAsStandAloneAttachment", 'N');
                cmd.Parameters.AddWithValue("@attachmentTypeID", 50014);
                cmd.Parameters.AddWithValue("@IsEmail", 'N');
                var msg = cmd.Parameters.Add("@msg", SqlDbType.VarChar);
                msg.Direction = ParameterDirection.Output;
                msg.Size = 100;
                try
                {
                    cmd.ExecuteNonQuery();
                    var AttributeID = attid.Value;
                    var message = msg.Value;
                    var guid = uniqueattchid.Value;
                    if(guid!=null)
                    {
                        return AttributeID + "#" + message + "#" + guid;

                    }
                    else
                    {
                        return "failed";
                    }
                    

                }
                catch (SqlException ex)
                {
                    viewpointDBConnection.Dispose();
                    return "failed";
                }

            }
        }

        public int updatePhysicalDateBatch(JArray equipArray)
        {
            using (SqlConnection viewpointDBConnection = new SqlConnection(WebConfigurationManager.ConnectionStrings["viewPointDBConnection"].ToString()))
            {
                viewpointDBConnection.Open();
                return 1;
            }
        }



    }
}