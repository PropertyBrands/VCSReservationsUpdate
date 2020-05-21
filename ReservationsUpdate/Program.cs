using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using PayFac.Utilities;
using System.IO;
using System.Linq;

namespace ReservationsUpdate
{
    public class StreamlineReservations
    {
        [JsonProperty("methodName")]
        public string MethodName { get; set; }

        [JsonProperty("params")]
        public ReservationParamaters resparms { get; set; }
    }

    public class ReservationParamaters
    {
        [JsonProperty("token_key")]
        public string TokenKey { get; set; }

        [JsonProperty("token_secret")]
        public string TokenSecret { get; set; }

        [JsonProperty("return_full")]
        public string ReturnFull { get; set; }

        [JsonProperty("arriving_after")]
        public string ArriveAfter { get; set; }

        [JsonProperty("arriving_before")]
        public string ArriveBefore { get; set; }
    }

    public class VRMReservations
    {
        public string APIKey { get; set; }
        public string DateStart { get; set; }
        public string DateEnd { get; set; }
        public string ClientCode { get; set; }

    }

    class Program
    {
        const int batchsize = 500;
        const int daysback = 14;
        const int monthspollsize = 2;
        const int monthstopull = 16;
        const int numberofpulls = monthstopull / monthspollsize;  // This better be an int
        public static void LogMessage(string message)
        {
            string filename = $@"c:\logs\VacationReservations-{DateTime.Now.ToString("yyyy-MM-dd")}.log";
            try
            {
                using (StreamWriter sw = new StreamWriter(filename, true))
                {
                    sw.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} {message}");
                }
            }
            catch { }

            Console.WriteLine(message);
        }
        static string getstartdate(int pollnumber, string dateformat)
        {
            return DateTime.Now.AddDays(-daysback).AddMonths(monthspollsize * pollnumber).ToString(dateformat);
        }

        static string getenddate(int pollnumber, string dateformat)
        {
            return DateTime.Now.AddDays(-daysback).AddMonths(monthspollsize * (pollnumber + 1)).ToString(dateformat);
        }


        static void Main(string[] args)
        {
            ProcessStreamline();
            ProcessVRM();
        }
         

        private static  void ProcessStreamline()
        {
            SQLFunctions sql = new SQLFunctions(Secrets.CRMConnectionString());
            SQLFunctions sql1 = new SQLFunctions(Secrets.ReportingConnectionString());

            List<Dictionary<string, object>> slproperties = sql.ListDictionarySQLQuery("select PMC as companyname, credential1, credential2 from VacationCredentials" +
                " where gatewayname = 'streamline'");

            List<string> badones = new List<string>();

            foreach (Dictionary<string, object> slproperty in slproperties)
            {
                string company = slproperty["companyname"].ToString();
                LogMessage($"Pulling data for {company}");
                for (int dategrouppoll = 0; dategrouppoll < numberofpulls; dategrouppoll++)
                {
                    string result = StreamLineResult(slproperty["credential1"].ToString(), slproperty["credential2"].ToString(), dategrouppoll);

                    dynamic jsonresults = JsonConvert.DeserializeObject(result);
                    try
                    {
                        string test = $"{jsonresults.data.reservations.Count}";
                        LogMessage($"Pushing {test} results into SQL Server for {company}");
                        string sqlstatement = "";
                        int counter = 0;
                        foreach (var reservation in jsonresults.data.reservations)
                        {
                            reservation.propertyname = slproperty["companyname"].ToString();
                            reservation.Source = "Streamline";
                            sqlstatement += BuildSLUpsert(reservation, 17167, "Streamline", company);
                            if (counter++ % batchsize == 0)
                            {
                                RunTransaction(sql1, sqlstatement);
                                sqlstatement = "";
                            }
                        }
                        RunTransaction(sql1, sqlstatement);

                    }
                    catch (Exception)
                    {
                        badones.Add(slproperty["companyname"].ToString());
                    }
                }
            }
            if (badones.Count > 0)
                LogMessage($"Had issues with {badones.ToArray()}");
            else
                LogMessage("Processed all Streamline properties");

        }

        private static string StreamLineResult(string tokenkey, string tokensecret, int dategrouppoll)
        {
            string url = "https://web.streamlinevrs.com/api/mjson"; 

            string startdate = getstartdate(dategrouppoll, "M/d/yyyy");
            string enddate = getenddate(dategrouppoll, "M/d/yyyy");

            var payload = new StreamlineReservations
            {
                MethodName = "GetReservations",
                resparms = new ReservationParamaters
                {
                    TokenKey = tokenkey,
                    TokenSecret = tokensecret,
                    ReturnFull = "Y",
                    ArriveAfter = startdate,
                    ArriveBefore = enddate
                }
            };

             return GetHTTPResults(url, new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));
        }

        public static string BuildSLUpsert(dynamic reservation, int affiliate, string source, string PMC)
        {
            string property = reservation.location_name;
            property = NoBobbyTables(property);
            string unit = reservation.unit_name;
            unit = NoBobbyTables(unit);
            string reservationid = reservation.id;
            string reservationdate = reservation.creation_date;
            reservationdate = DateTime.Parse(reservationdate).ToString("yyyy-MM-dd");
            int statuscode = reservation.status_code;
            string reservationstatus = GetSLStatusName(statuscode);
            string arrivaldate = reservation.startdate;
            arrivaldate = DateTime.Parse(arrivaldate).ToString("yyyy-MM-dd");
            string departuredate = reservation.enddate;
            departuredate = DateTime.Parse(departuredate).ToString("yyyy-MM-dd");
            float totalprice = reservation.price_total;
            float totalpaid = reservation.price_paidsum;
            float securitydeposit = 0;
            string cancellationreason = "";

            return $"EXEC UpsertVacationReservations {affiliate},'{source}','{PMC}','{property}','{unit}','{reservationid}','{reservationdate}'," +
                $"'{reservationstatus}','{arrivaldate}','{departuredate}',{totalprice},{totalpaid},{securitydeposit},'{cancellationreason}'";
        }
        public static string GetSLStatusName(int code)
        {
            return code switch
            {
                0 => "Non Blocked Request",
                1 => "Blocked Request",
                2 => "Booked",
                4 => "Booked but Modified",
                5 => "Checked In",
                6 => "Modified and Checked In",
                8 => "Checked Out",
                9 => "Cancelled",
                10 => "No Show",
                _ => "Unknown",
            };
        }

        public static string NoBobbyTables(string input)
        {
            // remove commas for CSV compatibility and apostrophes for sql injection/sql breakage
            try
            {
                return input.Replace("'", "").Replace(",", "");
            }
            catch
            {
                return "Exception";
            }
        }

        private static void ProcessVRM()
        {
            SQLFunctions sql = new SQLFunctions(Secrets.CRMConnectionString());
            SQLFunctions sql1 = new SQLFunctions(Secrets.ReportingConnectionString());

            List<Dictionary<string, object>> vrmproperties = sql.ListDictionarySQLQuery("select PMC as companyname, credential1, credential2 from VacationCredentials" +
                " where gatewayname = 'VRM'");

            foreach (var vrmproperty in vrmproperties)
            {
                string company = vrmproperty["companyname"].ToString();
                LogMessage($"Pulling data for {company}");
                for (int dategrouppoll = 0; dategrouppoll < numberofpulls; dategrouppoll++)
                {
                    string result = VRMResult(vrmproperty["credential1"].ToString(), vrmproperty["credential2"].ToString(),dategrouppoll);

                    dynamic jsonresults = JsonConvert.DeserializeObject(result);

                    string sqlstatement = "";
                    int counter = 0;
                    foreach (var reservation in jsonresults)
                    {
                        reservation.propertyname = vrmproperty["companyname"].ToString();
                        reservation.Source = "VRM";
                        sqlstatement += BuildVRMUpsert(reservation, 17167, "VRM", company);
                        if (counter++ % batchsize == 0)
                        {
                            RunTransaction(sql1, sqlstatement);
                            sqlstatement = "";
                        }
                    }
                    RunTransaction(sql1, sqlstatement);
                }
            }
        }

        public static string BuildVRMUpsert(dynamic reservation, int affiliate, string source, string PMC)
        {
            string property = reservation.PropertyName;
            property = NoBobbyTables(property);
            string unit = reservation.PropertyName;
            unit = NoBobbyTables(unit);
            string reservationid = reservation.ReservationID;
            string reservationdate = reservation.DateReserved;
            reservationdate = DateTime.Parse(reservationdate).ToString("yyyy-MM-dd");
            //int statuscode = reservation.status_code;
            string reservationstatus = reservation.Status;
            string arrivaldate = reservation.ReservationDateStart;
            arrivaldate = DateTime.Parse(arrivaldate).ToString("yyyy-MM-dd");
            string departuredate = reservation.ReservationDateEnd;
            departuredate = DateTime.Parse(departuredate).ToString("yyyy-MM-dd");
            float totalprice = reservation.ReservationTotal;
            float totalpaid = reservation.TotalPaid;
            float securitydeposit = reservation.TotalPrepay;
            string cancellationreason = reservation.CancellationReason;

            return $"EXEC UpsertVacationReservations {affiliate},'{source}','{PMC}','{property}','{unit}','{reservationid}','{reservationdate}'," +
                $"'{reservationstatus}','{arrivaldate}','{departuredate}',{totalprice},{totalpaid},{securitydeposit},'{cancellationreason}';\r\n";
        }

        private static string VRMResult(string apikey, string clientcode, int dategrouppoll)
        {
            string url = "https://api.vrmreservations.com/core/2.0/api/Lynnbrook/GetReservationsByArrivalDate";

            var payload = new VRMReservations
            {
                APIKey = apikey,
                ClientCode = clientcode,
                DateStart = getstartdate(dategrouppoll, "yyyy-MM-dd"), // "2020-04-30",
                DateEnd = getenddate(dategrouppoll, "yyyy-MM-dd") // "2020-07-31"
            };

            return GetHTTPResults(url, new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json")); 
        }

        private static string GetHTTPResults(string url, StringContent httpContent)
        {
            string result = "";
            using (var httpClient = new HttpClient())
            {
                var httpResponse = httpClient.PostAsync(url, httpContent).GetAwaiter().GetResult();
                if (httpResponse.IsSuccessStatusCode)
                {
                    if (httpResponse.Content != null)
                    {
                        result = httpResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                    }
                }
                else
                {
                    LogMessage($"The HTTP Call failed with status code {httpResponse.StatusCode}");
                }
            }
            return result;
        }


        public static int RunTransaction(SQLFunctions sql,string sqlstatement)
        {
            try
            {
                return sql.SQLExecute($"Begin Transaction; {sqlstatement} Commit;");
            }
            catch (Exception ex)
            {
                LogMessage($"SQL Transaction blew up because {ex.Message}");
                return -1;
            }
        }

    }
}
