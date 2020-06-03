
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ReservationsUpdate;
using System.Data;
using System.Data.SqlClient;

namespace PayFac
{
    namespace Utilities
    {
        public class SQLFunctions
        {
            private SqlConnection conn;

            public SQLFunctions(string ConnectionString)
            {
                try
                {
                    conn = new SqlConnection(ConnectionString);
                    conn.Open();
                }
                catch (Exception e)
                {
                    conn = null;
                    Program.LogMessage("Exception connecting to SQL Server" + e.StackTrace.ToString());
                    throw e;
                }
            }

            public SQLFunctions(string Instance, string Database, string UserName, string Password, bool pooling = true)
            {
                string connectionString;
                if (pooling)
                {
                    connectionString = "Data Source = " + Instance + ";" +
                        " Initial Catalog = " + Database + ";" +
                        " Integrated Security = False; " +
                        " User ID = " + UserName + ";" +
                        " Password = " + Password + ";" +
                        " Connect Timeout = 60; " +
                        " Encrypt = True; " +
                        " TrustServerCertificate = True; " +
                        " ApplicationIntent = ReadWrite; " +
                        " MultiSubnetFailover = False;";
                }
                else
                {
                    connectionString = "Data Source = " + Instance + ";" +
                        " Initial Catalog = " + Database + ";" +
                        " Integrated Security = False; " +
                        " User ID = " + UserName + ";" +
                        " Password = " + Password + ";" +
                        " Connect Timeout = 60; " +
                        " Encrypt = True; " +
                        " Pooling = False; " +
                        " TrustServerCertificate = True; " +
                        " ApplicationIntent = ReadWrite; " +
                        " MultiSubnetFailover = False;";
                }
                conn = new SqlConnection(connectionString);
                conn.Open();
                Program.LogMessage("Opened SQL Connection");
            }

            public SQLFunctions(string Instance, string Database)
            {
                string connectionString = "Data Source = " + Instance + ";" +
                    " Initial Catalog = " + Database + ";" +
                    " Integrated Security = True; " +
                    " Connect Timeout = 60; " +
                    " Encrypt = True; " +
                    " TrustServerCertificate = True; " +
                    " ApplicationIntent = ReadWrite; " +
                    " MultiSubnetFailover = False;";

                conn = new SqlConnection(connectionString);
                conn.Open();
                Program.LogMessage("Opened SQL Connection");
            }

            public void Close()
            {
                if (conn?.State == ConnectionState.Open || conn?.State == ConnectionState.Broken)
                {
             //       conn?.Close();
                    Program.LogMessage("Closing SQL connection");
                }
            }

            ~SQLFunctions()
            {
                if (conn?.State == ConnectionState.Open || conn?.State == ConnectionState.Broken)
                {
          //          conn?.Close();
                    Program.LogMessage("Closing SQL connection");
                }
                //conn?.Close();
                //;
            }


            public SqlDataReader SQLQuery(string Query)
            {
                try
                {
                    if (conn != null)
                    {
                        SqlCommand query = new SqlCommand(Query, conn);
                        //Program.LogMessage("created query: " + Query);
                        return query.ExecuteReader();
                    }
                    else
                    {
                        throw new Exception("The SQL Connection is not established");
                    }
                }
                catch (Exception e)
                {
                    Program.LogMessage("Exception Querying SQL Server: " + e.ToString());
                    throw e;
                }
            }

            public List<Dictionary<string, object>> ListDictionarySQLQuery(string Query, int Timeout = 90, bool GetNulls = false)
            {
                List<Dictionary<string, object>> parentRow = new List<Dictionary<string, object>>();
                Dictionary<string, object> childRow;
                SqlCommand query;
                try
                {
                    if (conn != null)
                    {
                        SqlDataReader results;
                        query = new SqlCommand(Query, conn);
                        // Program.LogMessage("created query: " + Query);

                        query.CommandTimeout = Timeout;
                        results = query.ExecuteReader();

                        if (results.HasRows)
                        {
                            while (results.Read())
                            {
                                childRow = new Dictionary<string, object>();
                                for (int column = 0; column < results.FieldCount; column++)
                                {
                                    if ((results[column].GetType().Name == "DBNull") && GetNulls == true)
                                    {
                                        childRow.Add(results.GetName(column), null);
                                    }
                                    else
                                    {
                                        childRow.Add(results.GetName(column), results[column].ToString());
                                    }
                                }
                                parentRow.Add(childRow);
                            }
                        }
                        results.Close();
                        query.Dispose();

                    }
                }
                catch (Exception e)
                {
                    Program.LogMessage("Exception Querying SQL Server: " + e.ToString());
                    throw e;
                }
                return parentRow;
            }


            public int SQLExecute(string Command, int timeout = 30)
            {
                int returncode;
                try
                {
                    if (conn != null)
                    {
                        SqlCommand SQLcommand = new SqlCommand(Command, conn);
                        SQLcommand.CommandTimeout = timeout;
                        
                        returncode = SQLcommand.ExecuteNonQuery();
                        if (returncode < 0)
                            Program.LogMessage("created Command:" + Command);
                        return returncode;
                    }
                    else
                    {
                        throw new Exception("The SQL Connection is not established");
                    }
                }
                catch (Exception e)
                {
                    Program.LogMessage("Exception sending command to SQL Server: " + e.ToString());
                    throw e;
                }
            }
        }
    }
}
