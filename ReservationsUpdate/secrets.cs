using System;
using System.Collections.Generic;
using System.Text;

namespace ReservationsUpdate
{
    public class Secrets
    {
        public static string CRMConnectionString()
        {
            return "Server=tcp:vcs-payfac-sql01.database.windows.net,1433;Initial Catalog=payfacDB;Persist Security Info=False;User ID=payfacadmin;Password=qwer1029this!;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
        }

        public static string ReportingConnectionString()
        {
            return "Server=tcp:vcs-payfac-sql01.database.windows.net,1433;Initial Catalog=payfac_tx_reports;Persist Security Info=False;User ID=payfacadmin;Password=qwer1029this!;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
        }

    }
}
