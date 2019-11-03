using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AI.Infrastructure.Writers.Infrastructure;

namespace AI.Infrastructure.Writers
{
    public class GeneralDataWriter : IDataWriter<IList<string>>
    {
        string connectionString;
        public GeneralDataWriter(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public void WriteData(IList<string> data)
        {
            var con = new SqlConnection(connectionString);
            con.Open();
            var cmd = new SqlCommand();
            cmd.Connection = con;
            var cnt = 0;
            foreach (var cmdText in data)
            {
                cmd.CommandText = cmdText;
                cmd.ExecuteNonQuery();
                Console.WriteLine("Writing : {0} {1}", cnt, cmdText);
                cnt++;
            }
        }
    }
}
