using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AI.Infrastructure.Readers.Interfaces;

namespace AI.Infrastructure.Readers
{
    /// <summary>
    /// Use a numeric fields code to identify 
    /// </summary>
    public class GeneralTableReader : IGeneralDataReader<IList<string>>
    {

        public GeneralTableReader()
        {

        }
            /// <summary>
            /// Warning!! If 
            /// </summary>
            /// <param name="tableName"></param>
            /// <param name="noOfColumns"></param>
            /// <param name="connectionString"></param>
            /// <returns></returns>
        public  IList<string> GetData(string tableName,int noOfColumns,string connectionString)
        {
            var retList = new List<string>();
            var con = new SqlConnection(connectionString);
            con.Open();
            var cmd = new SqlCommand("SELECT * FROM "+tableName+";",con);
            var reader = cmd.ExecuteReader();
            while(reader.Read())
            {
                
                var strTemp = "";
                for(int i=0;i<noOfColumns;i++)
                {
                    if (i == noOfColumns - 1)
                    {
                        strTemp += reader[i];
                    }
                    else
                    {
                        strTemp += reader[i] + "|";
                    }
                    
                }
                Console.WriteLine("Formatting : {0}", strTemp);
                retList.Add(strTemp);
            }
            return retList;
        }

        public IList<string> GetData(string tableName, int noOfColumns, int noOfRowsYouIntendToRetrieveForInsert, int rowStartIndex, string connectionString)
        {
            throw new NotImplementedException();
        }
    }

    public class ORACLETableReader : IGeneralDataReader<IList<string>>
    {

        public ORACLETableReader()
        {

        }
        /// <summary>
        /// Warning!! If 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="noOfColumns"></param>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        /// "Data Source=(DESCRIPTION =(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST = "+host+ ")(PORT = " + port + ")))(CONNECT_DATA =(SERVICE_NAME = " + service + ")));User ID=" + id + ";Password=" + pass + ";";
        public IList<string> GetData(string tableName, int noOfColumns, string connectionString)
        {
            var retList = new List<string>();
            var con = new OracleConnection(connectionString);
            con.Open();
            var cmd = new OracleCommand("select c.CASE_KEY, c.CMNC_DT, m.EFF_DT,c.PROD_TYP_CD,0 from COMPASS.CASE_DATA c  JOIN COMPASS.WD_REQUEST m ON m.CASE_KEY = c.CASE_KEY  WHERE c.CMNC_DT LIKE '%JUN/14'  ORDER BY c.CASE_KEY DESC", con);
            var reader = cmd.ExecuteReader();
            while (reader.Read())
            {

                var strTemp = "";
                for (int i = 0; i < noOfColumns; i++)
                {
                    if (i == noOfColumns - 1)
                    {
                        strTemp += reader[i].ToString().Replace("\'",string.Empty).Replace("\"", string.Empty).Replace(",", ".");
                    }
                    else
                    {
                        strTemp += reader[i].ToString().Replace("\'", string.Empty).Replace("\"", string.Empty).Replace(",", ".") + "|";
                    }

                }
                Console.WriteLine("Formatting : {0}", strTemp);
                retList.Add(strTemp);
            }
            return retList;
        }

        public IList<string> GetData(string tableName, int noOfColumns,int noOfRowsYouIntendToRetrieveForInsert,int rowStartIndex, string connectionString)
        {
            var retList = new List<string>();
            var con = new OracleConnection(connectionString);
            con.Open();
            var endIndex = rowStartIndex + noOfRowsYouIntendToRetrieveForInsert;
            //var cmd = new OracleCommand(" SELECT * from (select m.*, rownum r from "+ tableName + " m WHERE m.BOOK_DT LIKE '%JAN/18' ORDER BY WDRQ_KEY DESC) where r >= " + rowStartIndex + " and r < "+ endIndex, con);
            var cmd = new OracleCommand(" SELECT * from (select m.*, rownum r from " + tableName + " m WHERE m.BOOK_DT LIKE '%JAN/18' ORDER BY WDRQ_KEY DESC) where r >= " + rowStartIndex + " and r < " + endIndex, con);
            var reader = cmd.ExecuteReader();
            while (reader.Read())
            {

                var strTemp = "";
                for (int i = 0; i < noOfColumns; i++)
                {
                    if (i == noOfColumns - 1)
                    {
                        strTemp += reader[i].ToString().Replace("\'", string.Empty).Replace("\"", string.Empty).Replace(",", ".");
                    }
                    else
                    {
                        strTemp += reader[i].ToString().Replace("\'", string.Empty).Replace("\"", string.Empty).Replace(",", ".") + "|";
                    }

                }
                Console.WriteLine("Formatting : {0}", strTemp);
                retList.Add(strTemp);
            }
            return retList;
        }

    }
}
