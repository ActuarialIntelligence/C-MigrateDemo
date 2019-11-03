using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AI.Calculators.Interfaces;
using AI.Domain;
using AI.Infrastructure.Readers.Interfaces;

namespace AI.Calculators
{
    public class CalculateInsertStatements : ICalculate<IList<string>>
    {
        private readonly IGeneralDataReader<IList<string>> reader;
        private readonly IDataReader<IList<string>> fileReader;
        private string readTableName;
        private string writeTableName;
        private int noOfColumns;
        private string connectionString;
        private string numericInsertFieldIndices;
        private bool hasIndex,isFile;
        public CalculateInsertStatements(IGeneralDataReader<IList<string>> reader, 
            string readTableName,string writeTableName, int noOfColumns, string connectionString, string numericInsertFieldIndices,
            bool hasIndex)
        {
            this.reader = reader;
            this.readTableName = readTableName;
            this.noOfColumns = noOfColumns;
            this.connectionString = connectionString;
            this.numericInsertFieldIndices = numericInsertFieldIndices;
            this.hasIndex = hasIndex;
            this.writeTableName = writeTableName;
        }

        public CalculateInsertStatements(IDataReader<IList<string>> reader,bool isFile,
            string readTableName, string writeTableName, int noOfColumns, string connectionString, string numericInsertFieldIndices,
            bool hasIndex)
        {
            this.isFile = isFile;
            this.fileReader = reader;
            this.readTableName = readTableName;
            this.noOfColumns = noOfColumns;
            this.connectionString = connectionString;
            this.numericInsertFieldIndices = numericInsertFieldIndices;
            this.hasIndex = hasIndex;
            this.writeTableName = writeTableName;
        }

        public IList<string> Calculate()
        {
            var sw = new StreamWriter(@"C:\Users\jyp1510\Documents\TestData\InsertStatements.txt",append:true);
            var insertStrings = new List<string>();
            if (!isFile)
            {
                var readerResults = reader.GetData(readTableName, noOfColumns, connectionString);
                var insertObject = new InsertObject(readerResults, numericInsertFieldIndices);

                var strings = insertObject.GetInsertStrings(hasIndex);
                var cnt = 0;
                foreach (var str in strings)
                {
                    var insert = "INSERT INTO " + writeTableName + " VALUES (" + str + ");";
                    Console.WriteLine("Calculating : {0} {1}", insert, cnt);
                    sw.WriteLine(insert);
                    insertStrings.Add(insert);
                    cnt++;
                }
                sw.Close();
            }
            else
            {
                var readerResults = fileReader.GetData();
                var insertObject = new InsertObject(readerResults, numericInsertFieldIndices);

                var strings = insertObject.GetInsertStrings(hasIndex);
                var cnt = 0;
                foreach (var str in strings)
                {
                    var insert = "INSERT INTO " + writeTableName + " VALUES (" + str + ");";
                    Console.WriteLine("Calculating : {0} {1}", insert, cnt);
                    sw.WriteLine(insert);
                    insertStrings.Add(insert);
                    cnt++;
                }
                sw.Close();
            }

            return insertStrings;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="noOfRowsYouIntendToRetrieveForInsert"></param>
        /// <param name="startIndex">>=</param>
        /// <returns></returns>
        public IList<string> Calculate(int noOfRowsYouIntendToRetrieveForInsert,int startIndex)
        {
            var sw = new StreamWriter(@"C:\Users\jyp1510\Documents\TestData\InsertStatements.txt", append: true);
            var readerResults = reader.GetData(readTableName, noOfColumns, noOfRowsYouIntendToRetrieveForInsert, startIndex, connectionString);
            var insertObject = new InsertObject(readerResults, numericInsertFieldIndices);
            var insertStrings = new List<string>();
            var strings = insertObject.GetInsertStrings(hasIndex);
            var cnt = 0;
            foreach (var str in strings)
            {
                var insert = "INSERT INTO " + writeTableName + " VALUES (" + str + ");";
                Console.WriteLine("Calculating : {0} {1}", insert, cnt);
                sw.WriteLine(insert);
                insertStrings.Add(insert);
                cnt++;
            }
            sw.Close();
            return insertStrings;
        }
    }
}
