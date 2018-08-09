using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using SqlUtils.Contracts;

namespace SqlUtils
{
    
    public class BulkCopyHelper
    {
        private static string _connStr = "";
        private static DataTable dataTable = null;
        private static long recordCount = 0;

        public static async Task FlushAsync<T>(IEnumerable<T> list, string connStr, int batchSize = 500, string tableName = "", bool keepIdentity = false) where T : IBulkCopy
        {
            if (!list.Any()) return;

            IBulkCopy baseEntity = list.First();

            dataTable = baseEntity.TableStructure();

            Console.WriteLine($"BatchSize: {batchSize}");
            _connStr = connStr;

            var countBatch = 1;
            foreach (IBulkCopy item in list)
            {
                DataRow row = dataTable.NewRow();
                dataTable.Rows.Add(item.GetDataRow(row));
                recordCount++;

                if (recordCount >= batchSize)
                {
                    await WriteToDatabase(tableName, keepIdentity);
                    Console.WriteLine($"Processed batch {countBatch}");
                }
                countBatch++;
            }

            // write remaining records to the DB
            if (recordCount > 0)
                await WriteToDatabase(tableName, keepIdentity);

        }

        private static async Task WriteToDatabase(string tableName = "", bool keepIdentity = false)
        {
            // connect to SQL
            using (SqlConnection connection = new SqlConnection(_connStr))
            {
                try
                {
                    var options =

                    SqlBulkCopyOptions.TableLock |

                    SqlBulkCopyOptions.FireTriggers |

                    SqlBulkCopyOptions.UseInternalTransaction;

                    if (keepIdentity)
                        options = options | SqlBulkCopyOptions.KeepIdentity;
                    SqlBulkCopy bulkCopy = new SqlBulkCopy
                    (
                        connection,
                        options,
                        null
                    );

                    // set the destination table name
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.DestinationTableName = dataTable.TableName;

                    if (!string.IsNullOrEmpty(tableName))
                    {
                        bulkCopy.DestinationTableName = tableName;
                    }

                    connection.Open();

                    // write the data in the "dataTable"
                    await bulkCopy.WriteToServerAsync(dataTable);

                    connection.Close();
                }
                catch (Exception ex)
                {

                    throw;
                }


            }

            // reset
            Console.WriteLine("WriteCompleted");
            dataTable.Clear();

            recordCount = 0;

        }
    }
}