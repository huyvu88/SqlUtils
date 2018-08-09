using SqlUtils.Contracts;
using SqlUtils.Dto;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlUtils
{
    public class BulkCopyProcessor
    {
        public string ConnectionString { get; set; }

        #region ctors
        public BulkCopyProcessor()
        {

        }

        public BulkCopyProcessor(string connStr)
        {
            ConnectionString = connStr;
        }

        #endregion
        public void Process(IEnumerable<IEnumerable<IBulkCopy>> sources)
        {
            foreach (var list in sources)
            {
                
            }
        }

        public string Process(IEnumerable<IEnumerable<OriginalBulkCopy>> OriginalSources)
        {
            string error = "";
            if (string.IsNullOrEmpty(ConnectionString)) return "Missing ConnectionString";
            Dictionary<string, List<IdPair>> dicIdPairs = new Dictionary<string, List<IdPair>>();

            try
            {
                foreach (var list in OriginalSources)
                {
                    try
                    {
                        if (list.Any())
                        {
                            var idPairs = new List<IdPair>();
                            SqlHelper.Init(list, ConnectionString);

                            // Step1. Alter table add OriginalId column, create temporary tables for bulk inserting
                            SqlHelper.Prepare();

                            // Step2. Bulk Insert data to temp table
                            SqlHelper.DoBulkCopy(list, dicIdPairs);

                            // Step3. Insert to real table
                            SqlHelper.InsertToRealTable();

                            // Step4. Return {NewID, OriginalID} use for referencing
                            var tupleRs = SqlHelper.GetReturnedIDs();
                            if (tupleRs.IdPairs == null)
                            {
                                throw new Exception($"Could not get NewID. ItemCount: {list.Count()}");
                            }
                            idPairs.AddRange(tupleRs.IdPairs);
                            dicIdPairs.Add(tupleRs.TableName, idPairs);
                        }
                    }
                    catch (Exception iex)
                    {
                        // log list of error
                        error += iex.Message + "StackTrace: " + iex.StackTrace;
                        continue;
                    }
                    
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                // Step5. Clean up
                SqlHelper.CleanUp();
            }
            return error;
        }
    }
}
