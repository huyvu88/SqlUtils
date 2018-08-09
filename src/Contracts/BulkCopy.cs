using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Reflection;
namespace SqlUtils.Contracts
{
    public class BulkCopy : IBulkCopy
    {
        public DataRow GetDataRow(DataRow row)
        {
            throw new NotImplementedException();
        }

        public DataTable TableStructure()
        {
            throw new NotImplementedException();
        }
    }
}
