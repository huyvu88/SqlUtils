using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SqlUtils.Contracts
{
    public interface IBulkCopy
    {
        DataTable TableStructure();

        DataRow GetDataRow(DataRow row);
    }
}
