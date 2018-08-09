using System;
using System.Collections.Generic;
using System.Text;

namespace SqlUtils.Attributes
{
    public class PKAttribute : Attribute
    {
        public string IDSqlType { get; set; }

        public PKAttribute(string iDSqlType = "")
        {
            IDSqlType = iDSqlType;
        }
    }
}
