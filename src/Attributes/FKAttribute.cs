using System;
using System.Collections.Generic;
using System.Text;

namespace SqlUtils.Attributes
{
    public class FKAttribute : Attribute
    {
        public string ToTable { get; set; }

        public FKAttribute(string tableName)
        {
            ToTable = tableName;
        }
    }
}
