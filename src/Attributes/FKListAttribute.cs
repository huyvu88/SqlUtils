using System;
using System.Collections.Generic;
using System.Text;

namespace SqlUtils.Attributes
{
    public class FKListAttribute : Attribute
    {
        public string ToTable { get; private set; }
        public char Separator { get; private set; }

        public string[] IgnoreValues { get; private set; }

        public FKListAttribute(string toTable, char separator, params string [] ignoreValues)
        {
            ToTable = toTable;
            Separator = separator;
            IgnoreValues = ignoreValues;
        }
    }
}
