using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Reflection;
using SqlUtils.Attributes;

namespace SqlUtils.Contracts
{
    public interface IOriginalBulkCopy<T> : IBulkCopy, IOriginal<T> where T: class
    {
    }

    public class OriginalBulkCopy : IOriginalBulkCopy<object>
    {
        public object OriginalID { get; set; }

        public DataRow GetDataRow(DataRow row)
        {
            foreach (var propInfo in this.GetType().GetRuntimeProperties())
            {
                var currentValue = propInfo.GetValue(this);
                if(currentValue == null && Nullable.GetUnderlyingType(propInfo.PropertyType) != null)
                    row[propInfo.Name] = (object)DBNull.Value;
                else
                    row[propInfo.Name] = currentValue;
            }
            return row;
        }

        public DataTable TableStructure()
        {
            Type type = this.GetType();
            PropertyInfo idProp = null;
            foreach (var t in type.GetRuntimeProperties())
            {
                if (t.GetCustomAttribute<PKAttribute>() != null)
                {
                    idProp = t;
                    break;
                }
            };
            if(idProp == null)
            {
                throw new Exception($"Missing [PK] Property for {type.Name}");
            }
            var DataTable = new DataTable();
            DataTable.TableName = $"dbo.{this.GetType().Name}";


            foreach (var propInfo in this.GetType().GetRuntimeProperties())
            {
                if(propInfo.Name == "OriginalID")
                    DataTable.Columns.Add(propInfo.Name, HandleNullableType(idProp.PropertyType));
                else
                    DataTable.Columns.Add(propInfo.Name, HandleNullableType(propInfo.PropertyType));
            }

            //DataTable.Columns.Add("CustomerID", typeof(int));
            //DataTable.Columns.Add("CustomerType", typeof(string));
            //DataTable.Columns.Add("CustomerCode", typeof(string));
            //DataTable.Columns.Add("City", typeof(string));
            //DataTable.Columns.Add("Country", typeof(string));
            //DataTable.Columns.Add("CompanyName", typeof(string));
            //DataTable.Columns.Add("CompanyAddress1", typeof(string));
            //DataTable.Columns.Add("CompanyAddress2", typeof(string));
            //DataTable.Columns.Add("CompanyAddress3", typeof(string));
            //DataTable.Columns.Add("CompanyAddress4", typeof(string));
            //DataTable.Columns.Add("CompanyAddress5", typeof(string));
            //DataTable.Columns.Add("CompanyAddress6", typeof(string));
            //DataTable.Columns.Add("ModifyBy", typeof(string));
            //DataTable.Columns.Add("ModifyDT", typeof(DateTime));
            //DataTable.Columns.Add("Action", typeof(string));
            //DataTable.Columns.Add("DistributorCustomers", typeof(string));
            //DataTable.Columns.Add("DistributorRatio", typeof(decimal));
            //DataTable.Columns.Add("OldDevID", typeof(int));
            return DataTable;
        }

        private Type HandleNullableType(Type type)
        {
            if (Nullable.GetUnderlyingType(type) != null)
            {
                // It's nullable
                return Nullable.GetUnderlyingType(type);
            }
            else
            {
                return type;
            }
        }
    }
}
