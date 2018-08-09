using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using SqlUtils.Contracts;
using Dapper;
using SqlUtils.Dto;
using System.Reflection;
using SqlUtils.Attributes;

namespace SqlUtils
{
    internal class SqlHelper
    {
        private static string _tempTable = "TMP";
        private static string _tempIdTable = "TMP_ID";
        private static DataTable _dataTable;
        private static string _tableName = "";
        private static string _connStr = "";
        private static string _idProperty = "";
        private static Type _itemType;
        private static PropertyInfo _idProp;
        private static PKAttribute _tableIDAttribute;
        private static bool _isIdIdentityColumn
        {
            get
            {
                return _idProp != null && (_idProp.PropertyType == typeof(Int16) || _idProp.PropertyType == typeof(Int32)
                    || _idProp.PropertyType == typeof(Int64));
            }
        }
        internal static void Init(IEnumerable<OriginalBulkCopy> list, string connStr)
        {
             _dataTable = list.FirstOrDefault().TableStructure();
             _tableName = list.FirstOrDefault().TableStructure().TableName;
            _connStr = connStr;

            // Get item Type
            Type type = list.FirstOrDefault().GetType();
            _itemType = type;
            foreach (var t in type.GetRuntimeProperties())
            {
                _tableIDAttribute = t.GetCustomAttribute<PKAttribute>();
                if (_tableIDAttribute != null)
                {
                    _idProp = t;
                    break;
                }
            };
        }
        internal static void Prepare()
        {
            try
            {
                var idType = _idProp.PropertyType;
                var idSqlType = !string.IsNullOrEmpty(_tableIDAttribute.IDSqlType) ?
                    _tableIDAttribute.IDSqlType : GetSQLDbType(idType);

                var createOriginalIDquery = $"ALTER TABLE {_tableName} ADD OriginalID {idSqlType} null";
                var tempCreateTableQuery = SqlTableCreator.GetCreateFromDataTableSQL(_tempTable, _dataTable);
                var tempCreateOriginalIDquery = $"ALTER TABLE {_tempTable} ADD OriginalID {idSqlType} null";
                var tempIdTableQuery = $"CREATE TABLE [{_tempIdTable}] (id int not null identity(1,1), NewID {idSqlType} not null, OriginalID {idSqlType} not null)";
                using (IDbConnection db = new SqlConnection(_connStr))
                {
                    db.Execute(createOriginalIDquery);
                    db.Execute(tempCreateTableQuery);
                    //db.Execute(tempCreateOriginalIDquery);
                    db.Execute(tempIdTableQuery);
                }
            }
            catch (Exception ex)
            {
                RestoreOriginalTable();
                Console.WriteLine(ex.Message);
                throw;
            }
            
        }

        internal static void DoBulkCopy(IEnumerable<OriginalBulkCopy> list, IDictionary<string, List<IdPair>> dicIdPairs)
        {
            try
            {
                // 1. Resolve FK Properties
                var records = list;
                ResolveFKProps(records, dicIdPairs);

                // 2. BulkCopy
                BulkCopyHelper.FlushAsync(records, _connStr, tableName: _tempTable).Wait();
            }
            catch (Exception ex)
            {
                RestoreOriginalTable();
                throw;
            }

        }

        private static void ResolveFKProps(IEnumerable<OriginalBulkCopy> list, IDictionary<string, List<IdPair>> dicIdPairs)
        {
            
            if (_idProp == null) throw new Exception($"There is no property decorated by TableID of type {_tableName}");
            _idProperty = _idProp.Name;
            
            foreach (var item in list)
            {
                foreach (var prop in _itemType.GetRuntimeProperties())
                {
                    // 1. If prop is OriginalID -> find prop with TableID Attribute -> set OriginalID = TableID property 
                    if(prop.Name == "OriginalID")
                    {
                        prop.SetValue(item, _idProp.GetValue(item));
                    }
                    // 2. Check if has decorated by FK Attribute. If Yes set the value of prop to be the new ID in dicIdPairs
                    // If cannot found newID -> throw error
                    var fkAttr = prop.GetCustomAttribute<FKAttribute>();
                    var fkListAttr = prop.GetCustomAttribute<FKListAttribute>();
                    if (fkAttr != null)
                    {
                        if(dicIdPairs.ContainsKey(fkAttr.ToTable))
                        {
                            var currentValue = prop.GetValue(item);
                            if (currentValue != null)
                            {
                                // set prop value to new Id
                                var newValue = dicIdPairs[fkAttr.ToTable].FirstOrDefault(t => t.OriginalID.Equals(currentValue));
                                if(newValue == null)
                                {
                                    throw new Exception($"Not found NewID for OriginalID: {currentValue} for {prop.Name} column of table {_itemType.Name}. Skipping table {_itemType.Name}");
                                }
                                prop.SetValue(item, newValue.NewID);
                            }
                        }
                        else
                        {
                            throw new Exception($"FKAttribute. Cannot found ID pairs for {prop.Name} of type {_itemType.Name}. Ensure data source was ordered in right order of dependencies. Skipping {_itemType.Name}");
                        }
                        
                    }
                    // 3. Check if has decorated by FKList Attribute. If Yes set the values of prop to be the new IDs in dicIdPairs
                    if (fkListAttr != null)
                    {
                        if (dicIdPairs.ContainsKey(fkListAttr.ToTable))
                        {
                            // Foreign Key List can only be string, eg: id1, id2, id3
                            var currentValue = prop.GetValue(item);

                            if (currentValue != null)
                            {
                                string curValue = currentValue.ToString();
                                var values = curValue.Split(fkListAttr.Separator);
                                var newValues = new List<string>();
                                var missingValues = "";
                                foreach (var val in values)
                                {
                                    if (!fkListAttr.IgnoreValues.Contains(val))
                                    {
                                        var newValue = dicIdPairs[fkListAttr.ToTable].FirstOrDefault(t => t.OriginalID.Equals(val));
                                        if (newValue == null)
                                        {
                                            missingValues += ($"Not found new ID for OriginalID: {val} in {prop.Name} column of table {_itemType.Name}.\n");
                                            continue;
                                        }
                                        newValues.Add(newValue.NewID.ToString());
                                    }
                                    
                                }
                                if (!string.IsNullOrWhiteSpace(missingValues))
                                {
                                    throw new Exception($"FKListAttribute. Cannot resolved ForeignKey because of missing values:\n{missingValues}\n Skipped {_itemType.Name} table.");
                                }
                                prop.SetValue(item, string.Join(fkListAttr.Separator.ToString(), newValues));
                            }
                        }
                        else
                        {
                            throw new Exception($"FKListAttribute. Cannot found ID pairs for {prop.Name} of type {_itemType.Name}. Ensure data source was ordered in right order of dependencies. Skipping {_itemType.Name}");
                        }
                    }
                }

            }
        }

        internal static void InsertToRealTable()
        {
            try
            {
                var insertToRealTableSql = $"INSERT {_tableName} "
                              + $"OUTPUT INSERTED.{_idProperty}, INSERTED.OriginalID INTO [{_tempIdTable}] (NewID, OriginalID) "
                              + $"SELECT {GetSqlPropertiesList(_itemType, _isIdIdentityColumn)} FROM {_tempTable}";
                using (IDbConnection db = new SqlConnection(_connStr))
                {
                    db.Execute(insertToRealTableSql);
                }
            }
            catch (Exception ex)
            {
                RestoreOriginalTable();
                throw;
            }
        }


        internal static (string TableName, List<IdPair> IdPairs) GetReturnedIDs()
        {
            try
            {
                var selectNewIDSql = $"SELECT * FROM {_tempIdTable}";
                using (IDbConnection db = new SqlConnection(_connStr))
                {
                    var result = db.Query<IdPair>(selectNewIDSql).ToList();
                    return (_itemType.Name, result );
                }
            }
            catch (Exception ex)
            {
                return (null, null);
            }
            finally
            {
                RestoreOriginalTable();
                CleanUp();
            }
        }

        internal static void RestoreOriginalTable()
        {
            try
            {
                var selectNewIDSql = $"ALTER TABLE {_tableName} DROP column OriginalID";
                using (IDbConnection db = new SqlConnection(_connStr))
                {
                    var result = db.Execute(selectNewIDSql);
                }
                CleanUp();
            }
            catch (Exception ex)
            {
            }
        }

        internal static void CleanUp()
        {
            try
            {
                var dropTempTableSql = $"DROP TABLE {_tempTable}";
                var dropTempIdTableSql = $"DROP TABLE {_tempIdTable}";
                using (IDbConnection db = new SqlConnection(_connStr))
                {
                    db.Execute(dropTempTableSql);
                    db.Execute(dropTempIdTableSql);
                }
            }
            catch (Exception ex)
            {
            }
        }

        private static string GetSqlPropertiesList(Type itemType, bool idIsIdentity = true)
        {
            var properties = itemType.GetRuntimeProperties();
            if(idIsIdentity)
                properties = properties.Where(t => t.Name != _idProperty);
            var list = properties.Select(t => t.Name).ToList();
            return string.Join(",", list);
        }

        private static string GetSQLDbType(Type type)
        {
            switch (type.Name.ToLower())
            {
                case "int16": case "int32":
                    return "int";
                case "int64":
                    return "bigint";
                case "guid":
                    return "uniqueidentifier";
                case "string":
                    return "nvarchar(max)";
                default:
                    return "";
            }
        }
    }
}
