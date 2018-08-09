using SqlUtils.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlUtils.Extensions
{
    public static class SqlExtensions
    {
        public static List<OriginalBulkCopy> ToOriginalBulkCopyList(this IList<object> list)
        {
            var result = new List<OriginalBulkCopy>();
            if (!list.Any()) return result;
            var first = list.First();
            if(first is OriginalBulkCopy)
            {
                foreach (var item in list)
                {
                    var newItem = item as OriginalBulkCopy;
                    result.Add(newItem);
                }
            }
            else
            {
                throw new Exception("ToIOriginalBulkCopyList error. Ensure class inherited from IOriginalBulkCopy");
            }
            return result;
        }
    }
}
