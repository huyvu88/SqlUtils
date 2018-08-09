using System;
using System.Collections.Generic;
using System.Text;

namespace SqlUtils.Contracts
{
    public interface IOriginal<T> where T : class
    {
        T OriginalID { get; set; }
    }
}
