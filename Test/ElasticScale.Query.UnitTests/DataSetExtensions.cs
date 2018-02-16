using System.Data;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
#if !NET451
    static class DataSetExtensions
    {
        public static T Field<T>(this DataRow dataSet, int index)
        {
            return (T)dataSet[index];
        }
    }
#endif
}
