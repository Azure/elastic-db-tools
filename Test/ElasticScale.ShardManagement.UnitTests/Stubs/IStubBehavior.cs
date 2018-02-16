using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    class IStubBehavior
    {
        public TReturn Result<T, TReturn>(T obj, string name)
        {
            throw new NotImplementedException();
        }

        public void VoidResult<T>(T obj, string name)
        {
            throw new NotImplementedException();
        }
    }
}
