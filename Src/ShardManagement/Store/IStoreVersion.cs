using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Storage representation of shard map manager version
    /// </summary>
    interface IStoreVersion
    {
        /// <summary>
        /// Store version information.
        /// </summary>
        Version Version
        {
            get;
        }
    }
}
