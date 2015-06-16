using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Storage representation of a single location.
    /// </summary>
    internal interface IStoreLocation
    {
        /// <summary>
        /// Data source location.
        /// </summary>
        ShardLocation Location
        {
            get;
        }
    }
}
