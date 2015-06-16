﻿
namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>Type of shard map.</summary>
    public enum ShardMapType : int
    {
        /// <summary>
        /// Invalid kind of shard map. Only used for serialization/deserialization.
        /// </summary>
        None = 0,

        /// <summary>
        /// Shard map with list based mappings.
        /// </summary>
        List = 1,

        /// <summary>
        /// Shard map with range based mappings.
        /// </summary>
        Range = 2,
    }
}
