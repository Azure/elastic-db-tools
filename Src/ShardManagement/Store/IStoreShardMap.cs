using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Store representation of a shard map.
    /// </summary>
    internal interface IStoreShardMap
    {
        /// <summary>
        /// Shard map's identity.
        /// </summary>
        Guid Id
        {
            get;
        }

        /// <summary>
        /// Shard map name.
        /// </summary>
        string Name
        {
            get;
        }

        /// <summary>
        /// Type of shard map.
        /// </summary>
        ShardMapType MapType
        {
            get;
        }
        
        /// <summary>
        /// Key type.
        /// </summary>
        ShardKeyType KeyType
        {
            get;
        }
    }
}
