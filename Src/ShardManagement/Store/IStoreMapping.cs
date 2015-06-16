using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Storage representation of a mapping b/w key ranges and shards.
    /// </summary>
    internal interface IStoreMapping
    {
        /// <summary>
        /// Mapping Id.
        /// </summary>
        Guid Id
        {
            get;
        }

        /// <summary>
        /// Shard map Id.
        /// </summary>
        Guid ShardMapId
        {
            get;
        }

        /// <summary>
        /// Min value.
        /// </summary>
        byte[] MinValue
        {
            get;
        }

        /// <summary>
        /// Max value.
        /// </summary>
        byte[] MaxValue
        {
            get;
        }

        /// <summary>
        /// Mapping status.
        /// </summary>
        int Status
        {
            get;
        }

        /// <summary>
        /// Lock owner id of this mapping
        /// </summary>
        Guid LockOwnerId
        {
            get;
        }

        /// <summary>
        /// Shard referenced by mapping.
        /// </summary>
        IStoreShard StoreShard
        {
            get;
        }
    }
}
