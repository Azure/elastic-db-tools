// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Storage representation of a shard map.
    /// </summary>
    internal sealed class DefaultStoreShardMap : IStoreShardMap
    {
        /// <summary>
        /// Constructs an instance of DefaultStoreShardMap used for creating new shard maps.
        /// </summary>
        /// <param name="id">Shard map Id.</param>
        /// <param name="name">Shard map name.</param>
        /// <param name="mapType">Shard map kind.</param>
        /// <param name="keyType">Shard map key type.</param>
        /// <param name="shardId">Optional argument for shardId if this instance is for a local shardmap.</param>
        internal DefaultStoreShardMap(
            Guid id,
            string name,
            ShardMapType mapType,
            ShardKeyType keyType,
            Guid? shardId = null)
        {
            this.Id = id;
            this.Name = name;
            this.MapType = mapType;
            this.KeyType = keyType;
            this.ShardId = shardId;
        }

        /// <summary>
        /// Shard map's identity.
        /// </summary>
        public Guid Id
        {
            get;
            private set;
        }

        /// <summary>
        /// Shard map name.
        /// </summary>
        public string Name
        {
            get;
            private set;
        }

        /// <summary>
        /// Type of shard map.
        /// </summary>
        public ShardMapType MapType
        {
            get;
            private set;
        }

        /// <summary>
        /// Key type.
        /// </summary>
        public ShardKeyType KeyType
        {
            get;
            private set;
        }

        /// <summary>
        /// The id of the local shardmap. Null if a global shardmap.
        /// </summary>
        public Guid? ShardId
        {
            get;
            private set;
        }
    }
}
