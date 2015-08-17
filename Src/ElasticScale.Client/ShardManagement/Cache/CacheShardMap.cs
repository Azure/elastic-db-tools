// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    ///  Cached representation of shard map.
    /// </summary>
    internal class CacheShardMap : CacheObject
    {
        /// <summary>
        /// Constructs the cached shard map.
        /// </summary>
        /// <param name="ssm">Storage representation of shard map.</param>
        internal CacheShardMap(IStoreShardMap ssm)
            : base()
        {
            this.StoreShardMap = ssm;

            switch (ssm.MapType)
            {
                case ShardMapType.List:
                    this.Mapper = new CacheListMapper(ssm.KeyType);
                    break;
                case ShardMapType.Range:
                    this.Mapper = new CacheRangeMapper(ssm.KeyType);
                    break;
            }
        }

        /// <summary>
        /// Storage representation of shard map.
        /// </summary>
        internal IStoreShardMap StoreShardMap
        {
            get;
            private set;
        }

        /// <summary>
        /// Mapper object. Exists only for List/Range/Hash shard maps.
        /// </summary>
        internal CacheMapper Mapper
        {
            get;
            private set;
        }

        /// <summary>
        /// Transfers the child cache objects to current instance from the source instance.
        /// Useful for mantaining the cache even in case of refreshes to shard map objects.
        /// </summary>
        /// <param name="source">Source cached shard map to copy child objects from.</param>
        internal void TransferStateFrom(CacheShardMap source)
        {
            this.Mapper = source.Mapper;
        }
    }
}
