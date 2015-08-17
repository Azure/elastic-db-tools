// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Cache that keeps track of lookups, hits and misses.
    /// </summary>
    internal class CountingCacheStore : CacheStoreDecorator
    {
        internal CountingCacheStore(ICacheStore inner)
            : base(inner)
        {
            this.ResetCounters();
        }

        internal int AddShardMapCount
        {
            get;
            private set;
        }

        internal int DeleteShardMapCount
        {
            get;
            private set;
        }

        internal int LookupShardMapCount
        {
            get;
            private set;
        }

        internal int LookupShardMapHitCount
        {
            get;
            private set;
        }

        internal int LookupShardMapMissCount
        {
            get;
            private set;
        }

        internal int AddMappingCount
        {
            get;
            private set;
        }

        internal int DeleteMappingCount
        {
            get;
            private set;
        }

        internal int LookupMappingCount
        {
            get;
            private set;
        }

        internal int LookupMappingHitCount
        {
            get;
            private set;
        }

        internal int LookupMappingMissCount
        {
            get;
            private set;
        }

        internal void ResetCounters()
        {
            this.AddShardMapCount = 0;
            this.DeleteShardMapCount = 0;
            this.LookupShardMapCount = 0;
            this.LookupShardMapHitCount = 0;
            this.LookupShardMapMissCount = 0;

            this.AddMappingCount = 0;
            this.DeleteMappingCount = 0;
            this.LookupMappingCount = 0;
            this.LookupMappingHitCount = 0;
            this.LookupMappingMissCount = 0;
        }


        public override void AddOrUpdateShardMap(IStoreShardMap shardMap)
        {
            this.AddShardMapCount++;
            base.AddOrUpdateShardMap(shardMap);
        }

        public override void DeleteShardMap(IStoreShardMap shardMap)
        {
            this.DeleteShardMapCount++;
            base.DeleteShardMap(shardMap);
        }

        public override IStoreShardMap LookupShardMapByName(string shardMapName)
        {
            this.LookupShardMapCount++;

            IStoreShardMap result = base.LookupShardMapByName(shardMapName);
            if (result == null)
            {
                this.LookupShardMapMissCount++;
            }
            else
            {
                this.LookupShardMapHitCount++;
            }

            return result;
        }

        public override void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
        {
            this.AddMappingCount++;
            base.AddOrUpdateMapping(mapping, policy);
        }

        public override void DeleteMapping(IStoreMapping mapping)
        {
            this.DeleteMappingCount++;
            base.DeleteMapping(mapping);
        }

        public override ICacheStoreMapping LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
        {
            this.LookupMappingCount++;

            ICacheStoreMapping result = base.LookupMappingByKey(shardMap, key);
            if (result == null)
            {
                this.LookupMappingMissCount++;
            }
            else
            {
                this.LookupMappingHitCount++;
            }

            return result;
        }
    }
}
