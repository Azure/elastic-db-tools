// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    internal class CacheStoreDecorator : ICacheStore
    {
        protected readonly ICacheStore inner;

        internal CacheStoreDecorator(ICacheStore inner)
        {
            this.inner = inner;
        }

        public virtual void AddOrUpdateShardMap(IStoreShardMap shardMap)
        {
            this.inner.AddOrUpdateShardMap(shardMap);
        }

        public virtual void DeleteShardMap(IStoreShardMap shardMap)
        {
            this.inner.DeleteShardMap(shardMap);
        }

        public virtual IStoreShardMap LookupShardMapByName(string shardMapName)
        {
            return this.inner.LookupShardMapByName(shardMapName);
        }

        public virtual void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
        {
            this.inner.AddOrUpdateMapping(mapping, policy);
        }

        public virtual void DeleteMapping(IStoreMapping mapping)
        {
            this.inner.DeleteMapping(mapping);
        }

        public virtual ICacheStoreMapping LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
        {
            return this.inner.LookupMappingByKey(shardMap, key);
        }

        public virtual void IncrementPerformanceCounter(IStoreShardMap shardMap, PerformanceCounterName name)
        {
            this.inner.IncrementPerformanceCounter(shardMap, name);
        }

        public virtual void Clear()
        {
            this.inner.Clear();
        }

        public virtual void Dispose()
        {
            this.inner.Dispose();
        }
    }
}
