// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Decorators;

internal class CacheStoreDecorator : ICacheStore
{
    protected readonly ICacheStore inner;

    internal CacheStoreDecorator(ICacheStore inner) => this.inner = inner;

    public virtual void AddOrUpdateShardMap(IStoreShardMap shardMap) => inner.AddOrUpdateShardMap(shardMap);

    public virtual void DeleteShardMap(IStoreShardMap shardMap) => inner.DeleteShardMap(shardMap);

    public virtual IStoreShardMap LookupShardMapByName(string shardMapName) => inner.LookupShardMapByName(shardMapName);

    public virtual void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy) => inner.AddOrUpdateMapping(mapping, policy);

    public virtual void DeleteMapping(IStoreMapping mapping) => inner.DeleteMapping(mapping);

    public virtual ICacheStoreMapping LookupMappingByKey(IStoreShardMap shardMap, ShardKey key) => inner.LookupMappingByKey(shardMap, key);

    public virtual void IncrementPerformanceCounter(IStoreShardMap shardMap, PerformanceCounterName name) => inner.IncrementPerformanceCounter(shardMap, name);

    public virtual void Clear() => inner.Clear();

    public virtual void Dispose() => inner.Dispose();
}
