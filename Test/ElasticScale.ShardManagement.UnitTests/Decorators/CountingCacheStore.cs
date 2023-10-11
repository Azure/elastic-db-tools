// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Decorators;

/// <summary>
/// Cache that keeps track of lookups, hits and misses.
/// </summary>
internal class CountingCacheStore : CacheStoreDecorator
{
    internal CountingCacheStore(ICacheStore inner)
        : base(inner) => ResetCounters();

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
        AddShardMapCount = 0;
        DeleteShardMapCount = 0;
        LookupShardMapCount = 0;
        LookupShardMapHitCount = 0;
        LookupShardMapMissCount = 0;

        AddMappingCount = 0;
        DeleteMappingCount = 0;
        LookupMappingCount = 0;
        LookupMappingHitCount = 0;
        LookupMappingMissCount = 0;
    }


    public override void AddOrUpdateShardMap(IStoreShardMap shardMap)
    {
        AddShardMapCount++;
        base.AddOrUpdateShardMap(shardMap);
    }

    public override void DeleteShardMap(IStoreShardMap shardMap)
    {
        DeleteShardMapCount++;
        base.DeleteShardMap(shardMap);
    }

    public override IStoreShardMap LookupShardMapByName(string shardMapName)
    {
        LookupShardMapCount++;

        var result = base.LookupShardMapByName(shardMapName);
        if (result == null)
            LookupShardMapMissCount++;
        else
            LookupShardMapHitCount++;

        return result;
    }

    public override void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
    {
        AddMappingCount++;
        base.AddOrUpdateMapping(mapping, policy);
    }

    public override void DeleteMapping(IStoreMapping mapping)
    {
        DeleteMappingCount++;
        base.DeleteMapping(mapping);
    }

    public override ICacheStoreMapping LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
    {
        LookupMappingCount++;

        var result = base.LookupMappingByKey(shardMap, key);
        if (result == null)
            LookupMappingMissCount++;
        else
            LookupMappingHitCount++;

        return result;
    }
}
