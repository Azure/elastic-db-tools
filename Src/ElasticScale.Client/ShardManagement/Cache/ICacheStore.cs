// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Policy for AddOrUpdateMapping operation.
    /// </summary>
    internal enum CacheStoreMappingUpdatePolicy
    {
        /// <summary>
        /// Overwrite the mapping blindly.
        /// </summary>
        OverwriteExisting,

        /// <summary>
        /// Keep the original mapping but change TTL.
        /// </summary>
        UpdateTimeToLive
    }


    /// <summary>
    /// Representation of client side cache.
    /// </summary>
    internal interface ICacheStore : IDisposable
    {
        /// <summary>
        /// Invoked for refreshing shard map in cache from store.
        /// </summary>
        /// <param name="shardMap">Storage representation of shard map.</param>
        void AddOrUpdateShardMap(IStoreShardMap shardMap);

        /// <summary>
        /// Invoked for deleting shard map in cache becase it no longer exists in store.
        /// </summary>
        /// <param name="shardMap">Storage representation of shard map.</param>
        void DeleteShardMap(IStoreShardMap shardMap);

        /// <summary>
        /// Looks up a given shard map in cache based on it's name.
        /// </summary>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <returns>The shard being searched.</returns>
        IStoreShardMap LookupShardMapByName(string shardMapName);

        /// <summary>
        /// Invoked for refreshing mapping in cache from store.
        /// </summary>
        /// <param name="mapping">Storage representation of mapping.</param>
        /// <param name="policy">Policy to use for preexisting cache entries during update.</param>
        void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy);

        /// <summary>
        /// Invoked for deleting mapping in cache becase it no longer exists in store.
        /// </summary>
        /// <param name="mapping">Storage representation of mapping.</param>
        void DeleteMapping(IStoreMapping mapping);

        /// <summary>
        /// Looks up a given key in given shard map.
        /// </summary>
        /// <param name="shardMap">Storage representation of shard map.</param>
        /// <param name="key">Key value.</param>
        /// <returns>Mapping corresponding to <paramref name="key"/> or null.</returns>
        ICacheStoreMapping LookupMappingByKey(IStoreShardMap shardMap, ShardKey key);

        /// <summary>
        /// Increment specified perf counter.
        /// </summary>
        /// <param name="shardMap">Storage representation of shard map.</param>
        /// <param name="name">Performance counter to increment.s</param>
        void IncrementPerformanceCounter(IStoreShardMap shardMap, PerformanceCounterName name);

        /// <summary>
        /// Clears the cache.
        /// </summary>
        void Clear();
    }
}
