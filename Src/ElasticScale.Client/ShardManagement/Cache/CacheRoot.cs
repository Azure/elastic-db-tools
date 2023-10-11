// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Cache;

/// <summary>
///  Cached representation of shard map manager.
/// </summary>
internal class CacheRoot : CacheObject
{
    /// <summary>
    /// Contained shard maps. Look up to be done by name.
    /// </summary>
    private readonly SortedDictionary<string, CacheShardMap> _shardMapsByName;

    /// <summary>
    /// Contained shard maps. Lookup to be done by Id.
    /// </summary>
    private readonly SortedDictionary<Guid, CacheShardMap> _shardMapsById;

    /// <summary>
    /// Constructs the cached shard map manager.
    /// </summary>
    internal CacheRoot() : base()
    {
        _shardMapsByName = new SortedDictionary<string, CacheShardMap>(StringComparer.OrdinalIgnoreCase);
        _shardMapsById = new SortedDictionary<Guid, CacheShardMap>();
    }

    /// <summary>
    /// Adds a shard map to the cache given storage representation.
    /// </summary>
    /// <param name="ssm">Storage representation of shard map.</param>
    /// <returns>Cached shard map object.</returns>
    internal CacheShardMap AddOrUpdate(IStoreShardMap ssm)
    {
        var csm = new CacheShardMap(ssm);

        if (_shardMapsByName.TryGetValue(ssm.Name, out var csmOldByName))
            _ = _shardMapsByName.Remove(ssm.Name);

        if (_shardMapsById.TryGetValue(ssm.Id, out var csmOldById))
            _ = _shardMapsById.Remove(ssm.Id);

        // Both should be found or none should be found.
        Debug.Assert((csmOldByName == null && csmOldById == null) ||
                     (csmOldByName != null && csmOldById != null));

        // Both should point to same cached copy.
        Debug.Assert(ReferenceEquals(csmOldByName, csmOldById));

        if (csmOldByName != null)
        {
            csm.TransferStateFrom(csmOldByName);

            // Dispose off the old cached shard map
            csmOldByName.Dispose();
        }

        _shardMapsByName.Add(ssm.Name, csm);

        _shardMapsById.Add(ssm.Id, csm);
        return csm;
    }

    /// <summary>
    /// Removes shard map from cache given the name.
    /// </summary>
    /// <param name="ssm">Storage representation of shard map.</param>
    internal void Remove(IStoreShardMap ssm)
    {
        if (_shardMapsByName.ContainsKey(ssm.Name))
        {
            var csm = _shardMapsByName[ssm.Name];
            _ = _shardMapsByName.Remove(ssm.Name);

            // Dispose off the cached map
            csm?.Dispose();
        }

        if (_shardMapsById.ContainsKey(ssm.Id))
        {
            var csm = _shardMapsById[ssm.Id];
            _ = _shardMapsById.Remove(ssm.Id);

            // Dispose off the cached map
            csm?.Dispose();
        }
    }

    /// <summary>
    /// Finds shard map in cache given the name.
    /// </summary>
    /// <param name="name">Name of shard map.</param>
    /// <param name="shardMap">The found shard map object.</param>
    /// <returns>Cached shard map object.</returns>
    internal CacheShardMap LookupByName(string name, out IStoreShardMap shardMap)
    {

        _ = _shardMapsByName.TryGetValue(name, out var csm);

        shardMap = csm?.StoreShardMap;

        return csm;
    }

    /// <summary>
    /// Finds shard map in cache given the name.
    /// </summary>
    /// <param name="shardMapId">Id of shard map.</param>
    /// <returns>Cached shard map object.</returns>
    internal CacheShardMap LookupById(Guid shardMapId)
    {

        _ = _shardMapsById.TryGetValue(shardMapId, out var csm);

        return csm;
    }

    /// <summary>
    /// Clears the cache of shard maps.
    /// </summary>
    internal void Clear()
    {
        foreach (var kvp in _shardMapsByName)
            kvp.Value?.Dispose();

        _shardMapsByName.Clear();

        foreach (var kvp in _shardMapsById)
            kvp.Value?.Dispose();

        _shardMapsById.Clear();
    }
}
