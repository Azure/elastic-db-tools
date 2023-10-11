// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;

/// <summary>
/// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.CacheStore
/// </summary>
[DebuggerDisplay("Stub of CacheStore")]
[DebuggerNonUserCode]
internal class StubCacheStore : CacheStore
{
    /// <summary>
    /// Sets the stub of CacheStore.AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
    /// </summary>
    internal Action<IStoreMapping, CacheStoreMappingUpdatePolicy> AddOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy;
    /// <summary>
    /// Sets the stub of CacheStore.AddOrUpdateShardMap(IStoreShardMap shardMap)
    /// </summary>
    internal Action<IStoreShardMap> AddOrUpdateShardMapIStoreShardMap;
    /// <summary>
    /// Sets the stub of CacheStore.Clear()
    /// </summary>
    public Action Clear01;
    /// <summary>
    /// Sets the stub of CacheStore.DeleteMapping(IStoreMapping mapping)
    /// </summary>
    internal Action<IStoreMapping> DeleteMappingIStoreMapping;
    /// <summary>
    /// Sets the stub of CacheStore.DeleteShardMap(IStoreShardMap shardMap)
    /// </summary>
    internal Action<IStoreShardMap> DeleteShardMapIStoreShardMap;
    /// <summary>
    /// Sets the stub of CacheStore.Dispose(Boolean disposing)
    /// </summary>
    public Action<bool> DisposeBoolean;
    /// <summary>
    /// Sets the stub of CacheStore.LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
    /// </summary>
    internal Func<IStoreShardMap, ShardKey, ICacheStoreMapping> LookupMappingByKeyIStoreShardMapShardKey;
    /// <summary>
    /// Sets the stub of CacheStore.LookupShardMapByName(String shardMapName)
    /// </summary>
    internal Func<string, IStoreShardMap> LookupShardMapByNameString;
    private IStubBehavior ___instanceBehavior;

    /// <summary>
    /// Gets or sets a value that indicates if the base method should be called instead of the fallback behavior
    /// </summary>
    public bool CallBase { get; set; }

    /// <summary>
    /// Gets or sets the instance behavior.
    /// </summary>
    public IStubBehavior InstanceBehavior
    {
        get => StubBehaviors.GetValueOrCurrent(___instanceBehavior);
        set => ___instanceBehavior = value;
    }

    /// <summary>
    /// Initializes a new instance
    /// </summary>
    public StubCacheStore() => InitializeStub();

    /// <summary>
    /// Sets the stub of CacheStore.AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
    /// </summary>
    public override void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
    {
        var action1 = AddOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy;
        if (action1 != null)
            action1(mapping, policy);
        else if (CallBase)
            base.AddOrUpdateMapping(mapping, policy);
        else
            InstanceBehavior.VoidResult<StubCacheStore>(this, "AddOrUpdateMapping");
    }

    /// <summary>
    /// Sets the stub of CacheStore.AddOrUpdateShardMap(IStoreShardMap shardMap)
    /// </summary>
    public override void AddOrUpdateShardMap(IStoreShardMap shardMap)
    {
        var action1 = AddOrUpdateShardMapIStoreShardMap;
        if (action1 != null)
            action1(shardMap);
        else if (CallBase)
            base.AddOrUpdateShardMap(shardMap);
        else
            InstanceBehavior.VoidResult<StubCacheStore>(this, "AddOrUpdateShardMap");
    }

    /// <summary>
    /// Sets the stub of CacheStore.Clear()
    /// </summary>
    public override void Clear()
    {
        var action1 = Clear01;
        if (action1 != null)
            action1();
        else if (CallBase)
            base.Clear();
        else
            InstanceBehavior.VoidResult<StubCacheStore>(this, "Clear");
    }

    /// <summary>
    /// Sets the stub of CacheStore.DeleteMapping(IStoreMapping mapping)
    /// </summary>
    public override void DeleteMapping(IStoreMapping mapping)
    {
        var action1 = DeleteMappingIStoreMapping;
        if (action1 != null)
            action1(mapping);
        else if (CallBase)
            base.DeleteMapping(mapping);
        else
            InstanceBehavior.VoidResult<StubCacheStore>(this, "DeleteMapping");
    }

    /// <summary>
    /// Sets the stub of CacheStore.DeleteShardMap(IStoreShardMap shardMap)
    /// </summary>
    public override void DeleteShardMap(IStoreShardMap shardMap)
    {
        var action1 = DeleteShardMapIStoreShardMap;
        if (action1 != null)
            action1(shardMap);
        else if (CallBase)
            base.DeleteShardMap(shardMap);
        else
            InstanceBehavior.VoidResult<StubCacheStore>(this, "DeleteShardMap");
    }

    /// <summary>
    /// Sets the stub of CacheStore.Dispose(Boolean disposing)
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        var action1 = DisposeBoolean;
        if (action1 != null)
            action1(disposing);
        else if (CallBase)
            base.Dispose(disposing);
        else
            InstanceBehavior.VoidResult<StubCacheStore>(this, "Dispose");
    }

    /// <summary>
    /// Initializes a new instance of type StubCacheStore
    /// </summary>
    private void InitializeStub()
    {
    }

    /// <summary>
    /// Sets the stub of CacheStore.LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
    /// </summary>
    public override ICacheStoreMapping LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
    {
        var func1 = LookupMappingByKeyIStoreShardMapShardKey;
        return func1 != null
            ? func1(shardMap, key)
            : CallBase
            ? base.LookupMappingByKey(shardMap, key)
            : InstanceBehavior.Result<StubCacheStore, ICacheStoreMapping>(this, "LookupMappingByKey");
    }

    /// <summary>
    /// Sets the stub of CacheStore.LookupShardMapByName(String shardMapName)
    /// </summary>
    public override IStoreShardMap LookupShardMapByName(string shardMapName)
    {
        var func1 = LookupShardMapByNameString;
        return func1 != null
            ? func1(shardMapName)
            : CallBase
            ? base.LookupShardMapByName(shardMapName)
            : InstanceBehavior.Result<StubCacheStore, IStoreShardMap>(this, "LookupShardMapByName");
    }
}
