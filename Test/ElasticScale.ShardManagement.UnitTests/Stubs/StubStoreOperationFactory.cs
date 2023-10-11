// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;

/// <summary>
/// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.StoreOperationFactory
/// </summary>
[DebuggerDisplay("Stub of StoreOperationFactory")]
[DebuggerNonUserCode]
internal class StubStoreOperationFactory : StoreOperationFactory
{
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping)
    /// </summary>
    internal Func<ShardMapManager, StoreOperationCode, IStoreShardMap, IStoreMapping, IStoreOperation> CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionAdds)
    /// </summary>
    internal Func<StoreOperationCode, ShardMapManager, Guid, StoreOperationState, XElement, Guid, IStoreOperation> CreateAddMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreShardMap, IStoreOperationGlobal> CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
    /// </summary>
    internal Func<ShardMapManager, Guid, StoreOperationState, XElement, IStoreOperation> CreateAddShardOperationShardMapManagerGuidStoreOperationStateXElement;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    /// </summary>
    internal Func<ShardMapManager, IStoreShardMap, IStoreShard, IStoreOperation> CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreSchemaInfo schemaInfo)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreSchemaInfo, IStoreOperationGlobal> CreateAddShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAttachShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    /// </summary>
    internal Func<ShardMapManager, IStoreShardMap, IStoreShard, IStoreOperation> CreateAttachShardOperationShardMapManagerIStoreShardMapIStoreShard;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateCheckShardLocalOperation(String operationName, ShardMapManager shardMapManager, ShardLocation location)
    /// </summary>
    internal Func<string, ShardMapManager, ShardLocation, IStoreOperationLocal> CreateCheckShardLocalOperationStringShardMapManagerShardLocation;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, ShardMapManagerCreateMode createMode, Version targetVersion)
    /// </summary>
    internal Func<SqlShardMapManagerCredentials, TransientFaultHandling.RetryPolicy, string, ShardMapManagerCreateMode, Version, IStoreOperationGlobal> CreateCreateShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringShardMapManagerCreateModeVersion;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateDetachShardGlobalOperation(ShardMapManager shardMapManager, String operationName, ShardLocation location, String shardMapName)
    /// </summary>
    internal Func<ShardMapManager, string, ShardLocation, string, IStoreOperationGlobal> CreateDetachShardGlobalOperationShardMapManagerStringShardLocationString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindMappingByIdGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreMapping mapping, ShardManagementErrorCategory errorCategory)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreShardMap, IStoreMapping, ShardManagementErrorCategory, IStoreOperationGlobal> CreateFindMappingByIdGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingShardManagementErrorCategory;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, Boolean cacheResults, Boolean ignoreFailure)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreShardMap, ShardKey, CacheStoreMappingUpdatePolicy, ShardManagementErrorCategory, bool, bool, IStoreOperationGlobal> CreateFindMappingByKeyGlobalOperationShardMapManagerStringIStoreShardMapShardKeyCacheStoreMappingUpdatePolicyShardManagementErrorCategoryBooleanBoolean;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindShardByLocationGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, ShardLocation location)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreShardMap, ShardLocation, IStoreOperationGlobal> CreateFindShardByLocationGlobalOperationShardMapManagerStringIStoreShardMapShardLocation;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindShardMapByNameGlobalOperation(ShardMapManager shardMapManager, String operationName, String shardMapName)
    /// </summary>
    internal Func<ShardMapManager, string, string, IStoreOperationGlobal> CreateFindShardMapByNameGlobalOperationShardMapManagerStringString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, String schemaInfoName)
    /// </summary>
    internal Func<ShardMapManager, string, string, IStoreOperationGlobal> CreateFindShardingSchemaInfoGlobalOperationShardMapManagerStringString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager, String operationName)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreOperationGlobal> CreateGetDistinctShardLocationsGlobalOperationShardMapManagerString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, ShardManagementErrorCategory errorCategory, Boolean cacheResults, Boolean ignoreFailure)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreShardMap, IStoreShard, ShardRange, ShardManagementErrorCategory, bool, bool, IStoreOperationGlobal> CreateGetMappingsByRangeGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardShardRangeShardManagementErrorCategoryBooleanBoolean;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetMappingsByRangeLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, Boolean ignoreFailure)
    /// </summary>
    internal Func<ShardMapManager, ShardLocation, string, IStoreShardMap, IStoreShard, ShardRange, bool, IStoreOperationLocal> CreateGetMappingsByRangeLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardShardRangeBoolean;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, Boolean throwOnFailure)
    /// </summary>
    internal Func<SqlShardMapManagerCredentials, TransientFaultHandling.RetryPolicy, string, bool, IStoreOperationGlobal> CreateGetShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringBoolean;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardMapsGlobalOperation(ShardMapManager shardMapManager, String operationName)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreOperationGlobal> CreateGetShardMapsGlobalOperationShardMapManagerString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager, String operationName)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreOperationGlobal> CreateGetShardingSchemaInfosGlobalOperationShardMapManagerString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardsGlobalOperation(String operationName, ShardMapManager shardMapManager, IStoreShardMap shardMap)
    /// </summary>
    internal Func<string, ShardMapManager, IStoreShardMap, IStoreOperationGlobal> CreateGetShardsGlobalOperationStringShardMapManagerIStoreShardMap;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName)
    /// </summary>
    internal Func<ShardMapManager, ShardLocation, string, IStoreOperationLocal> CreateGetShardsLocalOperationShardMapManagerShardLocationString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateLoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, String operationName)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreOperationGlobal> CreateLoadShardMapManagerGlobalOperationShardMapManagerString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateLockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreShardMap, IStoreMapping, Guid, LockOwnerIdOpType, ShardManagementErrorCategory, IStoreOperationGlobal> CreateLockOrUnLockMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingGuidLockOwnerIdOpTypeShardManagementErrorCategory;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId)
    /// </summary>
    internal Func<ShardMapManager, StoreOperationCode, IStoreShardMap, IStoreMapping, Guid, IStoreOperation> CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionRemoves)
    /// </summary>
    internal Func<StoreOperationCode, ShardMapManager, Guid, StoreOperationState, XElement, Guid, IStoreOperation> CreateRemoveMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreShardMap, IStoreOperationGlobal> CreateRemoveShardMapGlobalOperationShardMapManagerStringIStoreShardMap;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
    /// </summary>
    internal Func<ShardMapManager, Guid, StoreOperationState, XElement, IStoreOperation> CreateRemoveShardOperationShardMapManagerGuidStoreOperationStateXElement;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    /// </summary>
    internal Func<ShardMapManager, IStoreShardMap, IStoreShard, IStoreOperation> CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, String schemaInfoName)
    /// </summary>
    internal Func<ShardMapManager, string, string, IStoreOperationGlobal> CreateRemoveShardingSchemaInfoGlobalOperationShardMapManagerStringString;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable`1&lt;IStoreMapping&gt; mappingsToRemove, IEnumerable`1&lt;IStoreMapping&gt; mappingsToAdd)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreShardMap, IStoreShard, IEnumerable<IStoreMapping>, IEnumerable<IStoreMapping>, IStoreOperationGlobal> CreateReplaceMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardIEnumerableOfIStoreMappingIEnumerableOfIStoreMapping;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable`1&lt;ShardRange&gt; rangesToRemove, IEnumerable`1&lt;IStoreMapping&gt; mappingsToAdd)
    /// </summary>
    internal Func<ShardMapManager, ShardLocation, string, IStoreShardMap, IStoreShard, IEnumerable<ShardRange>, IEnumerable<IStoreMapping>, IStoreOperationLocal> CreateReplaceMappingsLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardIEnumerableOfShardRangeIEnumerableOfIStoreMapping;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, Tuple`2&lt;IStoreMapping,Guid&gt;[] mappingsSource, Tuple`2&lt;IStoreMapping,Guid&gt;[] mappingsTarget)
    /// </summary>
    internal Func<ShardMapManager, StoreOperationCode, IStoreShardMap, Tuple<IStoreMapping, Guid>[], Tuple<IStoreMapping, Guid>[], IStoreOperation> CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionAdds)
    /// </summary>
    internal Func<StoreOperationCode, ShardMapManager, Guid, StoreOperationState, XElement, Guid, IStoreOperation> CreateReplaceMappingsOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpdateMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mappingSource, IStoreMapping mappingTarget, String patternForKill, Guid lockOwnerId, Bool killConnection)
    /// </summary>
    internal Func<ShardMapManager, StoreOperationCode, IStoreShardMap, IStoreMapping, IStoreMapping, string, Guid, bool, IStoreOperation> CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpdateMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionRemoves, Guid originalShardVersionAdds)
    /// </summary>
    internal Func<StoreOperationCode, ShardMapManager, Guid, StoreOperationState, XElement, Guid, Guid, IStoreOperation> CreateUpdateMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuidGuid;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpdateShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
    /// </summary>
    internal Func<ShardMapManager, Guid, StoreOperationState, XElement, IStoreOperation> CreateUpdateShardOperationShardMapManagerGuidStoreOperationStateXElement;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpdateShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew)
    /// </summary>
    internal Func<ShardMapManager, IStoreShardMap, IStoreShard, IStoreShard, IStoreOperation> CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpdateShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreSchemaInfo schemaInfo)
    /// </summary>
    internal Func<ShardMapManager, string, IStoreSchemaInfo, IStoreOperationGlobal> CreateUpdateShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpgradeStoreGlobalOperation(ShardMapManager shardMapManager, String operationName, Version targetVersion)
    /// </summary>
    internal Func<ShardMapManager, string, Version, IStoreOperationGlobal> CreateUpgradeStoreGlobalOperationShardMapManagerStringVersion;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpgradeStoreLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, Version targetVersion)
    /// </summary>
    internal Func<ShardMapManager, ShardLocation, string, Version, IStoreOperationLocal> CreateUpgradeStoreLocalOperationShardMapManagerShardLocationStringVersion;
    /// <summary>
    /// Sets the stub of StoreOperationFactory.FromLogEntry(ShardMapManager shardMapManager, IStoreLogEntry so)
    /// </summary>
    internal Func<ShardMapManager, IStoreLogEntry, IStoreOperation> FromLogEntryShardMapManagerIStoreLogEntry;
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
    public StubStoreOperationFactory() => InitializeStub();

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping)
    /// </summary>
    public override IStoreOperation CreateAddMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping)
    {
        var func1 = CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping;
        return func1 != null
            ? func1(shardMapManager, operationCode, shardMap, mapping)
            : CallBase
            ? base.CreateAddMappingOperation(shardMapManager, operationCode, shardMap, mapping)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAddMappingOperation");
    }

    public override IStoreOperation CreateAddMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionAdds)
    {
        var func1 = CreateAddMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
        return func1 != null
            ? func1(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionAdds)
            : CallBase
            ? base.CreateAddMappingOperation(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionAdds)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAddMappingOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap)
    /// </summary>
    public override IStoreOperationGlobal CreateAddShardMapGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap)
    {
        var func1 = CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMap)
            : CallBase
            ? base.CreateAddShardMapGlobalOperation(shardMapManager, operationName, shardMap)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateAddShardMapGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    /// </summary>
    public override IStoreOperation CreateAddShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    {
        var func1 = CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard;
        return func1 != null
            ? func1(shardMapManager, shardMap, shard)
            : CallBase
            ? base.CreateAddShardOperation(shardMapManager, shardMap, shard)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAddShardOperation");
    }

    public override IStoreOperation CreateAddShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
    {
        var func1 = CreateAddShardOperationShardMapManagerGuidStoreOperationStateXElement;
        return func1 != null
            ? func1(shardMapManager, operationId, undoStartState, root)
            : CallBase
            ? base.CreateAddShardOperation(shardMapManager, operationId, undoStartState, root)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAddShardOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreSchemaInfo schemaInfo)
    /// </summary>
    public override IStoreOperationGlobal CreateAddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreSchemaInfo schemaInfo)
    {
        var func1 = CreateAddShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo;
        return func1 != null
            ? func1(shardMapManager, operationName, schemaInfo)
            : CallBase
            ? base.CreateAddShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateAddShardingSchemaInfoGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateAttachShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    /// </summary>
    public override IStoreOperation CreateAttachShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    {
        var func1 = CreateAttachShardOperationShardMapManagerIStoreShardMapIStoreShard;
        return func1 != null
            ? func1(shardMapManager, shardMap, shard)
            : CallBase
            ? base.CreateAttachShardOperation(shardMapManager, shardMap, shard)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAttachShardOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateCheckShardLocalOperation(String operationName, ShardMapManager shardMapManager, ShardLocation location)
    /// </summary>
    public override IStoreOperationLocal CreateCheckShardLocalOperation(string operationName, ShardMapManager shardMapManager, ShardLocation location)
    {
        var func1 = CreateCheckShardLocalOperationStringShardMapManagerShardLocation;
        return func1 != null
            ? func1(operationName, shardMapManager, location)
            : CallBase
            ? base.CreateCheckShardLocalOperation(operationName, shardMapManager, location)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateCheckShardLocalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, ShardMapManagerCreateMode createMode, Version targetVersion)
    /// </summary>
    public override IStoreOperationGlobal CreateCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, TransientFaultHandling.RetryPolicy retryPolicy, string operationName, ShardMapManagerCreateMode createMode, Version targetVersion)
    {
        var func1 = CreateCreateShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringShardMapManagerCreateModeVersion;
        return func1 != null
            ? func1(credentials, retryPolicy, operationName, createMode, targetVersion)
            : CallBase
            ? base.CreateCreateShardMapManagerGlobalOperation(credentials, retryPolicy, operationName, createMode, targetVersion)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateCreateShardMapManagerGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateDetachShardGlobalOperation(ShardMapManager shardMapManager, String operationName, ShardLocation location, String shardMapName)
    /// </summary>
    public override IStoreOperationGlobal CreateDetachShardGlobalOperation(ShardMapManager shardMapManager, string operationName, ShardLocation location, string shardMapName)
    {
        var func1 = CreateDetachShardGlobalOperationShardMapManagerStringShardLocationString;
        return func1 != null
            ? func1(shardMapManager, operationName, location, shardMapName)
            : CallBase
            ? base.CreateDetachShardGlobalOperation(shardMapManager, operationName, location, shardMapName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateDetachShardGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindMappingByIdGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreMapping mapping, ShardManagementErrorCategory errorCategory)
    /// </summary>
    public override IStoreOperationGlobal CreateFindMappingByIdGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreMapping mapping, ShardManagementErrorCategory errorCategory)
    {
        var func1 = CreateFindMappingByIdGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingShardManagementErrorCategory;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMap, mapping, errorCategory)
            : CallBase
            ? base.CreateFindMappingByIdGlobalOperation(shardMapManager, operationName, shardMap, mapping, errorCategory)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindMappingByIdGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, Boolean cacheResults, Boolean ignoreFailure)
    /// </summary>
    public override IStoreOperationGlobal CreateFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, bool cacheResults, bool ignoreFailure)
    {
        var func1 = CreateFindMappingByKeyGlobalOperationShardMapManagerStringIStoreShardMapShardKeyCacheStoreMappingUpdatePolicyShardManagementErrorCategoryBooleanBoolean;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults, ignoreFailure)
            : CallBase
            ? base.CreateFindMappingByKeyGlobalOperation(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults, ignoreFailure)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindMappingByKeyGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindShardByLocationGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, ShardLocation location)
    /// </summary>
    public override IStoreOperationGlobal CreateFindShardByLocationGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, ShardLocation location)
    {
        var func1 = CreateFindShardByLocationGlobalOperationShardMapManagerStringIStoreShardMapShardLocation;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMap, location)
            : CallBase
            ? base.CreateFindShardByLocationGlobalOperation(shardMapManager, operationName, shardMap, location)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindShardByLocationGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindShardMapByNameGlobalOperation(ShardMapManager shardMapManager, String operationName, String shardMapName)
    /// </summary>
    public override IStoreOperationGlobal CreateFindShardMapByNameGlobalOperation(ShardMapManager shardMapManager, string operationName, string shardMapName)
    {
        var func1 = CreateFindShardMapByNameGlobalOperationShardMapManagerStringString;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMapName)
            : CallBase
            ? base.CreateFindShardMapByNameGlobalOperation(shardMapManager, operationName, shardMapName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindShardMapByNameGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateFindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, String schemaInfoName)
    /// </summary>
    public override IStoreOperationGlobal CreateFindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, string schemaInfoName)
    {
        var func1 = CreateFindShardingSchemaInfoGlobalOperationShardMapManagerStringString;
        return func1 != null
            ? func1(shardMapManager, operationName, schemaInfoName)
            : CallBase
            ? base.CreateFindShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfoName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindShardingSchemaInfoGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager, String operationName)
    /// </summary>
    public override IStoreOperationGlobal CreateGetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager, string operationName)
    {
        var func1 = CreateGetDistinctShardLocationsGlobalOperationShardMapManagerString;
        return func1 != null
            ? func1(shardMapManager, operationName)
            : CallBase
            ? base.CreateGetDistinctShardLocationsGlobalOperation(shardMapManager, operationName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetDistinctShardLocationsGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, ShardManagementErrorCategory errorCategory, Boolean cacheResults, Boolean ignoreFailure)
    /// </summary>
    public override IStoreOperationGlobal CreateGetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, ShardManagementErrorCategory errorCategory, bool cacheResults, bool ignoreFailure)
    {
        var func1 = CreateGetMappingsByRangeGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardShardRangeShardManagementErrorCategoryBooleanBoolean;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMap, shard, range, errorCategory, cacheResults, ignoreFailure)
            : CallBase
            ? base.CreateGetMappingsByRangeGlobalOperation(shardMapManager, operationName, shardMap, shard, range, errorCategory, cacheResults, ignoreFailure)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetMappingsByRangeGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetMappingsByRangeLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, Boolean ignoreFailure)
    /// </summary>
    public override IStoreOperationLocal CreateGetMappingsByRangeLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, bool ignoreFailure)
    {
        var func1 = CreateGetMappingsByRangeLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardShardRangeBoolean;
        return func1 != null
            ? func1(shardMapManager, location, operationName, shardMap, shard, range, ignoreFailure)
            : CallBase
            ? base.CreateGetMappingsByRangeLocalOperation(shardMapManager, location, operationName, shardMap, shard, range, ignoreFailure)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateGetMappingsByRangeLocalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, Boolean throwOnFailure)
    /// </summary>
    public override IStoreOperationGlobal CreateGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, TransientFaultHandling.RetryPolicy retryPolicy, string operationName, bool throwOnFailure)
    {
        var func1 = CreateGetShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringBoolean;
        return func1 != null
            ? func1(credentials, retryPolicy, operationName, throwOnFailure)
            : CallBase
            ? base.CreateGetShardMapManagerGlobalOperation(credentials, retryPolicy, operationName, throwOnFailure)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetShardMapManagerGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardMapsGlobalOperation(ShardMapManager shardMapManager, String operationName)
    /// </summary>
    public override IStoreOperationGlobal CreateGetShardMapsGlobalOperation(ShardMapManager shardMapManager, string operationName)
    {
        var func1 = CreateGetShardMapsGlobalOperationShardMapManagerString;
        return func1 != null
            ? func1(shardMapManager, operationName)
            : CallBase
            ? base.CreateGetShardMapsGlobalOperation(shardMapManager, operationName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetShardMapsGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager, String operationName)
    /// </summary>
    public override IStoreOperationGlobal CreateGetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager, string operationName)
    {
        var func1 = CreateGetShardingSchemaInfosGlobalOperationShardMapManagerString;
        return func1 != null
            ? func1(shardMapManager, operationName)
            : CallBase
            ? base.CreateGetShardingSchemaInfosGlobalOperation(shardMapManager, operationName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetShardingSchemaInfosGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardsGlobalOperation(String operationName, ShardMapManager shardMapManager, IStoreShardMap shardMap)
    /// </summary>
    public override IStoreOperationGlobal CreateGetShardsGlobalOperation(string operationName, ShardMapManager shardMapManager, IStoreShardMap shardMap)
    {
        var func1 = CreateGetShardsGlobalOperationStringShardMapManagerIStoreShardMap;
        return func1 != null
            ? func1(operationName, shardMapManager, shardMap)
            : CallBase
            ? base.CreateGetShardsGlobalOperation(operationName, shardMapManager, shardMap)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetShardsGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateGetShardsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName)
    /// </summary>
    public override IStoreOperationLocal CreateGetShardsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName)
    {
        var func1 = CreateGetShardsLocalOperationShardMapManagerShardLocationString;
        return func1 != null
            ? func1(shardMapManager, location, operationName)
            : CallBase
            ? base.CreateGetShardsLocalOperation(shardMapManager, location, operationName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateGetShardsLocalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateLoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, String operationName)
    /// </summary>
    public override IStoreOperationGlobal CreateLoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, string operationName)
    {
        var func1 = CreateLoadShardMapManagerGlobalOperationShardMapManagerString;
        return func1 != null
            ? func1(shardMapManager, operationName)
            : CallBase
            ? base.CreateLoadShardMapManagerGlobalOperation(shardMapManager, operationName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateLoadShardMapManagerGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateLockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory)
    /// </summary>
    public override IStoreOperationGlobal CreateLockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory)
    {
        var func1 = CreateLockOrUnLockMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingGuidLockOwnerIdOpTypeShardManagementErrorCategory;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMap, mapping, lockOwnerId, lockOpType, errorCategory)
            : CallBase
            ? base.CreateLockOrUnLockMappingsGlobalOperation(shardMapManager, operationName, shardMap, mapping, lockOwnerId, lockOpType, errorCategory)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateLockOrUnLockMappingsGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId)
    /// </summary>
    public override IStoreOperation CreateRemoveMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId)
    {
        var func1 = CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid;
        return func1 != null
            ? func1(shardMapManager, operationCode, shardMap, mapping, lockOwnerId)
            : CallBase
            ? base.CreateRemoveMappingOperation(shardMapManager, operationCode, shardMap, mapping, lockOwnerId)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateRemoveMappingOperation");
    }

    public override IStoreOperation CreateRemoveMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionRemoves)
    {
        var func1 = CreateRemoveMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
        return func1 != null
            ? func1(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionRemoves)
            : CallBase
            ? base.CreateRemoveMappingOperation(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionRemoves)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateRemoveMappingOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap)
    /// </summary>
    public override IStoreOperationGlobal CreateRemoveShardMapGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap)
    {
        var func1 = CreateRemoveShardMapGlobalOperationShardMapManagerStringIStoreShardMap;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMap)
            : CallBase
            ? base.CreateRemoveShardMapGlobalOperation(shardMapManager, operationName, shardMap)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateRemoveShardMapGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    /// </summary>
    public override IStoreOperation CreateRemoveShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
    {
        var func1 = CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard;
        return func1 != null
            ? func1(shardMapManager, shardMap, shard)
            : CallBase
            ? base.CreateRemoveShardOperation(shardMapManager, shardMap, shard)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateRemoveShardOperation");
    }

    public override IStoreOperation CreateRemoveShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
    {
        var func1 = CreateRemoveShardOperationShardMapManagerGuidStoreOperationStateXElement;
        return func1 != null
            ? func1(shardMapManager, operationId, undoStartState, root)
            : CallBase
            ? base.CreateRemoveShardOperation(shardMapManager, operationId, undoStartState, root)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateRemoveShardOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateRemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, String schemaInfoName)
    /// </summary>
    public override IStoreOperationGlobal CreateRemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, string schemaInfoName)
    {
        var func1 = CreateRemoveShardingSchemaInfoGlobalOperationShardMapManagerStringString;
        return func1 != null
            ? func1(shardMapManager, operationName, schemaInfoName)
            : CallBase
            ? base.CreateRemoveShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfoName)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateRemoveShardingSchemaInfoGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable`1&lt;IStoreMapping&gt; mappingsToRemove, IEnumerable`1&lt;IStoreMapping&gt; mappingsToAdd)
    /// </summary>
    public override IStoreOperationGlobal CreateReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable<IStoreMapping> mappingsToRemove, IEnumerable<IStoreMapping> mappingsToAdd)
    {
        var func1 = CreateReplaceMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardIEnumerableOfIStoreMappingIEnumerableOfIStoreMapping;
        return func1 != null
            ? func1(shardMapManager, operationName, shardMap, shard, mappingsToRemove, mappingsToAdd)
            : CallBase
            ? base.CreateReplaceMappingsGlobalOperation(shardMapManager, operationName, shardMap, shard, mappingsToRemove, mappingsToAdd)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateReplaceMappingsGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable`1&lt;ShardRange&gt; rangesToRemove, IEnumerable`1&lt;IStoreMapping&gt; mappingsToAdd)
    /// </summary>
    public override IStoreOperationLocal CreateReplaceMappingsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable<ShardRange> rangesToRemove, IEnumerable<IStoreMapping> mappingsToAdd)
    {
        var func1 = CreateReplaceMappingsLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardIEnumerableOfShardRangeIEnumerableOfIStoreMapping;
        return func1 != null
            ? func1(shardMapManager, location, operationName, shardMap, shard, rangesToRemove, mappingsToAdd)
            : CallBase
            ? base.CreateReplaceMappingsLocalOperation(shardMapManager, location, operationName, shardMap, shard, rangesToRemove, mappingsToAdd)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateReplaceMappingsLocalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, Tuple`2&lt;IStoreMapping,Guid&gt;[] mappingsSource, Tuple`2&lt;IStoreMapping,Guid&gt;[] mappingsTarget)
    /// </summary>
    public override IStoreOperation CreateReplaceMappingsOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, Tuple<IStoreMapping, Guid>[] mappingsSource, Tuple<IStoreMapping, Guid>[] mappingsTarget)
    {
        var func1 = CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray;
        return func1 != null
            ? func1(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget)
            : CallBase
            ? base.CreateReplaceMappingsOperation(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateReplaceMappingsOperation");
    }

    public override IStoreOperation CreateReplaceMappingsOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionAdds)
    {
        var func1 = CreateReplaceMappingsOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
        return func1 != null
            ? func1(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionAdds)
            : CallBase
            ? base.CreateReplaceMappingsOperation(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionAdds)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateReplaceMappingsOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpdateMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mappingSource, IStoreMapping mappingTarget, String patternForKill, Guid lockOwnerId, bool KillConnection)
    /// </summary>
    public override IStoreOperation CreateUpdateMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mappingSource, IStoreMapping mappingTarget, string patternForKill, Guid lockOwnerId, bool killConnection)
    {
        var func1 = CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool;
        return func1 != null
            ? func1(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill, lockOwnerId, killConnection)
            : CallBase
            ? base.CreateUpdateMappingOperation(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill, lockOwnerId, killConnection)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateUpdateMappingOperation");
    }

    public override IStoreOperation CreateUpdateMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionRemoves, Guid originalShardVersionAdds)
    {
        var func1 = CreateUpdateMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuidGuid;
        return func1 != null
            ? func1(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionRemoves, originalShardVersionAdds)
            : CallBase
            ? base.CreateUpdateMappingOperation(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionRemoves, originalShardVersionAdds)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateUpdateMappingOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpdateShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew)
    /// </summary>
    public override IStoreOperation CreateUpdateShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew)
    {
        var func1 = CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard;
        return func1 != null
            ? func1(shardMapManager, shardMap, shardOld, shardNew)
            : CallBase
            ? base.CreateUpdateShardOperation(shardMapManager, shardMap, shardOld, shardNew)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateUpdateShardOperation");
    }

    public override IStoreOperation CreateUpdateShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
    {
        var func1 = CreateUpdateShardOperationShardMapManagerGuidStoreOperationStateXElement;
        return func1 != null
            ? func1(shardMapManager, operationId, undoStartState, root)
            : CallBase
            ? base.CreateUpdateShardOperation(shardMapManager, operationId, undoStartState, root)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateUpdateShardOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpdateShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreSchemaInfo schemaInfo)
    /// </summary>
    public override IStoreOperationGlobal CreateUpdateShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreSchemaInfo schemaInfo)
    {
        var func1 = CreateUpdateShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo;
        return func1 != null
            ? func1(shardMapManager, operationName, schemaInfo)
            : CallBase
            ? base.CreateUpdateShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateUpdateShardingSchemaInfoGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpgradeStoreGlobalOperation(ShardMapManager shardMapManager, String operationName, Version targetVersion)
    /// </summary>
    public override IStoreOperationGlobal CreateUpgradeStoreGlobalOperation(ShardMapManager shardMapManager, string operationName, Version targetVersion)
    {
        var func1 = CreateUpgradeStoreGlobalOperationShardMapManagerStringVersion;
        return func1 != null
            ? func1(shardMapManager, operationName, targetVersion)
            : CallBase
            ? base.CreateUpgradeStoreGlobalOperation(shardMapManager, operationName, targetVersion)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateUpgradeStoreGlobalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.CreateUpgradeStoreLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, Version targetVersion)
    /// </summary>
    public override IStoreOperationLocal CreateUpgradeStoreLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName, Version targetVersion)
    {
        var func1 = CreateUpgradeStoreLocalOperationShardMapManagerShardLocationStringVersion;
        return func1 != null
            ? func1(shardMapManager, location, operationName, targetVersion)
            : CallBase
            ? base.CreateUpgradeStoreLocalOperation(shardMapManager, location, operationName, targetVersion)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateUpgradeStoreLocalOperation");
    }

    /// <summary>
    /// Sets the stub of StoreOperationFactory.FromLogEntry(ShardMapManager shardMapManager, IStoreLogEntry so)
    /// </summary>
    public override IStoreOperation FromLogEntry(ShardMapManager shardMapManager, IStoreLogEntry so)
    {
        var func1 = FromLogEntryShardMapManagerIStoreLogEntry;
        return func1 != null
            ? func1(shardMapManager, so)
            : CallBase
            ? base.FromLogEntry(shardMapManager, so)
            : InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "FromLogEntry");
    }

    /// <summary>
    /// Initializes a new instance of type StubStoreOperationFactory
    /// </summary>
    private void InitializeStub()
    {
    }
}
