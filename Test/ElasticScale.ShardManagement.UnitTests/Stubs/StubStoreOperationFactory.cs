// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
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
        private bool ___callBase;
        private IStubBehavior ___instanceBehavior;

        /// <summary>
        /// Gets or sets a value that indicates if the base method should be called instead of the fallback behavior
        /// </summary>
        public bool CallBase
        {
            get
            {
                return this.___callBase;
            }
            set
            {
                this.___callBase = value;
            }
        }

        /// <summary>
        /// Gets or sets the instance behavior.
        /// </summary>
        public IStubBehavior InstanceBehavior
        {
            get
            {
                return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
            }
            set
            {
                this.___instanceBehavior = value;
            }
        }

        /// <summary>
        /// Initializes a new instance
        /// </summary>
        public StubStoreOperationFactory()
        {
            this.InitializeStub();
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateAddMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping)
        /// </summary>
        public override IStoreOperation CreateAddMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping)
        {
            Func<ShardMapManager, StoreOperationCode, IStoreShardMap, IStoreMapping, IStoreOperation> func1 = this.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping;
            if (func1 != null)
                return func1(shardMapManager, operationCode, shardMap, mapping);
            if (this.___callBase)
                return base.CreateAddMappingOperation(shardMapManager, operationCode, shardMap, mapping);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAddMappingOperation");
        }

        public override IStoreOperation CreateAddMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionAdds)
        {
            Func<StoreOperationCode, ShardMapManager, Guid, StoreOperationState, XElement, Guid, IStoreOperation> func1 = this.CreateAddMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
            if (func1 != null)
                return func1(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionAdds);
            if (this.___callBase)
                return base.CreateAddMappingOperation(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionAdds);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAddMappingOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateAddShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap)
        /// </summary>
        public override IStoreOperationGlobal CreateAddShardMapGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap)
        {
            Func<ShardMapManager, string, IStoreShardMap, IStoreOperationGlobal> func1 = this.CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMap);
            if (this.___callBase)
                return base.CreateAddShardMapGlobalOperation(shardMapManager, operationName, shardMap);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateAddShardMapGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateAddShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
        /// </summary>
        public override IStoreOperation CreateAddShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
        {
            Func<ShardMapManager, IStoreShardMap, IStoreShard, IStoreOperation> func1 = this.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard;
            if (func1 != null)
                return func1(shardMapManager, shardMap, shard);
            if (this.___callBase)
                return base.CreateAddShardOperation(shardMapManager, shardMap, shard);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAddShardOperation");
        }

        public override IStoreOperation CreateAddShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
        {
            Func<ShardMapManager, Guid, StoreOperationState, XElement, IStoreOperation> func1 = this.CreateAddShardOperationShardMapManagerGuidStoreOperationStateXElement;
            if (func1 != null)
                return func1(shardMapManager, operationId, undoStartState, root);
            if (this.___callBase)
                return base.CreateAddShardOperation(shardMapManager, operationId, undoStartState, root);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAddShardOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateAddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreSchemaInfo schemaInfo)
        /// </summary>
        public override IStoreOperationGlobal CreateAddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreSchemaInfo schemaInfo)
        {
            Func<ShardMapManager, string, IStoreSchemaInfo, IStoreOperationGlobal> func1 = this.CreateAddShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo;
            if (func1 != null)
                return func1(shardMapManager, operationName, schemaInfo);
            if (this.___callBase)
                return base.CreateAddShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateAddShardingSchemaInfoGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateAttachShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
        /// </summary>
        public override IStoreOperation CreateAttachShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
        {
            Func<ShardMapManager, IStoreShardMap, IStoreShard, IStoreOperation> func1 = this.CreateAttachShardOperationShardMapManagerIStoreShardMapIStoreShard;
            if (func1 != null)
                return func1(shardMapManager, shardMap, shard);
            if (this.___callBase)
                return base.CreateAttachShardOperation(shardMapManager, shardMap, shard);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateAttachShardOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateCheckShardLocalOperation(String operationName, ShardMapManager shardMapManager, ShardLocation location)
        /// </summary>
        public override IStoreOperationLocal CreateCheckShardLocalOperation(string operationName, ShardMapManager shardMapManager, ShardLocation location)
        {
            Func<string, ShardMapManager, ShardLocation, IStoreOperationLocal> func1 = this.CreateCheckShardLocalOperationStringShardMapManagerShardLocation;
            if (func1 != null)
                return func1(operationName, shardMapManager, location);
            if (this.___callBase)
                return base.CreateCheckShardLocalOperation(operationName, shardMapManager, location);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateCheckShardLocalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, ShardMapManagerCreateMode createMode, Version targetVersion)
        /// </summary>
        public override IStoreOperationGlobal CreateCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, TransientFaultHandling.RetryPolicy retryPolicy, string operationName, ShardMapManagerCreateMode createMode, Version targetVersion)
        {
            Func<SqlShardMapManagerCredentials, TransientFaultHandling.RetryPolicy, string, ShardMapManagerCreateMode, Version, IStoreOperationGlobal> func1 = this.CreateCreateShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringShardMapManagerCreateModeVersion;
            if (func1 != null)
                return func1(credentials, retryPolicy, operationName, createMode, targetVersion);
            if (this.___callBase)
                return base.CreateCreateShardMapManagerGlobalOperation(credentials, retryPolicy, operationName, createMode, targetVersion);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateCreateShardMapManagerGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateDetachShardGlobalOperation(ShardMapManager shardMapManager, String operationName, ShardLocation location, String shardMapName)
        /// </summary>
        public override IStoreOperationGlobal CreateDetachShardGlobalOperation(ShardMapManager shardMapManager, string operationName, ShardLocation location, string shardMapName)
        {
            Func<ShardMapManager, string, ShardLocation, string, IStoreOperationGlobal> func1 = this.CreateDetachShardGlobalOperationShardMapManagerStringShardLocationString;
            if (func1 != null)
                return func1(shardMapManager, operationName, location, shardMapName);
            if (this.___callBase)
                return base.CreateDetachShardGlobalOperation(shardMapManager, operationName, location, shardMapName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateDetachShardGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateFindMappingByIdGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreMapping mapping, ShardManagementErrorCategory errorCategory)
        /// </summary>
        public override IStoreOperationGlobal CreateFindMappingByIdGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreMapping mapping, ShardManagementErrorCategory errorCategory)
        {
            Func<ShardMapManager, string, IStoreShardMap, IStoreMapping, ShardManagementErrorCategory, IStoreOperationGlobal> func1 = this.CreateFindMappingByIdGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingShardManagementErrorCategory;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMap, mapping, errorCategory);
            if (this.___callBase)
                return base.CreateFindMappingByIdGlobalOperation(shardMapManager, operationName, shardMap, mapping, errorCategory);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindMappingByIdGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, Boolean cacheResults, Boolean ignoreFailure)
        /// </summary>
        public override IStoreOperationGlobal CreateFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, bool cacheResults, bool ignoreFailure)
        {
            Func<ShardMapManager, string, IStoreShardMap, ShardKey, CacheStoreMappingUpdatePolicy, ShardManagementErrorCategory, bool, bool, IStoreOperationGlobal> func1 = this.CreateFindMappingByKeyGlobalOperationShardMapManagerStringIStoreShardMapShardKeyCacheStoreMappingUpdatePolicyShardManagementErrorCategoryBooleanBoolean;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults, ignoreFailure);
            if (this.___callBase)
                return base.CreateFindMappingByKeyGlobalOperation(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults, ignoreFailure);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindMappingByKeyGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateFindShardByLocationGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, ShardLocation location)
        /// </summary>
        public override IStoreOperationGlobal CreateFindShardByLocationGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, ShardLocation location)
        {
            Func<ShardMapManager, string, IStoreShardMap, ShardLocation, IStoreOperationGlobal> func1 = this.CreateFindShardByLocationGlobalOperationShardMapManagerStringIStoreShardMapShardLocation;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMap, location);
            if (this.___callBase)
                return base.CreateFindShardByLocationGlobalOperation(shardMapManager, operationName, shardMap, location);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindShardByLocationGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateFindShardMapByNameGlobalOperation(ShardMapManager shardMapManager, String operationName, String shardMapName)
        /// </summary>
        public override IStoreOperationGlobal CreateFindShardMapByNameGlobalOperation(ShardMapManager shardMapManager, string operationName, string shardMapName)
        {
            Func<ShardMapManager, string, string, IStoreOperationGlobal> func1 = this.CreateFindShardMapByNameGlobalOperationShardMapManagerStringString;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMapName);
            if (this.___callBase)
                return base.CreateFindShardMapByNameGlobalOperation(shardMapManager, operationName, shardMapName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindShardMapByNameGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateFindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, String schemaInfoName)
        /// </summary>
        public override IStoreOperationGlobal CreateFindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, string schemaInfoName)
        {
            Func<ShardMapManager, string, string, IStoreOperationGlobal> func1 = this.CreateFindShardingSchemaInfoGlobalOperationShardMapManagerStringString;
            if (func1 != null)
                return func1(shardMapManager, operationName, schemaInfoName);
            if (this.___callBase)
                return base.CreateFindShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfoName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateFindShardingSchemaInfoGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateGetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager, String operationName)
        /// </summary>
        public override IStoreOperationGlobal CreateGetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager, string operationName)
        {
            Func<ShardMapManager, string, IStoreOperationGlobal> func1 = this.CreateGetDistinctShardLocationsGlobalOperationShardMapManagerString;
            if (func1 != null)
                return func1(shardMapManager, operationName);
            if (this.___callBase)
                return base.CreateGetDistinctShardLocationsGlobalOperation(shardMapManager, operationName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetDistinctShardLocationsGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateGetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, ShardManagementErrorCategory errorCategory, Boolean cacheResults, Boolean ignoreFailure)
        /// </summary>
        public override IStoreOperationGlobal CreateGetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, ShardManagementErrorCategory errorCategory, bool cacheResults, bool ignoreFailure)
        {
            Func<ShardMapManager, string, IStoreShardMap, IStoreShard, ShardRange, ShardManagementErrorCategory, bool, bool, IStoreOperationGlobal> func1 = this.CreateGetMappingsByRangeGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardShardRangeShardManagementErrorCategoryBooleanBoolean;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMap, shard, range, errorCategory, cacheResults, ignoreFailure);
            if (this.___callBase)
                return base.CreateGetMappingsByRangeGlobalOperation(shardMapManager, operationName, shardMap, shard, range, errorCategory, cacheResults, ignoreFailure);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetMappingsByRangeGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateGetMappingsByRangeLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, Boolean ignoreFailure)
        /// </summary>
        public override IStoreOperationLocal CreateGetMappingsByRangeLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, bool ignoreFailure)
        {
            Func<ShardMapManager, ShardLocation, string, IStoreShardMap, IStoreShard, ShardRange, bool, IStoreOperationLocal> func1 = this.CreateGetMappingsByRangeLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardShardRangeBoolean;
            if (func1 != null)
                return func1(shardMapManager, location, operationName, shardMap, shard, range, ignoreFailure);
            if (this.___callBase)
                return base.CreateGetMappingsByRangeLocalOperation(shardMapManager, location, operationName, shardMap, shard, range, ignoreFailure);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateGetMappingsByRangeLocalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy, String operationName, Boolean throwOnFailure)
        /// </summary>
        public override IStoreOperationGlobal CreateGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, TransientFaultHandling.RetryPolicy retryPolicy, string operationName, bool throwOnFailure)
        {
            Func<SqlShardMapManagerCredentials, TransientFaultHandling.RetryPolicy, string, bool, IStoreOperationGlobal> func1 = this.CreateGetShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringBoolean;
            if (func1 != null)
                return func1(credentials, retryPolicy, operationName, throwOnFailure);
            if (this.___callBase)
                return base.CreateGetShardMapManagerGlobalOperation(credentials, retryPolicy, operationName, throwOnFailure);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetShardMapManagerGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateGetShardMapsGlobalOperation(ShardMapManager shardMapManager, String operationName)
        /// </summary>
        public override IStoreOperationGlobal CreateGetShardMapsGlobalOperation(ShardMapManager shardMapManager, string operationName)
        {
            Func<ShardMapManager, string, IStoreOperationGlobal> func1 = this.CreateGetShardMapsGlobalOperationShardMapManagerString;
            if (func1 != null)
                return func1(shardMapManager, operationName);
            if (this.___callBase)
                return base.CreateGetShardMapsGlobalOperation(shardMapManager, operationName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetShardMapsGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateGetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager, String operationName)
        /// </summary>
        public override IStoreOperationGlobal CreateGetShardingSchemaInfosGlobalOperation(ShardMapManager shardMapManager, string operationName)
        {
            Func<ShardMapManager, string, IStoreOperationGlobal> func1 = this.CreateGetShardingSchemaInfosGlobalOperationShardMapManagerString;
            if (func1 != null)
                return func1(shardMapManager, operationName);
            if (this.___callBase)
                return base.CreateGetShardingSchemaInfosGlobalOperation(shardMapManager, operationName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetShardingSchemaInfosGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateGetShardsGlobalOperation(String operationName, ShardMapManager shardMapManager, IStoreShardMap shardMap)
        /// </summary>
        public override IStoreOperationGlobal CreateGetShardsGlobalOperation(string operationName, ShardMapManager shardMapManager, IStoreShardMap shardMap)
        {
            Func<string, ShardMapManager, IStoreShardMap, IStoreOperationGlobal> func1 = this.CreateGetShardsGlobalOperationStringShardMapManagerIStoreShardMap;
            if (func1 != null)
                return func1(operationName, shardMapManager, shardMap);
            if (this.___callBase)
                return base.CreateGetShardsGlobalOperation(operationName, shardMapManager, shardMap);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateGetShardsGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateGetShardsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName)
        /// </summary>
        public override IStoreOperationLocal CreateGetShardsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName)
        {
            Func<ShardMapManager, ShardLocation, string, IStoreOperationLocal> func1 = this.CreateGetShardsLocalOperationShardMapManagerShardLocationString;
            if (func1 != null)
                return func1(shardMapManager, location, operationName);
            if (this.___callBase)
                return base.CreateGetShardsLocalOperation(shardMapManager, location, operationName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateGetShardsLocalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateLoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, String operationName)
        /// </summary>
        public override IStoreOperationGlobal CreateLoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, string operationName)
        {
            Func<ShardMapManager, string, IStoreOperationGlobal> func1 = this.CreateLoadShardMapManagerGlobalOperationShardMapManagerString;
            if (func1 != null)
                return func1(shardMapManager, operationName);
            if (this.___callBase)
                return base.CreateLoadShardMapManagerGlobalOperation(shardMapManager, operationName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateLoadShardMapManagerGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateLockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory)
        /// </summary>
        public override IStoreOperationGlobal CreateLockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory)
        {
            Func<ShardMapManager, string, IStoreShardMap, IStoreMapping, Guid, LockOwnerIdOpType, ShardManagementErrorCategory, IStoreOperationGlobal> func1 = this.CreateLockOrUnLockMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingGuidLockOwnerIdOpTypeShardManagementErrorCategory;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMap, mapping, lockOwnerId, lockOpType, errorCategory);
            if (this.___callBase)
                return base.CreateLockOrUnLockMappingsGlobalOperation(shardMapManager, operationName, shardMap, mapping, lockOwnerId, lockOpType, errorCategory);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateLockOrUnLockMappingsGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateRemoveMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId)
        /// </summary>
        public override IStoreOperation CreateRemoveMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId)
        {
            Func<ShardMapManager, StoreOperationCode, IStoreShardMap, IStoreMapping, Guid, IStoreOperation> func1 = this.CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid;
            if (func1 != null)
                return func1(shardMapManager, operationCode, shardMap, mapping, lockOwnerId);
            if (this.___callBase)
                return base.CreateRemoveMappingOperation(shardMapManager, operationCode, shardMap, mapping, lockOwnerId);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateRemoveMappingOperation");
        }

        public override IStoreOperation CreateRemoveMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionRemoves)
        {
            Func<StoreOperationCode, ShardMapManager, Guid, StoreOperationState, XElement, Guid, IStoreOperation> func1 = this.CreateRemoveMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
            if (func1 != null)
                return func1(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionRemoves);
            if (this.___callBase)
                return base.CreateRemoveMappingOperation(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionRemoves);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateRemoveMappingOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateRemoveShardMapGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap)
        /// </summary>
        public override IStoreOperationGlobal CreateRemoveShardMapGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap)
        {
            Func<ShardMapManager, string, IStoreShardMap, IStoreOperationGlobal> func1 = this.CreateRemoveShardMapGlobalOperationShardMapManagerStringIStoreShardMap;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMap);
            if (this.___callBase)
                return base.CreateRemoveShardMapGlobalOperation(shardMapManager, operationName, shardMap);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateRemoveShardMapGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateRemoveShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
        /// </summary>
        public override IStoreOperation CreateRemoveShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
        {
            Func<ShardMapManager, IStoreShardMap, IStoreShard, IStoreOperation> func1 = this.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard;
            if (func1 != null)
                return func1(shardMapManager, shardMap, shard);
            if (this.___callBase)
                return base.CreateRemoveShardOperation(shardMapManager, shardMap, shard);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateRemoveShardOperation");
        }

        public override IStoreOperation CreateRemoveShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
        {
            Func<ShardMapManager, Guid, StoreOperationState, XElement, IStoreOperation> func1 = this.CreateRemoveShardOperationShardMapManagerGuidStoreOperationStateXElement;
            if (func1 != null)
                return func1(shardMapManager, operationId, undoStartState, root);
            if (this.___callBase)
                return base.CreateRemoveShardOperation(shardMapManager, operationId, undoStartState, root);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateRemoveShardOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateRemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, String schemaInfoName)
        /// </summary>
        public override IStoreOperationGlobal CreateRemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, string schemaInfoName)
        {
            Func<ShardMapManager, string, string, IStoreOperationGlobal> func1 = this.CreateRemoveShardingSchemaInfoGlobalOperationShardMapManagerStringString;
            if (func1 != null)
                return func1(shardMapManager, operationName, schemaInfoName);
            if (this.___callBase)
                return base.CreateRemoveShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfoName);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateRemoveShardingSchemaInfoGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable`1&lt;IStoreMapping&gt; mappingsToRemove, IEnumerable`1&lt;IStoreMapping&gt; mappingsToAdd)
        /// </summary>
        public override IStoreOperationGlobal CreateReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable<IStoreMapping> mappingsToRemove, IEnumerable<IStoreMapping> mappingsToAdd)
        {
            Func<ShardMapManager, string, IStoreShardMap, IStoreShard, IEnumerable<IStoreMapping>, IEnumerable<IStoreMapping>, IStoreOperationGlobal> func1 = this.CreateReplaceMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardIEnumerableOfIStoreMappingIEnumerableOfIStoreMapping;
            if (func1 != null)
                return func1(shardMapManager, operationName, shardMap, shard, mappingsToRemove, mappingsToAdd);
            if (this.___callBase)
                return base.CreateReplaceMappingsGlobalOperation(shardMapManager, operationName, shardMap, shard, mappingsToRemove, mappingsToAdd);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateReplaceMappingsGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable`1&lt;ShardRange&gt; rangesToRemove, IEnumerable`1&lt;IStoreMapping&gt; mappingsToAdd)
        /// </summary>
        public override IStoreOperationLocal CreateReplaceMappingsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable<ShardRange> rangesToRemove, IEnumerable<IStoreMapping> mappingsToAdd)
        {
            Func<ShardMapManager, ShardLocation, string, IStoreShardMap, IStoreShard, IEnumerable<ShardRange>, IEnumerable<IStoreMapping>, IStoreOperationLocal> func1 = this.CreateReplaceMappingsLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardIEnumerableOfShardRangeIEnumerableOfIStoreMapping;
            if (func1 != null)
                return func1(shardMapManager, location, operationName, shardMap, shard, rangesToRemove, mappingsToAdd);
            if (this.___callBase)
                return base.CreateReplaceMappingsLocalOperation(shardMapManager, location, operationName, shardMap, shard, rangesToRemove, mappingsToAdd);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateReplaceMappingsLocalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateReplaceMappingsOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, Tuple`2&lt;IStoreMapping,Guid&gt;[] mappingsSource, Tuple`2&lt;IStoreMapping,Guid&gt;[] mappingsTarget)
        /// </summary>
        public override IStoreOperation CreateReplaceMappingsOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, Tuple<IStoreMapping, Guid>[] mappingsSource, Tuple<IStoreMapping, Guid>[] mappingsTarget)
        {
            Func<ShardMapManager, StoreOperationCode, IStoreShardMap, Tuple<IStoreMapping, Guid>[], Tuple<IStoreMapping, Guid>[], IStoreOperation> func1 = this.CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray;
            if (func1 != null)
                return func1(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
            if (this.___callBase)
                return base.CreateReplaceMappingsOperation(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateReplaceMappingsOperation");
        }

        public override IStoreOperation CreateReplaceMappingsOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionAdds)
        {
            Func<StoreOperationCode, ShardMapManager, Guid, StoreOperationState, XElement, Guid, IStoreOperation> func1 = this.CreateReplaceMappingsOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
            if (func1 != null)
                return func1(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionAdds);
            if (this.___callBase)
                return base.CreateReplaceMappingsOperation(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionAdds);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateReplaceMappingsOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateUpdateMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mappingSource, IStoreMapping mappingTarget, String patternForKill, Guid lockOwnerId, bool KillConnection)
        /// </summary>
        public override IStoreOperation CreateUpdateMappingOperation(ShardMapManager shardMapManager, StoreOperationCode operationCode, IStoreShardMap shardMap, IStoreMapping mappingSource, IStoreMapping mappingTarget, string patternForKill, Guid lockOwnerId, bool killConnection)
        {
            Func<ShardMapManager, StoreOperationCode, IStoreShardMap, IStoreMapping, IStoreMapping, string, Guid, bool, IStoreOperation> func1 = this.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool;
            if (func1 != null)
                return func1(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill, lockOwnerId, killConnection);
            if (this.___callBase)
                return base.CreateUpdateMappingOperation(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget, patternForKill, lockOwnerId, killConnection);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateUpdateMappingOperation");
        }

        public override IStoreOperation CreateUpdateMappingOperation(StoreOperationCode operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root, Guid originalShardVersionRemoves, Guid originalShardVersionAdds)
        {
            Func<StoreOperationCode, ShardMapManager, Guid, StoreOperationState, XElement, Guid, Guid, IStoreOperation> func1 = this.CreateUpdateMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuidGuid;
            if (func1 != null)
                return func1(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionRemoves, originalShardVersionAdds);
            if (this.___callBase)
                return base.CreateUpdateMappingOperation(operationCode, shardMapManager, operationId, undoStartState, root, originalShardVersionRemoves, originalShardVersionAdds);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateUpdateMappingOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateUpdateShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew)
        /// </summary>
        public override IStoreOperation CreateUpdateShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew)
        {
            Func<ShardMapManager, IStoreShardMap, IStoreShard, IStoreShard, IStoreOperation> func1 = this.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard;
            if (func1 != null)
                return func1(shardMapManager, shardMap, shardOld, shardNew);
            if (this.___callBase)
                return base.CreateUpdateShardOperation(shardMapManager, shardMap, shardOld, shardNew);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateUpdateShardOperation");
        }

        public override IStoreOperation CreateUpdateShardOperation(ShardMapManager shardMapManager, Guid operationId, StoreOperationState undoStartState, XElement root)
        {
            Func<ShardMapManager, Guid, StoreOperationState, XElement, IStoreOperation> func1 = this.CreateUpdateShardOperationShardMapManagerGuidStoreOperationStateXElement;
            if (func1 != null)
                return func1(shardMapManager, operationId, undoStartState, root);
            if (this.___callBase)
                return base.CreateUpdateShardOperation(shardMapManager, operationId, undoStartState, root);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "CreateUpdateShardOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateUpdateShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, String operationName, IStoreSchemaInfo schemaInfo)
        /// </summary>
        public override IStoreOperationGlobal CreateUpdateShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreSchemaInfo schemaInfo)
        {
            Func<ShardMapManager, string, IStoreSchemaInfo, IStoreOperationGlobal> func1 = this.CreateUpdateShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo;
            if (func1 != null)
                return func1(shardMapManager, operationName, schemaInfo);
            if (this.___callBase)
                return base.CreateUpdateShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateUpdateShardingSchemaInfoGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateUpgradeStoreGlobalOperation(ShardMapManager shardMapManager, String operationName, Version targetVersion)
        /// </summary>
        public override IStoreOperationGlobal CreateUpgradeStoreGlobalOperation(ShardMapManager shardMapManager, string operationName, Version targetVersion)
        {
            Func<ShardMapManager, string, Version, IStoreOperationGlobal> func1 = this.CreateUpgradeStoreGlobalOperationShardMapManagerStringVersion;
            if (func1 != null)
                return func1(shardMapManager, operationName, targetVersion);
            if (this.___callBase)
                return base.CreateUpgradeStoreGlobalOperation(shardMapManager, operationName, targetVersion);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationGlobal>(this, "CreateUpgradeStoreGlobalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.CreateUpgradeStoreLocalOperation(ShardMapManager shardMapManager, ShardLocation location, String operationName, Version targetVersion)
        /// </summary>
        public override IStoreOperationLocal CreateUpgradeStoreLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName, Version targetVersion)
        {
            Func<ShardMapManager, ShardLocation, string, Version, IStoreOperationLocal> func1 = this.CreateUpgradeStoreLocalOperationShardMapManagerShardLocationStringVersion;
            if (func1 != null)
                return func1(shardMapManager, location, operationName, targetVersion);
            if (this.___callBase)
                return base.CreateUpgradeStoreLocalOperation(shardMapManager, location, operationName, targetVersion);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperationLocal>(this, "CreateUpgradeStoreLocalOperation");
        }

        /// <summary>
        /// Sets the stub of StoreOperationFactory.FromLogEntry(ShardMapManager shardMapManager, IStoreLogEntry so)
        /// </summary>
        public override IStoreOperation FromLogEntry(ShardMapManager shardMapManager, IStoreLogEntry so)
        {
            Func<ShardMapManager, IStoreLogEntry, IStoreOperation> func1 = this.FromLogEntryShardMapManagerIStoreLogEntry;
            if (func1 != null)
                return func1(shardMapManager, so);
            if (this.___callBase)
                return base.FromLogEntry(shardMapManager, so);
            return this.InstanceBehavior.Result<StubStoreOperationFactory, IStoreOperation>(this, "FromLogEntry");
        }

        /// <summary>
        /// Initializes a new instance of type StubStoreOperationFactory
        /// </summary>
        private void InitializeStub()
        {
        }
    }
}
