// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Instantiates the operations that need to be undone corresponding to a request.
    /// </summary>
    internal class StoreOperationFactory : IStoreOperationFactory
    {
        /// <summary>
        /// Create instance of StoreOperationFactory.
        /// </summary>
        protected internal StoreOperationFactory()
        {
        }

        #region Global Operations

        /// <summary>
        /// Constructs request for deploying SMM storage objects to target GSM database.
        /// </summary>
        /// <param name="credentials">Credentials for connection.</param>
        /// <param name="retryPolicy">Retry policy.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="createMode">Creation mode.</param>
        /// <param name="targetVersion">target version of store to deploy, this will be used mainly for upgrade testing purposes.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateCreateShardMapManagerGlobalOperation(
            SqlShardMapManagerCredentials credentials,
            TransientFaultHandling.RetryPolicy retryPolicy,
            string operationName,
            ShardMapManagerCreateMode createMode,
            Version targetVersion)
        {
            return new CreateShardMapManagerGlobalOperation(
                credentials,
                retryPolicy,
                operationName,
                createMode,
                targetVersion);
        }

        /// <summary>
        /// Constructs request for obtaining shard map manager object if the GSM has the SMM objects in it.
        /// </summary>
        /// <param name="credentials">Credentials for connection.</param>
        /// <param name="retryPolicy">Retry policy.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="throwOnFailure">Whether to throw exception on failure or return error code.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateGetShardMapManagerGlobalOperation(
            SqlShardMapManagerCredentials credentials,
            TransientFaultHandling.RetryPolicy retryPolicy,
            string operationName,
            bool throwOnFailure)
        {
            return new GetShardMapManagerGlobalOperation(
                credentials,
                retryPolicy,
                operationName,
                throwOnFailure);
        }

        /// <summary>
        /// Constructs request for Detaching the given shard and mapping information to the GSM database.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="location">Location to be detached.</param>
        /// <param name="shardMapName">Shard map from which shard is being detached.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateDetachShardGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            ShardLocation location,
            string shardMapName)
        {
            return new DetachShardGlobalOperation(
                shardMapManager,
                operationName,
                location,
                shardMapName);
        }

        /// <summary>
        /// Constructs request for replacing the GSM mappings for given shard map with the input mappings.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMap">GSM Shard map.</param>
        /// <param name="shard">GSM Shard.</param>
        /// <param name="mappingsToRemove">Optional list of mappings to remove.</param>
        /// <param name="mappingsToAdd">List of mappings to add.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateReplaceMappingsGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            IEnumerable<IStoreMapping> mappingsToRemove,
            IEnumerable<IStoreMapping> mappingsToAdd)
        {
            return new ReplaceMappingsGlobalOperation(
                shardMapManager,
                operationName,
                shardMap,
                shard,
                mappingsToRemove,
                mappingsToAdd);
        }

        /// <summary>
        /// Constructs a request to add schema info to GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfo">Schema info to add.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateAddShardingSchemaInfoGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreSchemaInfo schemaInfo)
        {
            return new AddShardingSchemaInfoGlobalOperation(
                shardMapManager,
                operationName,
                schemaInfo);
        }

        /// <summary>
        /// Constructs a request to find schema info in GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfoName">Name of schema info to search.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateFindShardingSchemaInfoGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            string schemaInfoName)
        {
            return new FindShardingSchemaInfoGlobalOperation(
                shardMapManager,
                operationName,
                schemaInfoName);
        }

        /// <summary>
        /// Constructs a request to get all schema info objects from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateGetShardingSchemaInfosGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName)
        {
            return new GetShardingSchemaInfosGlobalOperation(
                shardMapManager,
                operationName);
        }

        /// <summary>
        /// Constructs a request to delete schema info from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfoName">Name of schema info to delete.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateRemoveShardingSchemaInfoGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            string schemaInfoName)
        {
            return new RemoveShardingSchemaInfoGlobalOperation(
                shardMapManager,
                operationName,
                schemaInfoName);
        }

        /// <summary>
        /// Constructs a request to update schema info to GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfo">Schema info to update.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateUpdateShardingSchemaInfoGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreSchemaInfo schemaInfo)
        {
            return new UpdateShardingSchemaInfoGlobalOperation(
                shardMapManager,
                operationName,
                schemaInfo);
        }

        /// <summary>
        /// Constructs request to get shard with specific location for given shard map from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map for which shard is being requested.</param>
        /// <param name="location">Location of shard being searched.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateFindShardByLocationGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            ShardLocation location)
        {
            return new FindShardByLocationGlobalOperation(
                shardMapManager,
                operationName,
                shardMap,
                location);
        }

        /// <summary>
        /// Constructs request to get all shards for given shard map from GSM.
        /// </summary>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which shards are being requested.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateGetShardsGlobalOperation(
            string operationName,
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap)
        {
            return new GetShardsGlobalOperation(
                operationName,
                shardMapManager,
                shardMap);
        }

        /// <summary>
        /// Constructs request to add given shard map to GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map to add.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateAddShardMapGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap)
        {
            return new AddShardMapGlobalOperation(
                shardMapManager,
                operationName,
                shardMap);
        }

        /// <summary>
        /// Constructs request to find shard map with given name from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapName">Name of the shard map being searched.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateFindShardMapByNameGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            string shardMapName)
        {
            return new FindShardMapByNameGlobalOperation(
                shardMapManager,
                operationName,
                shardMapName);
        }

        /// <summary>
        /// Constructs request to get distinct shard locations from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateGetDistinctShardLocationsGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName)
        {
            return new GetDistinctShardLocationsGlobalOperation(
                shardMapManager,
                operationName);
        }

        /// <summary>
        /// Constructs request to get all shard maps from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateGetShardMapsGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName)
        {
            return new GetShardMapsGlobalOperation(
                shardMapManager,
                operationName);
        }

        /// <summary>
        /// Constructs request to get all shard maps from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateLoadShardMapManagerGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName)
        {
            return new LoadShardMapManagerGlobalOperation(
                shardMapManager,
                operationName);
        }

        /// <summary>
        /// Constructs request to remove given shard map from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map to remove.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateRemoveShardMapGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap)
        {
            return new RemoveShardMapGlobalOperation(
                shardMapManager,
                operationName,
                shardMap);
        }

        /// <summary>
        /// Constructs request for obtaining mapping from GSM based on given key.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation being executed.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="mapping">Mapping whose Id will be used.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateFindMappingByIdGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            ShardManagementErrorCategory errorCategory)
        {
            return new FindMappingByIdGlobalOperation(
                shardMapManager,
                operationName,
                shardMap,
                mapping,
                errorCategory);
        }

        /// <summary>
        /// Constructs request for obtaining mapping from GSM based on given key.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation being executed.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="key">Key for lookup operation.</param>
        /// <param name="policy">Policy for cache update.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <param name="cacheResults">Whether to cache the results of the operation.</param>
        /// <param name="ignoreFailure">Ignore shard map not found error.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateFindMappingByKeyGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            ShardKey key,
            CacheStoreMappingUpdatePolicy policy,
            ShardManagementErrorCategory errorCategory,
            bool cacheResults,
            bool ignoreFailure)
        {
            return new FindMappingByKeyGlobalOperation(
                shardMapManager,
                operationName,
                shardMap,
                key,
                policy,
                errorCategory,
                cacheResults,
                ignoreFailure);
        }

        /// <summary>
        /// Constructs request for obtaining all the mappings from GSM based on given shard and mappings.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation being executed.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="shard">Local shard.</param>
        /// <param name="range">Optional range to get mappings from.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <param name="cacheResults">Whether to cache the results of the operation.</param>
        /// <param name="ignoreFailure">Ignore shard map not found error.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateGetMappingsByRangeGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardRange range,
            ShardManagementErrorCategory errorCategory,
            bool cacheResults,
            bool ignoreFailure)
        {
            return new GetMappingsByRangeGlobalOperation(
                shardMapManager,
                operationName,
                shardMap,
                shard,
                range,
                errorCategory,
                cacheResults,
                ignoreFailure);
        }

        /// <summary>
        /// Constructs request to lock or unlock given mappings in GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map to add.</param>
        /// <param name="mapping">Mapping to lock or unlock. Null means all mappings.</param>
        /// <param name="lockOwnerId">Lock owner.</param>
        /// <param name="lockOpType">Lock operation type.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateLockOrUnLockMappingsGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockOwnerId,
            LockOwnerIdOpType lockOpType,
            ShardManagementErrorCategory errorCategory)
        {
            return new LockOrUnLockMappingsGlobalOperation(
                shardMapManager,
                operationName,
                shardMap,
                mapping,
                lockOwnerId,
                lockOpType,
                errorCategory);
        }

        /// <summary>
        /// Constructs a request to upgrade global store.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="targetVersion">Target version of store to deploy.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationGlobal CreateUpgradeStoreGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            Version targetVersion)
        {
            return new UpgradeStoreGlobalOperation(
                shardMapManager,
                operationName,
                targetVersion);
        }

        #endregion Global Operations

        #region Local Operations

        /// <summary>
        /// Constructs request for obtaining all the shard maps and shards from an LSM.
        /// </summary>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        public virtual IStoreOperationLocal CreateCheckShardLocalOperation(
            string operationName,
            ShardMapManager shardMapManager,
            ShardLocation location)
        {
            return new CheckShardLocalOperation(
                operationName,
                shardMapManager,
                location);
        }

        /// <summary>
        /// Constructs request for obtaining all the shard maps and shards from an LSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="shard">Local shard.</param>
        /// <param name="range">Optional range to get mappings from.</param>
        /// <param name="ignoreFailure">Ignore shard map not found error.</param>
        public virtual IStoreOperationLocal CreateGetMappingsByRangeLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardRange range,
            bool ignoreFailure)
        {
            return new GetMappingsByRangeLocalOperation(
                shardMapManager,
                location,
                operationName,
                shardMap,
                shard,
                range,
                ignoreFailure);
        }

        /// <summary>
        /// Constructs request for obtaining all the shard maps and shards from an LSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        /// <param name="operationName">Operatio name.</param>
        public virtual IStoreOperationLocal CreateGetShardsLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName)
        {
            return new GetShardsLocalOperation(
                shardMapManager,
                location,
                operationName);
        }

        /// <summary>
        /// Constructs request for replacing the LSM mappings for given shard map with the input mappings.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="shard">Local shard.</param>
        /// <param name="rangesToRemove">Optional list of ranges to minimize amount of deletions.</param>
        /// <param name="mappingsToAdd">List of mappings to add.</param>
        public virtual IStoreOperationLocal CreateReplaceMappingsLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            IEnumerable<ShardRange> rangesToRemove,
            IEnumerable<IStoreMapping> mappingsToAdd)
        {
            return new ReplaceMappingsLocalOperation(
                shardMapManager,
                location,
                operationName,
                shardMap,
                shard,
                rangesToRemove,
                mappingsToAdd);
        }

        /// <summary>
        /// Constructs a request to upgrade store location.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="location">Location to upgrade.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="targetVersion">Target version of store to deploy.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperationLocal CreateUpgradeStoreLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            Version targetVersion)
        {
            return new UpgradeStoreLocalOperation(
                shardMapManager,
                location,
                operationName,
                targetVersion);
        }

        #endregion LocalOperations

        #region Global And Local Operations

        #region Do Operations

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which to add shard.</param>
        /// <param name="shard">Shard to add.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateAddShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
        {
            return new AddShardOperation(shardMapManager, shardMap, shard);
        }

        /// <summary>
        /// Creates request to remove shard from given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which to remove shard.</param>
        /// <param name="shard">Shard to remove.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateRemoveShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
        {
            return new RemoveShardOperation(shardMapManager, shardMap, shard);
        }

        /// <summary>
        /// Creates request to update shard in given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which to remove shard.</param>
        /// <param name="shardOld">Shard to update.</param>
        /// <param name="shardNew">Updated shard.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateUpdateShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shardOld,
            IStoreShard shardNew)
        {
            return new UpdateShardOperation(shardMapManager, shardMap, shardOld, shardNew);
        }

        /// <summary>
        /// Constructs request for attaching the given shard map and shard information to the GSM database.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shard">Shard to attach</param>
        /// <param name="shardMap">Shard map to attach specified shard</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateAttachShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
        {
            return new AttachShardOperation(shardMapManager, shardMap, shard);
        }

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to add mapping.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateAddMappingOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping)
        {
            return new AddMappingOperation(
                shardMapManager,
                operationCode,
                shardMap,
                mapping);
        }

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map from which to remove mapping.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <param name="lockOwnerId">Id of lock owner.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateRemoveMappingOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockOwnerId)
        {
            return new RemoveMappingOperation(
                shardMapManager,
                operationCode,
                shardMap,
                mapping,
                lockOwnerId);
        }

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to update mapping.</param>
        /// <param name="mappingSource">Mapping to update.</param>
        /// <param name="mappingTarget">Updated mapping.</param>
        /// <param name="patternForKill">Pattern for kill commands.</param>
        /// <param name="lockOwnerId">Id of lock owner.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateUpdateMappingOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget,
            string patternForKill,
            Guid lockOwnerId)
        {
            return new UpdateMappingOperation(
                shardMapManager,
                operationCode,
                shardMap,
                mappingSource,
                mappingTarget,
                patternForKill,
                lockOwnerId);
        }

        /// <summary>
        /// Creates request to replace mappings within shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to update mapping.</param>
        /// <param name="mappingsSource">Original mappings.</param>
        /// <param name="mappingsTarget">Target mappings mapping.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateReplaceMappingsOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            Tuple<IStoreMapping, Guid>[] mappingsSource,
            Tuple<IStoreMapping, Guid>[] mappingsTarget)
        {
            return new ReplaceMappingsOperation(
                shardMapManager,
                operationCode,
                shardMap,
                mappingsSource,
                mappingsTarget);
        }

        #endregion Do Operations

        #region Undo Operations

        /// <summary>
        /// Create operation corresponding to the <see cref="IStoreLogEntry"/> information.
        /// </summary>
        /// <param name="shardMapManager">ShardMapManager instance for undo operation.</param>
        /// <param name="so">Store operation information.</param>
        /// <returns>The operation to be undone.</returns>
        public virtual IStoreOperation FromLogEntry(ShardMapManager shardMapManager, IStoreLogEntry so)
        {
            XElement root;

            using (XmlReader reader = so.Data.CreateReader())
            {
                root = XElement.Load(reader);
            }

            switch (so.OpCode)
            {
                case StoreOperationCode.AddShard:
                    return this.CreateAddShardOperation(
                        shardMapManager,
                        so.Id,
                        so.UndoStartState,
                        root);

                case StoreOperationCode.RemoveShard:
                    return this.CreateRemoveShardOperation(
                        shardMapManager,
                        so.Id,
                        so.UndoStartState,
                        root);

                case StoreOperationCode.UpdateShard:
                    return this.CreateUpdateShardOperation(
                        shardMapManager,
                        so.Id,
                        so.UndoStartState,
                        root);

                case StoreOperationCode.AddPointMapping:
                case StoreOperationCode.AddRangeMapping:
                    return this.CreateAddMappingOperation(
                        so.OpCode,
                        shardMapManager,
                        so.Id,
                        so.UndoStartState,
                        root,
                        so.OriginalShardVersionAdds);

                case StoreOperationCode.RemovePointMapping:
                case StoreOperationCode.RemoveRangeMapping:
                    return this.CreateRemoveMappingOperation(
                        so.OpCode,
                        shardMapManager,
                        so.Id,
                        so.UndoStartState,
                        root,
                        so.OriginalShardVersionRemoves);

                case StoreOperationCode.UpdatePointMapping:
                case StoreOperationCode.UpdateRangeMapping:
                case StoreOperationCode.UpdatePointMappingWithOffline:
                case StoreOperationCode.UpdateRangeMappingWithOffline:
                    return this.CreateUpdateMappingOperation(
                        so.OpCode,
                        shardMapManager,
                        so.Id,
                        so.UndoStartState,
                        root,
                        so.OriginalShardVersionRemoves,
                        so.OriginalShardVersionAdds);

                case StoreOperationCode.SplitMapping:
                case StoreOperationCode.MergeMappings:
                    return this.CreateReplaceMappingsOperation(
                        so.OpCode,
                        shardMapManager,
                        so.Id,
                        so.UndoStartState,
                        root,
                        so.OriginalShardVersionAdds);

                default:
                    Debug.Fail("Unexpected operation code.");
                    return null;
            }
        }

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateAddShardOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root)
        {
            return new AddShardOperation(
                shardMapManager,
                operationId,
                undoStartState,
                StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")),
                StoreObjectFormatterXml.ReadIStoreShard(root.Element("Steps").Element("Step").Element("Shard")));
        }

        /// <summary>
        /// Creates request to remove shard from given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateRemoveShardOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root)
        {
            return new RemoveShardOperation(
                shardMapManager,
                operationId,
                undoStartState,
                StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")),
                StoreObjectFormatterXml.ReadIStoreShard(root.Element("Steps").Element("Step").Element("Shard")));
        }

        /// <summary>
        /// Creates request to update shard in given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateUpdateShardOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root)
        {
            return new UpdateShardOperation(
                shardMapManager,
                operationId,
                undoStartState,
                StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")),
                StoreObjectFormatterXml.ReadIStoreShard(root.Element("Steps").Element("Step").Element("Shard")),
                StoreObjectFormatterXml.ReadIStoreShard(root.Element("Steps").Element("Step").Element("Update").Element("Shard")));
        }

        /// <summary>
        /// Creates request to add a mapping in given shard map.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <param name="originalShardVersionAdds">Original shard version.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateAddMappingOperation(
            StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root,
            Guid originalShardVersionAdds)
        {
            return new AddMappingOperation(
                shardMapManager,
                operationId,
                undoStartState,
                operationCode,
                StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")),
                StoreObjectFormatterXml.ReadIStoreMapping(
                    root.Element("Steps").Element("Step").Element("Mapping"),
                    root.Element("Adds").Element("Shard")),
                originalShardVersionAdds);
        }

        /// <summary>
        /// Creates request to remove a mapping in given shard map.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <param name="originalShardVersionRemoves">Original shard version for Removes.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateRemoveMappingOperation(
            StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root,
            Guid originalShardVersionRemoves)
        {
            return new RemoveMappingOperation(
                shardMapManager,
                operationId,
                undoStartState,
                operationCode,
                StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")),
                StoreObjectFormatterXml.ReadIStoreMapping(
                    root.Element("Steps").Element("Step").Element("Mapping"),
                    root.Element("Removes").Element("Shard")),
                StoreObjectFormatterXml.ReadLock(root.Element("Steps").Element("Step").Element("Lock")),
                originalShardVersionRemoves);
        }

        /// <summary>
        /// Creates request to update a mapping in given shard map.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <param name="originalShardVersionRemoves">Original shard version for removes.</param>
        /// <param name="originalShardVersionAdds">Original shard version for adds.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateUpdateMappingOperation(
            StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root,
            Guid originalShardVersionRemoves,
            Guid originalShardVersionAdds)
        {
            return new UpdateMappingOperation(
                shardMapManager,
                operationId,
                undoStartState,
                operationCode,
                StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")),
                StoreObjectFormatterXml.ReadIStoreMapping(
                    root.Element("Steps").Element("Step").Element("Mapping"),
                    root.Element("Removes").Element("Shard")),
                StoreObjectFormatterXml.ReadIStoreMapping(
                    root.Element("Steps").Element("Step").Element("Update").Element("Mapping"),
                    root.Element("Adds").Element("Shard")),
                root.Element("PatternForKill").Value,
                StoreObjectFormatterXml.ReadLock(root.Element("Steps").Element("Step").Element("Lock")),
                originalShardVersionRemoves,
                originalShardVersionAdds);
        }

        /// <summary>
        /// Creates request to replace a set of mappings with new set in given shard map.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <param name="originalShardVersionAdds">Original shard Id of source.</param>
        /// <returns>The store operation.</returns>
        public virtual IStoreOperation CreateReplaceMappingsOperation(
            StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root,
            Guid originalShardVersionAdds)
        {
            XElement removeShard = root.Element("Removes").Element("Shard");
            XElement addShard = root.Element("Adds").Element("Shard");

            return new ReplaceMappingsOperation(
                shardMapManager,
                operationId,
                undoStartState,
                operationCode,
                StoreObjectFormatterXml.ReadIStoreShardMap(root.Element("ShardMap")),
                root.Element("Steps").Elements("Step").Where(e => e.Attribute("Kind").Value == "1").Select(
                     xe => new
                     {
                         Index = Int32.Parse(xe.Attribute("Id").Value),
                         Mapping = StoreObjectFormatterXml.ReadIStoreMapping(xe.Element("Mapping"), removeShard),
                         Lock = StoreObjectFormatterXml.ReadLock(xe.Element("Lock"))
                     })
                .OrderBy(el => el.Index)
                .Select(el => new Tuple<IStoreMapping, Guid>(el.Mapping, el.Lock))
                .ToArray(),
                root.Element("Steps").Elements("Step").Where(e => e.Attribute("Kind").Value == "3").Select(
                    xe => new
                    {
                        Index = Int32.Parse(xe.Attribute("Id").Value),
                        Mapping = StoreObjectFormatterXml.ReadIStoreMapping(xe.Element("Mapping"), addShard),
                        Lock = StoreObjectFormatterXml.ReadLock(xe.Element("Lock"))
                    })
                .OrderBy(el => el.Index)
                .Select(el => new Tuple<IStoreMapping, Guid>(el.Mapping, el.Lock))
                .ToArray(),
                originalShardVersionAdds);
        }

        #endregion Undo Operations

        #endregion Global And Local Operations
    }
}
