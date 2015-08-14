// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Factory for storage operation creation.
    /// </summary>
    internal interface IStoreOperationFactory
    {
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
        IStoreOperationGlobal CreateCreateShardMapManagerGlobalOperation(
            SqlShardMapManagerCredentials credentials,
            TransientFaultHandling.RetryPolicy retryPolicy,
            string operationName,
            ShardMapManagerCreateMode createMode,
            Version targetVersion);

        /// <summary>
        /// Constructs request for obtaining shard map manager object if the GSM has the SMM objects in it.
        /// </summary>
        /// <param name="credentials">Credentials for connection.</param>
        /// <param name="retryPolicy">Retry policy.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="throwOnFailure">Whether to throw exception on failure or return error code.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateGetShardMapManagerGlobalOperation(
            SqlShardMapManagerCredentials credentials,
            TransientFaultHandling.RetryPolicy retryPolicy,
            string operationName,
            bool throwOnFailure);

        /// <summary>
        /// Constructs request for Detaching the given shard and mapping information to the GSM database.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="location">Location to be detached.</param>
        /// <param name="shardMapName">Shard map from which shard is being detached.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateDetachShardGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            ShardLocation location,
            string shardMapName);

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
        IStoreOperationGlobal CreateReplaceMappingsGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            IEnumerable<IStoreMapping> mappingsToRemove,
            IEnumerable<IStoreMapping> mappingsToAdd);

        /// <summary>
        /// Constructs a request to add schema info to GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfo">Schema info to add.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateAddShardingSchemaInfoGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreSchemaInfo schemaInfo);

        /// <summary>
        /// Constructs a request to find schema info in GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfoName">Name of schema info to search.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateFindShardingSchemaInfoGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            string schemaInfoName);

        /// <summary>
        /// Constructs a request to get all schema info objects from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateGetShardingSchemaInfosGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName);

        /// <summary>
        /// Constructs a request to delete schema info from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfoName">Name of schema info to delete.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateRemoveShardingSchemaInfoGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            string schemaInfoName);

        /// <summary>
        /// Constructs a request to update schema info to GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfo">Schema info to update.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateUpdateShardingSchemaInfoGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreSchemaInfo schemaInfo);

        /// <summary>
        /// Constructs request to get shard with specific location for given shard map from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map for which shard is being requested.</param>
        /// <param name="location">Location of shard being searched.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateFindShardByLocationGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            ShardLocation location);

        /// <summary>
        /// Constructs request to get all shards for given shard map from GSM.
        /// </summary>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which shards are being requested.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateGetShardsGlobalOperation(
            string operationName,
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap);

        /// <summary>
        /// Constructs request to add given shard map to GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map to add.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateAddShardMapGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap);

        /// <summary>
        /// Constructs request to find shard map with given name from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapName">Name of the shard map being searched.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateFindShardMapByNameGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            string shardMapName);

        /// <summary>
        /// Constructs request to get distinct shard locations from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateGetDistinctShardLocationsGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName);

        /// <summary>
        /// Constructs request to get all shard maps from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateGetShardMapsGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName);

        /// <summary>
        /// Constructs request to get all shard maps from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateLoadShardMapManagerGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName);

        /// <summary>
        /// Constructs request to remove given shard map from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map to remove.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateRemoveShardMapGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap);

        /// <summary>
        /// Constructs request for obtaining mapping from GSM based on given key.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation being executed.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="mapping">Mapping whose Id will be used.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateFindMappingByIdGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            ShardManagementErrorCategory errorCategory);

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
        IStoreOperationGlobal CreateFindMappingByKeyGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            ShardKey key,
            CacheStoreMappingUpdatePolicy policy,
            ShardManagementErrorCategory errorCategory,
            bool cacheResults,
            bool ignoreFailure);

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
        IStoreOperationGlobal CreateGetMappingsByRangeGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardRange range,
            ShardManagementErrorCategory errorCategory,
            bool cacheResults,
            bool ignoreFailure);

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
        IStoreOperationGlobal CreateLockOrUnLockMappingsGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockOwnerId,
            LockOwnerIdOpType lockOpType,
            ShardManagementErrorCategory errorCategory);

        /// <summary>
        /// Constructs a request to upgrade global store.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="targetVersion">Target version of store to deploy.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationGlobal CreateUpgradeStoreGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            Version targetVersion);

        #endregion Global Operations

        #region Local Operations

        /// <summary>
        /// Constructs request for obtaining all the shard maps and shards from an LSM.
        /// </summary>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        IStoreOperationLocal CreateCheckShardLocalOperation(
            string operationName,
            ShardMapManager shardMapManager,
            ShardLocation location);

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
        IStoreOperationLocal CreateGetMappingsByRangeLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardRange range,
            bool ignoreFailure);

        /// <summary>
        /// Constructs request for obtaining all the shard maps and shards from an LSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        /// <param name="operationName">Operatio name.</param>
        IStoreOperationLocal CreateGetShardsLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName);

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
        IStoreOperationLocal CreateReplaceMappingsLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            IEnumerable<ShardRange> rangesToRemove,
            IEnumerable<IStoreMapping> mappingsToAdd);

        /// <summary>
        /// Constructs a request to upgrade store location.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="location">Location to upgrade.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="targetVersion">Target version of store to deploy.</param>
        /// <returns>The store operation.</returns>
        IStoreOperationLocal CreateUpgradeStoreLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            Version targetVersion);

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
        IStoreOperation CreateAddShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard);

        /// <summary>
        /// Creates request to remove shard from given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which to remove shard.</param>
        /// <param name="shard">Shard to remove.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateRemoveShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard);

        /// <summary>
        /// Creates request to update shard in given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which to remove shard.</param>
        /// <param name="shardOld">Shard to update.</param>
        /// <param name="shardNew">Updated shard.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateUpdateShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shardOld,
            IStoreShard shardNew);

        /// <summary>
        /// Constructs request for attaching the given shard map and shard information to the GSM database.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shard">Shard to attach</param>
        /// <param name="shardMap">Shard Map to attach shard to.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateAttachShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard);

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to add mapping.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateAddMappingOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping);

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map from which to remove mapping.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <param name="lockOwnerId">Id of lock owner.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateRemoveMappingOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockOwnerId);

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
        IStoreOperation CreateUpdateMappingOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget,
            string patternForKill,
            Guid lockOwnerId);

        /// <summary>
        /// Creates request to replace mappings within shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to update mapping.</param>
        /// <param name="mappingsSource">Original mappings.</param>
        /// <param name="mappingsTarget">Target mappings mapping.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateReplaceMappingsOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            Tuple<IStoreMapping, Guid>[] mappingsSource,
            Tuple<IStoreMapping, Guid>[] mappingsTarget);

        #endregion Do Operations

        #region Undo Operations

        /// <summary>
        /// Create operation corresponding to the <see cref="IStoreLogEntry"/> information.
        /// </summary>
        /// <param name="shardMapManager">ShardMapManager instance for undo operation.</param>
        /// <param name="so">Store operation information.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation FromLogEntry(ShardMapManager shardMapManager, IStoreLogEntry so);

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateAddShardOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root);

        /// <summary>
        /// Creates request to remove shard from given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateRemoveShardOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root);

        /// <summary>
        /// Creates request to update shard in given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateUpdateShardOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root);

        /// <summary>
        /// Creates request to add a mapping in given shard map.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <param name="shardIdOriginal">Original shard Id.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateAddMappingOperation(
            StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root,
            Guid shardIdOriginal);

        /// <summary>
        /// Creates request to remove a mapping in given shard map.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <param name="shardIdOriginal">Original shard Id.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateRemoveMappingOperation(
            StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root,
            Guid shardIdOriginal);

        /// <summary>
        /// Creates request to update a mapping in given shard map.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <param name="shardIdOriginalSource">Original shard Id of source.</param>
        /// <param name="shardIdOriginalTarget">Original shard Id of target.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateUpdateMappingOperation(
            StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root,
            Guid shardIdOriginalSource,
            Guid shardIdOriginalTarget);

        /// <summary>
        /// Creates request to replace a set of mappings with new set in given shard map.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Undo start state.</param>
        /// <param name="root">Xml representation of the object.</param>
        /// <param name="shardIdOriginalSource">Original shard Id of source.</param>
        /// <returns>The store operation.</returns>
        IStoreOperation CreateReplaceMappingsOperation(
            StoreOperationCode operationCode,
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            XElement root,
            Guid shardIdOriginalSource);

        #endregion Undo Operations

        #endregion Global And Local Operations
    }
}
