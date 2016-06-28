// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Base class for keyed mappers.
    /// </summary>
    internal abstract class BaseShardMapper
    {
        /// <summary>
        /// Base shard mapper, which is just a holder of some fields.
        /// </summary>
        /// <param name="manager">Reference to ShardMapManager.</param>
        /// <param name="sm">Containing shard map.</param>
        protected BaseShardMapper(ShardMapManager manager, ShardMap sm)
        {
            Debug.Assert(manager != null);
            Debug.Assert(sm != null);

            this.Manager = manager;
            this.ShardMap = sm;
        }

        /// <summary>
        /// Reference to ShardMapManager.
        /// </summary>
        protected ShardMapManager Manager
        {
            get;
            private set;
        }

        /// <summary>
        /// Containing shard map.
        /// </summary>
        protected ShardMap ShardMap
        {
            get;
            private set;
        }

        /// <summary>
        /// The Tracer
        /// </summary>
        private static ILogger Tracer
        {
            get
            {
                return TraceHelper.Tracer;
            }
        }

        /// <summary>
        /// Sets the status of a shardmapping
        /// </summary>
        /// <typeparam name="TMapping">Mapping type.</typeparam>
        /// <typeparam name="TUpdate">Update type.</typeparam>
        /// <typeparam name="TStatus">Status type.</typeparam>
        /// <param name="mapping">Mapping being added.</param>
        /// <param name="status">Status of <paramref name="mapping">mapping</paramref> being added.</param>
        /// <param name="getStatus">Delegate to construct new status from 
        /// <paramref name="status">input status</paramref>.</param>
        /// <param name="createUpdate">Delegate to construct new update from new status returned by 
        /// <paramref name="getStatus">getStatus</paramref>.</param>
        /// <param name="runUpdate">Delegate to perform update from the <paramref name="mapping">input mapping</paramref> and 
        /// the update object returned by <paramref name="getStatus">createUpdate</paramref>.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns></returns>
        protected static TMapping SetStatus<TMapping, TUpdate, TStatus>(
            TMapping mapping,
            TStatus status,
            Func<TStatus, TStatus> getStatus,
            Func<TStatus, TUpdate> createUpdate,
            Func<TMapping, TUpdate, Guid, TMapping> runUpdate,
            Guid lockOwnerId = default(Guid))
        {
            TStatus newStatus = getStatus(status);
            TUpdate update = createUpdate(newStatus);
            return runUpdate(mapping, update, lockOwnerId);
        }

        /// <summary>
        /// Given a key value, obtains a SqlConnection to the shard in the mapping
        /// that contains the key value.
        /// </summary>
        /// <typeparam name="TMapping">Mapping type.</typeparam>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="key">Input key value.</param>
        /// <param name="constructMapping">Delegate to construct a mapping object.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <param name="connectionString">
        /// Connection string with credential information, the DataSource and Database are 
        /// obtained from the results of the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>An opened SqlConnection.</returns>
        protected SqlConnection OpenConnectionForKey<TMapping, TKey>(
            TKey key,
            Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping,
            ShardManagementErrorCategory errorCategory,
            string connectionString,
            ConnectionOptions options = ConnectionOptions.Validate) where TMapping : class, IShardProvider
        {
            ShardKey sk = new ShardKey(ShardKey.ShardKeyTypeFromType(typeof(TKey)), key);

            // Try to find the mapping within the cache.
            ICacheStoreMapping csm = this.Manager.Cache.LookupMappingByKey(this.ShardMap.StoreShardMap, sk);

            IStoreMapping sm;

            if (csm != null)
            {
                sm = csm.Mapping;
            }
            else
            {
                sm = this.LookupMappingForOpenConnectionForKey(
                    sk,
                    CacheStoreMappingUpdatePolicy.OverwriteExisting,
                    errorCategory);
            }

            SqlConnection result;

            try
            {
                // Initially attempt to connect based on lookup results from either cache or GSM.
                result = this.ShardMap.OpenConnection(
                    constructMapping(this.Manager, this.ShardMap, sm),
                    connectionString,
                    options);

                // Reset TTL on successful connection.
                if (csm != null && csm.TimeToLiveMilliseconds > 0)
                {
                    csm.ResetTimeToLive();
                }

                this.Manager.Cache.IncrementPerformanceCounter(this.ShardMap.StoreShardMap, PerformanceCounterName.DdrOperationsPerSec);
                return result;
            }
            catch (ShardManagementException smme)
            {
                // If we hit a validation failure due to stale version of mapping, we will perform one more attempt. 
                if (((options & ConnectionOptions.Validate) == ConnectionOptions.Validate) &&
                    smme.ErrorCategory == ShardManagementErrorCategory.Validation &&
                    smme.ErrorCode == ShardManagementErrorCode.MappingDoesNotExist)
                {
                    // Assumption here is that this time the attempt should succeed since the cache entry 
                    // has already been either evicted, or updated based on latest data from the server.
                    sm = this.LookupMappingForOpenConnectionForKey(
                        sk,
                        CacheStoreMappingUpdatePolicy.OverwriteExisting,
                        errorCategory);

                    result = this.ShardMap.OpenConnection(
                        constructMapping(this.Manager, this.ShardMap, sm),
                        connectionString,
                        options);
                    this.Manager.Cache.IncrementPerformanceCounter(this.ShardMap.StoreShardMap, PerformanceCounterName.DdrOperationsPerSec);
                    return result;
                }
                else
                {
                    // The error was not due to validation but something else e.g.
                    // 1) Shard map does not exist
                    // 2) Mapping could not be found.
                    throw;
                }
            }
            catch (SqlException)
            {
                // We failed to connect. If we were trying to connect from an entry in cache and mapping expired in cache.
                if (csm != null && TimerUtils.ElapsedMillisecondsSince(csm.CreationTime) >= csm.TimeToLiveMilliseconds)
                {
                    using (IdLock _idLock = new IdLock(csm.Mapping.StoreShard.Id))
                    {
                        // Similar to DCL pattern, we need to refresh the mapping again to see if we still need to go to the store
                        // to lookup the mapping after acquiring the shard lock. It might be the case that a fresh version has already
                        // been obtained by some other thread.
                        csm = this.Manager.Cache.LookupMappingByKey(this.ShardMap.StoreShardMap, sk);

                        // Only go to store if the mapping is stale even after refresh.
                        if (csm == null || TimerUtils.ElapsedMillisecondsSince(csm.CreationTime) >= csm.TimeToLiveMilliseconds)
                        {
                            // Refresh the mapping in cache. And try to open the connection after refresh.
                            sm = this.LookupMappingForOpenConnectionForKey(
                                sk,
                                CacheStoreMappingUpdatePolicy.UpdateTimeToLive,
                                errorCategory);
                        }
                        else
                        {
                            sm = csm.Mapping;
                        }
                    }

                    result = this.ShardMap.OpenConnection(
                        constructMapping(this.Manager, this.ShardMap, sm),
                        connectionString,
                        options);

                    // Reset TTL on successful connection.
                    if (csm != null && csm.TimeToLiveMilliseconds > 0)
                    {
                        csm.ResetTimeToLive();
                    }

                    this.Manager.Cache.IncrementPerformanceCounter(this.ShardMap.StoreShardMap, PerformanceCounterName.DdrOperationsPerSec);
                    return result;
                }
                else
                {
                    // Either: 
                    // 1) The mapping is still within the TTL. No refresh.
                    // 2) Mapping was not in cache, we originally did a lookup for mapping in GSM and even then could not connect.
                    throw;
                }
            }
        }

        /// <summary>
        /// Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
        /// that contains the key value.
        /// </summary>
        /// <typeparam name="TMapping">Mapping type.</typeparam>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="key">Input key value.</param>
        /// <param name="constructMapping">Delegate to construct a mapping object.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <param name="connectionString">
        /// Connection string with credential information, the DataSource and Database are 
        /// obtained from the results of the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>A task encapsulating an opened SqlConnection as the result.</returns>
        protected async Task<SqlConnection> OpenConnectionForKeyAsync<TMapping, TKey>(
            TKey key,
            Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping,
            ShardManagementErrorCategory errorCategory,
            string connectionString,
            ConnectionOptions options = ConnectionOptions.Validate) where TMapping : class, IShardProvider
        {
            ShardKey sk = new ShardKey(ShardKey.ShardKeyTypeFromType(typeof(TKey)), key);

            // Try to find the mapping within the cache.
            ICacheStoreMapping csm = this.Manager.Cache.LookupMappingByKey(this.ShardMap.StoreShardMap, sk);

            IStoreMapping sm;

            if (csm != null)
            {
                sm = csm.Mapping;
            }
            else
            {
                sm = await this.LookupMappingForOpenConnectionForKeyAsync(
                    sk,
                    CacheStoreMappingUpdatePolicy.OverwriteExisting,
                    errorCategory).ConfigureAwait(false);
            }

            SqlConnection result;
            bool lookupMappingOnEx = false;
            CacheStoreMappingUpdatePolicy cacheUpdatePolicyOnEx = CacheStoreMappingUpdatePolicy.OverwriteExisting;

            try
            {
                // Initially attempt to connect based on lookup results from either cache or GSM.
                result = await this.ShardMap.OpenConnectionAsync(
                    constructMapping(this.Manager, this.ShardMap, sm),
                    connectionString,
                    options).ConfigureAwait(false);

                csm.ResetTimeToLiveIfNecessary();

                return result;
            }
            catch (ShardManagementException smme)
            {
                // If we hit a validation failure due to stale version of mapping, we will perform one more attempt. 
                if (((options & ConnectionOptions.Validate) == ConnectionOptions.Validate) &&
                    smme.ErrorCategory == ShardManagementErrorCategory.Validation &&
                    smme.ErrorCode == ShardManagementErrorCode.MappingDoesNotExist)
                {
                    // Assumption here is that this time the attempt should succeed since the cache entry 
                    // has already been either evicted, or updated based on latest data from the server.
                    lookupMappingOnEx = true;
                    cacheUpdatePolicyOnEx = CacheStoreMappingUpdatePolicy.OverwriteExisting;
                }
                else
                {
                    // The error was not due to validation but something else e.g.
                    // 1) Shard map does not exist
                    // 2) Mapping could not be found.
                    throw;
                }
            }
            catch (SqlException)
            {
                // We failed to connect. If we were trying to connect from an entry in cache and mapping expired in cache.
                if (csm != null && csm.HasTimeToLiveExpired())
                {
                    using (IdLock _idLock = new IdLock(csm.Mapping.StoreShard.Id))
                    {
                        // Similar to DCL pattern, we need to refresh the mapping again to see if we still need to go to the store
                        // to lookup the mapping after acquiring the shard lock. It might be the case that a fresh version has already
                        // been obtained by some other thread.
                        csm = this.Manager.Cache.LookupMappingByKey(this.ShardMap.StoreShardMap, sk);

                        // Only go to store if the mapping is stale even after refresh.
                        if (csm == null || csm.HasTimeToLiveExpired())
                        {
                            // Refresh the mapping in cache. And try to open the connection after refresh.
                            lookupMappingOnEx = true;
                            cacheUpdatePolicyOnEx = CacheStoreMappingUpdatePolicy.UpdateTimeToLive;
                        }
                        else
                        {
                            sm = csm.Mapping;
                        }
                    }
                }
                else
                {
                    // Either: 
                    // 1) The mapping is still within the TTL. No refresh.
                    // 2) Mapping was not in cache, we originally did a lookup for mapping in GSM and even then could not connect.
                    throw;
                }
            }

            if (lookupMappingOnEx)
            {
                sm = await this.LookupMappingForOpenConnectionForKeyAsync(
                    sk,
                    cacheUpdatePolicyOnEx,
                    errorCategory).ConfigureAwait(false);
            }

            // One last attempt to open the connection after a cache refresh
            result = await this.ShardMap.OpenConnectionAsync(
                        constructMapping(this.Manager, this.ShardMap, sm),
                        connectionString,
                        options).ConfigureAwait(false);

            // Reset TTL on successful connection.
            csm.ResetTimeToLiveIfNecessary();

            return result;
        }

        /// <summary>
        /// Adds a mapping to shard map.
        /// </summary>
        /// <typeparam name="TMapping">Mapping type.</typeparam>
        /// <param name="mapping">Mapping being added.</param>
        /// <param name="constructMapping">Delegate to construct a mapping object.</param>
        /// <returns>The added mapping object.</returns>
        protected TMapping Add<TMapping>(
            TMapping mapping,
            Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping)
            where TMapping : class, IShardProvider, IMappingInfoProvider
        {
            ExceptionUtils.EnsureShardBelongsToShardMap(
                this.Manager,
                this.ShardMap,
                mapping.ShardInfo,
                "CreateMapping",
                mapping.Kind == MappingKind.PointMapping ? "PointMapping" : "RangeMapping");

            this.EnsureMappingBelongsToShardMap(mapping, "Add", "mapping");

            TMapping newMapping = constructMapping(
                this.Manager,
                this.ShardMap,
                new DefaultStoreMapping(
                    mapping.StoreMapping.Id,
                    mapping.StoreMapping.ShardMapId,
                    new DefaultStoreShard(
                        mapping.ShardInfo.StoreShard.Id,
                        Guid.NewGuid(),
                        mapping.ShardInfo.StoreShard.ShardMapId,
                        mapping.ShardInfo.StoreShard.Location,
                        mapping.ShardInfo.StoreShard.Status),
                    mapping.StoreMapping.MinValue,
                    mapping.StoreMapping.MaxValue,
                    mapping.StoreMapping.Status,
                    mapping.StoreMapping.LockOwnerId));

            using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateAddMappingOperation(
                this.Manager,
                mapping.Kind == MappingKind.RangeMapping ?
                    StoreOperationCode.AddRangeMapping :
                    StoreOperationCode.AddPointMapping,
                this.ShardMap.StoreShardMap,
                newMapping.StoreMapping))
            {
                op.Do();
            }

            return newMapping;
        }

        /// <summary>
        /// Removes a mapping from shard map.
        /// </summary>
        /// <typeparam name="TMapping">Mapping type.</typeparam>
        /// <param name="mapping">Mapping being removed.</param>
        /// <param name="constructMapping">Delegate to construct a mapping object.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        protected void Remove<TMapping>(
            TMapping mapping,
            Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping,
            Guid lockOwnerId)
            where TMapping : class, IShardProvider, IMappingInfoProvider
        {
            this.EnsureMappingBelongsToShardMap<TMapping>(mapping, "Remove", "mapping");

            TMapping newMapping = constructMapping(
                this.Manager,
                this.ShardMap,
                new DefaultStoreMapping(
                    mapping.StoreMapping.Id,
                    mapping.StoreMapping.ShardMapId,
                    new DefaultStoreShard(
                        mapping.ShardInfo.Id,
                        Guid.NewGuid(),
                        mapping.ShardInfo.StoreShard.ShardMapId,
                        mapping.ShardInfo.StoreShard.Location,
                        mapping.ShardInfo.StoreShard.Status),
                    mapping.StoreMapping.MinValue,
                    mapping.StoreMapping.MaxValue,
                    mapping.StoreMapping.Status,
                    mapping.StoreMapping.LockOwnerId));

            using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateRemoveMappingOperation(
                this.Manager,
                mapping.Kind == MappingKind.RangeMapping ?
                    StoreOperationCode.RemoveRangeMapping :
                    StoreOperationCode.RemovePointMapping,
                this.ShardMap.StoreShardMap,
                newMapping.StoreMapping,
                lockOwnerId))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <typeparam name="TMapping">Mapping type.</typeparam>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="key">Input key value.</param>
        /// <param name="useCache">Whether to use cache for lookups.</param>
        /// <param name="constructMapping">Delegate to construct a mapping object.</param>
        /// <param name="errorCategory">Category under which errors must be thrown.</param>
        /// <returns>Mapping that contains the key value.</returns>
        protected TMapping Lookup<TMapping, TKey>(
            TKey key,
            bool useCache,
            Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping,
            ShardManagementErrorCategory errorCategory)
            where TMapping : class, IShardProvider
        {
            ShardKey sk = new ShardKey(ShardKey.ShardKeyTypeFromType(typeof(TKey)), key);

            if (useCache)
            {
                ICacheStoreMapping cachedMapping = this.Manager.Cache.LookupMappingByKey(this.ShardMap.StoreShardMap, sk);

                if (cachedMapping != null)
                {
                    return constructMapping(this.Manager, this.ShardMap, cachedMapping.Mapping);
                }
            }

            // Cache-miss, find mapping for given key in GSM.
            TMapping m = null;

            IStoreResults gsmResult;

            Stopwatch stopwatch = Stopwatch.StartNew();

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(
                this.Manager,
                "Lookup",
                this.ShardMap.StoreShardMap,
                sk,
                CacheStoreMappingUpdatePolicy.OverwriteExisting,
                errorCategory,
                true,
                false))
            {
                gsmResult = op.Do();
            }

            stopwatch.Stop();

            Tracer.TraceVerbose(
                TraceSourceConstants.ComponentNames.BaseShardMapper,
                "Lookup",
                "Lookup key from GSM complete; Key type : {0}; Result: {1}; Duration: {2}",
                typeof(TKey),
                gsmResult.Result,
                stopwatch.Elapsed);

            // If we could not locate the mapping, we return null and do nothing here.
            if (gsmResult.Result != StoreResult.MappingNotFoundForKey)
            {
                return gsmResult.StoreMappings.Select(sm => constructMapping(this.Manager, this.ShardMap, sm)).Single();
            }

            return m;
        }

        /// <summary>
        /// Finds mapping in store for OpenConnectionForKey operation.
        /// </summary>
        /// <param name="sk">Key to find.</param>
        /// <param name="policy">Cache update policy.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <returns>Mapping corresponding to the given key if found.</returns>
        private IStoreMapping LookupMappingForOpenConnectionForKey(
            ShardKey sk,
            CacheStoreMappingUpdatePolicy policy,
            ShardManagementErrorCategory errorCategory)
        {
            IStoreResults gsmResult;

            Stopwatch stopwatch = Stopwatch.StartNew();

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(
                this.Manager,
                "Lookup",
                this.ShardMap.StoreShardMap,
                sk,
                policy,
                errorCategory,
                true,
                false))
            {
                gsmResult = op.Do();
            }

            stopwatch.Stop();

            Tracer.TraceVerbose(
                TraceSourceConstants.ComponentNames.BaseShardMapper,
                "LookupMappingForOpenConnectionForKey",
                "Lookup key from GSM complete; Key type : {0}; Result: {1}; Duration: {2}",
                sk.DataType,
                gsmResult.Result,
                stopwatch.Elapsed);

            // If we could not locate the mapping, we throw.
            if (gsmResult.Result == StoreResult.MappingNotFoundForKey)
            {
                throw new ShardManagementException(
                    errorCategory,
                    ShardManagementErrorCode.MappingNotFoundForKey,
                    Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal,
                    this.ShardMap.Name,
                    StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal,
                    "LookupMappingForOpenConnectionForKey");
            }
            else
            {
                return gsmResult.StoreMappings.Single();
            }
        }


        /// <summary>
        /// Asynchronously finds the mapping in store for OpenConnectionForKey operation.
        /// </summary>
        /// <param name="sk">Key to find.</param>
        /// <param name="policy">Cache update policy.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <returns>Task with the Mapping corresponding to the given key if found as the result.</returns>
        private async Task<IStoreMapping> LookupMappingForOpenConnectionForKeyAsync(
            ShardKey sk,
            CacheStoreMappingUpdatePolicy policy,
            ShardManagementErrorCategory errorCategory)
        {
            IStoreResults gsmResult;

            Stopwatch stopwatch = Stopwatch.StartNew();

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateFindMappingByKeyGlobalOperation(
                this.Manager,
                "Lookup",
                this.ShardMap.StoreShardMap,
                sk,
                policy,
                errorCategory,
                true,
                false))
            {
                gsmResult = await op.DoAsync().ConfigureAwait(false);
            }

            stopwatch.Stop();

            Tracer.TraceVerbose(
                TraceSourceConstants.ComponentNames.BaseShardMapper,
                "LookupMappingForOpenConnectionForKeyAsync",
                "Lookup key from GSM complete; Key type : {0}; Result: {1}; Duration: {2}",
                sk.DataType,
                gsmResult.Result,
                stopwatch.Elapsed);

            // If we could not locate the mapping, we throw.
            if (gsmResult.Result == StoreResult.MappingNotFoundForKey)
            {
                throw new ShardManagementException(
                    errorCategory,
                    ShardManagementErrorCode.MappingNotFoundForKey,
                    Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal,
                    this.ShardMap.Name,
                    StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal,
                    "LookupMappingForOpenConnectionForKeyAsync");
            }
            else
            {
                return gsmResult.StoreMappings.Single();
            }
        }

        /// <summary>
        /// Gets all the mappings that exist within given range.
        /// </summary>
        /// <param name="range">Optional range value, if null, we cover everything.</param>
        /// <param name="shard">Optional shard parameter, if null, we cover all shards.</param>
        /// <param name="constructMapping">Delegate to construct a mapping object.</param>
        /// <param name="errorCategory">Category under which errors will be posted.</param>
        /// <param name="mappingType">Name of mapping type.</param>
        /// <returns>Read-only collection of mappings that overlap with given range.</returns>
        protected IReadOnlyList<TMapping> GetMappingsForRange<TMapping, TKey>(
            Range<TKey> range,
            Shard shard,
            Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping,
            ShardManagementErrorCategory errorCategory,
            string mappingType)
            where TMapping : class
        {
            ShardRange sr = null;

            if (shard != null)
            {
                ExceptionUtils.EnsureShardBelongsToShardMap(
                    this.Manager,
                    this.ShardMap,
                    shard,
                    "GetMappings",
                    mappingType);
            }

            if (range != null)
            {
                sr = new ShardRange(
                    new ShardKey(ShardKey.ShardKeyTypeFromType(typeof(TKey)), range.Low),
                    new ShardKey(ShardKey.ShardKeyTypeFromType(typeof(TKey)), range.HighIsMax ? null : (object)range.High));
            }

            IStoreResults result;

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateGetMappingsByRangeGlobalOperation(
                this.Manager,
                "GetMappingsForRange",
                this.ShardMap.StoreShardMap,
                shard != null ? shard.StoreShard : null,
                sr,
                errorCategory,
                true, // Always cache.
                false))
            {
                result = op.Do();
            }

            return result.StoreMappings
                            .Select(sm => constructMapping(this.Manager, this.ShardMap, sm))
                            .ToList()
                            .AsReadOnly();
        }

        /// <summary>
        /// Allows for update to a mapping with the updates provided in 
        /// the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentMapping">Mapping being updated.</param>
        /// <param name="update">Updated properties of the Shard.</param>
        /// <param name="constructMapping">Delegate to construct a mapping object.</param>
        /// <param name="statusAsInt">Delegate to get the mapping status as an integer value.</param>
        /// <param name="intAsStatus">Delegate to get the mapping status from an integer value.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>New instance of mapping with updated information.</returns>
        protected TMapping Update<TMapping, TUpdate, TStatus>(
            TMapping currentMapping,
            TUpdate update,
            Func<ShardMapManager, ShardMap, IStoreMapping, TMapping> constructMapping,
            Func<TStatus, int> statusAsInt,
            Func<int, TStatus> intAsStatus,
            Guid lockOwnerId = default(Guid))
            where TUpdate : class, IMappingUpdate<TStatus>
            where TMapping : class, IShardProvider, IMappingInfoProvider
            where TStatus : struct
        {
            Debug.Assert(currentMapping != null);
            Debug.Assert(update != null);

            this.EnsureMappingBelongsToShardMap<TMapping>(currentMapping, "Update", "currentMapping");

            IMappingUpdate<TStatus> mu = update as IMappingUpdate<TStatus>;

            // CONSIDER(wbasheer): Have refresh semantics for trivial case when nothing is modified.
            if (!mu.IsAnyPropertySet(MappingUpdatedProperties.All))
            {
                return currentMapping;
            }

            bool shardChanged = mu.IsAnyPropertySet(MappingUpdatedProperties.Shard) && !mu.Shard.Equals(currentMapping.ShardInfo);

            // Ensure that shard belongs to current shard map.
            if (shardChanged)
            {
                ExceptionUtils.EnsureShardBelongsToShardMap(
                    this.Manager,
                    this.ShardMap,
                    mu.Shard,
                    "UpdateMapping",
                    currentMapping.Kind == MappingKind.PointMapping ? "PointMapping" : "RangeMapping");
            }

            IStoreShard originalShard = new DefaultStoreShard(
                currentMapping.ShardInfo.Id,
                Guid.NewGuid(),
                currentMapping.ShardInfo.StoreShard.ShardMapId,
                currentMapping.ShardInfo.StoreShard.Location,
                currentMapping.ShardInfo.StoreShard.Status);

            IStoreMapping originalMapping = new DefaultStoreMapping(
                    currentMapping.StoreMapping.Id,
                    currentMapping.ShardMapId,
                    originalShard,
                    currentMapping.StoreMapping.MinValue,
                    currentMapping.StoreMapping.MaxValue,
                    currentMapping.StoreMapping.Status,
                    lockOwnerId);

            IStoreShard updatedShard;

            if (shardChanged)
            {
                updatedShard = new DefaultStoreShard(
                update.Shard.ShardInfo.Id,
                Guid.NewGuid(),
                update.Shard.ShardInfo.StoreShard.ShardMapId,
                update.Shard.ShardInfo.StoreShard.Location,
                update.Shard.ShardInfo.StoreShard.Status);
            }
            else
            {
                updatedShard = originalShard;
            }

            IStoreMapping updatedMapping = new DefaultStoreMapping(
                    Guid.NewGuid(),
                    currentMapping.ShardMapId,
                    updatedShard,
                    currentMapping.StoreMapping.MinValue,
                    currentMapping.StoreMapping.MaxValue,
                    mu.IsAnyPropertySet(MappingUpdatedProperties.Status) ?
                        statusAsInt(update.Status) :
                        currentMapping.StoreMapping.Status,
                    lockOwnerId);

            bool fromOnlineToOffline = mu.IsMappingBeingTakenOffline(intAsStatus(currentMapping.StoreMapping.Status));

            StoreOperationCode opCode;

            if (fromOnlineToOffline)
            {
                opCode = currentMapping.Kind == MappingKind.PointMapping ?
                    StoreOperationCode.UpdatePointMappingWithOffline :
                    StoreOperationCode.UpdateRangeMappingWithOffline;
            }
            else
            {
                opCode = currentMapping.Kind == MappingKind.PointMapping ?
                    StoreOperationCode.UpdatePointMapping :
                    StoreOperationCode.UpdateRangeMapping;
            }

            using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateUpdateMappingOperation(
                this.Manager,
                opCode,
                this.ShardMap.StoreShardMap,
                originalMapping,
                updatedMapping,
                this.ShardMap.ApplicationNameSuffix,
                lockOwnerId))
            {
                op.Do();
            }

            return constructMapping(this.Manager, this.ShardMap, updatedMapping);
        }

        /// <summary>
        /// Gets the lock owner of a mapping.
        /// </summary>
        /// <param name="mapping">The mapping</param>
        /// <param name="errorCategory">Error category to use for the store operation</param>
        /// <returns>Lock owner for the mapping.</returns>
        internal Guid GetLockOwnerForMapping<TMapping>(TMapping mapping, ShardManagementErrorCategory errorCategory) where TMapping : class, IShardProvider, IMappingInfoProvider
        {
            this.EnsureMappingBelongsToShardMap<TMapping>(mapping, "LookupLockOwner", "mapping");

            IStoreResults result;

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateFindMappingByIdGlobalOperation(
                this.Manager,
                "LookupLockOwner",
                this.ShardMap.StoreShardMap,
                mapping.StoreMapping,
                errorCategory))
            {
                result = op.Do();
            }

            return result.StoreMappings.Single().LockOwnerId;
        }

        /// <summary>
        /// Locks or unlocks a given mapping or all mappings.
        /// </summary>
        /// <param name="mapping">Optional mapping</param>
        /// <param name="lockOwnerId">The lock onwer id</param>
        /// <param name="lockOwnerIdOpType">Operation to perform on this mapping with the given lockOwnerId</param>
        /// <param name="errorCategory">Error category to use for the store operation</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity", Justification = "Follows GSM/LSM interaction archetype")]
        internal void LockOrUnlockMappings<TMapping>(TMapping mapping, Guid lockOwnerId, LockOwnerIdOpType lockOwnerIdOpType, ShardManagementErrorCategory errorCategory) where TMapping : class, IShardProvider, IMappingInfoProvider
        {
            string operationName = lockOwnerIdOpType == LockOwnerIdOpType.Lock ? "Lock" : "UnLock";

            if (lockOwnerIdOpType != LockOwnerIdOpType.UnlockAllMappingsForId && lockOwnerIdOpType != LockOwnerIdOpType.UnlockAllMappings)
            {
                this.EnsureMappingBelongsToShardMap<TMapping>(mapping, operationName, "mapping");

                if (lockOwnerIdOpType == LockOwnerIdOpType.Lock &&
                    lockOwnerId == MappingLockToken.ForceUnlock.LockOwnerId)
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._ShardMapping_LockIdNotSupported,
                                mapping.ShardInfo.Location,
                                this.ShardMap.Name,
                                lockOwnerId),
                                "lockOwnerId");
                }
            }
            else
            {
                Debug.Assert(mapping == null);
            }

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateLockOrUnLockMappingsGlobalOperation(
                this.Manager,
                operationName,
                this.ShardMap.StoreShardMap,
                mapping != null ? mapping.StoreMapping : null,
                lockOwnerId,
                lockOwnerIdOpType,
                errorCategory))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Validates the input parameters and ensures that the mapping parameter belong to this shard map.
        /// </summary>
        /// <param name="mapping">Mapping to be validated.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="parameterName">Parameter name for mapping parameter.</param>
        protected void EnsureMappingBelongsToShardMap<TMapping>(
            TMapping mapping,
            string operationName,
            string parameterName)
            where TMapping : class, IMappingInfoProvider
        {
            Debug.Assert(mapping.Manager != null);

            // Ensure that shard belongs to current shard map.
            if (mapping.ShardMapId != this.ShardMap.Id)
            {
                throw new InvalidOperationException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMapping_DifferentShardMap,
                        mapping.TypeName,
                        operationName,
                        this.ShardMap.Name,
                        parameterName));
            }

            // Ensure that the mapping objects belong to same shard map.
            if (mapping.Manager != this.Manager)
            {
                throw new InvalidOperationException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMapping_DifferentShardMapManager,
                        mapping.TypeName,
                        operationName,
                        this.Manager.Credentials.ShardMapManagerLocation,
                        this.ShardMap.Name,
                        parameterName));
            }
        }
    }
}
