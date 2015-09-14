// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Serves as the entry point for creation, management and lookup operations over shard maps.
    /// </summary>
    public sealed class ShardMapManager
    {
        /// <summary>
        /// Given the connection string, opens up the corresponding data source and obtains the ShardMapManager.
        /// </summary>
        /// <param name="credentials">Credentials for performing ShardMapManager operations.</param>
        /// <param name="storeConnectionFactory">Factory for store connections.</param>
        /// <param name="storeOperationFactory">Factory for store operations.</param>
        /// <param name="cacheStore">Cache store.</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryPolicy">Policy for performing retries on connections to shard map manager database.</param>
        /// <param name="retryBehavior">Policy for detecting transient errors.</param>
        internal ShardMapManager(
            SqlShardMapManagerCredentials credentials,
            IStoreConnectionFactory storeConnectionFactory,
            IStoreOperationFactory storeOperationFactory,
            ICacheStore cacheStore,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryPolicy retryPolicy,
            RetryBehavior retryBehavior)
            : this(credentials, storeConnectionFactory, storeOperationFactory, cacheStore, loadPolicy, retryPolicy, retryBehavior, null)
        {
        }

        /// <summary>
        /// Given the connection string, opens up the corresponding data source and obtains the ShardMapManager.
        /// </summary>
        /// <param name="credentials">Credentials for performing ShardMapManager operations.</param>
        /// <param name="storeConnectionFactory">Factory for store connections.</param>
        /// <param name="storeOperationFactory">Factory for store operations.</param>
        /// <param name="cacheStore">Cache store.</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryPolicy">Policy for performing retries on connections to shard map manager database.</param>
        /// <param name="retryBehavior">Policy for detecting transient errors.</param>
        /// <param name="retryEventHandler">Event handler for store operation retry events.</param>
        internal ShardMapManager(
            SqlShardMapManagerCredentials credentials,
            IStoreConnectionFactory storeConnectionFactory,
            IStoreOperationFactory storeOperationFactory,
            ICacheStore cacheStore,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryPolicy retryPolicy,
            RetryBehavior retryBehavior,
            EventHandler<RetryingEventArgs> retryEventHandler)
        {
            Debug.Assert(credentials != null);

            this.Credentials = credentials;
            this.StoreConnectionFactory = storeConnectionFactory;
            this.StoreOperationFactory = storeOperationFactory;
            this.Cache = cacheStore;

            this.RetryPolicy = new TransientFaultHandling.RetryPolicy(
                new ShardManagementTransientErrorDetectionStrategy(retryBehavior),
                retryPolicy.GetRetryStrategy());

            // Register for TfhImpl.RetryPolicy.Retrying event.
            this.RetryPolicy.Retrying += this.ShardMapManagerRetryingEventHandler;

            // Add user specified event handler.
            if (retryEventHandler != null)
            {
                this.ShardMapManagerRetrying += retryEventHandler;
            }

            if (loadPolicy == ShardMapManagerLoadPolicy.Eager)
            {
                // We eagerly load everything from ShardMapManager. In case of lazy
                // loading policy, we will add things to local caches based on cache
                // misses on lookups.
                this.LoadFromStore();
            }
        }

        /// <summary>
        /// Event to be raised on Shard Map Manager store retries.
        /// </summary>
        internal event EventHandler<RetryingEventArgs> ShardMapManagerRetrying;

        /// <summary>
        /// Credentials for performing ShardMapManager operations.
        /// </summary>
        internal SqlShardMapManagerCredentials Credentials
        {
            get;
            private set;
        }

        /// <summary>
        /// Factory for store connections.
        /// </summary>
        internal IStoreConnectionFactory StoreConnectionFactory
        {
            get;
            private set;
        }

        /// <summary>
        /// Factory for store operations.
        /// </summary>
        internal IStoreOperationFactory StoreOperationFactory
        {
            get;
            private set;
        }

        /// <summary>
        /// Policy for performing retries on connections to shard map manager database.
        /// </summary>
        internal TransientFaultHandling.RetryPolicy RetryPolicy
        {
            get;
            private set;
        }

        /// <summary>
        /// Local cache.
        /// </summary>
        internal ICacheStore Cache
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
        /// Creates a list based <see cref="ListShardMap{TKey}"/>.
        /// </summary>
        /// <typeparam name="TKey">Type of keys.</typeparam>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <returns>List shard map with the specified name.</returns>
        public ListShardMap<TKey> CreateListShardMap<TKey>(string shardMapName)
        {
            ShardMapManager.ValidateShardMapName(shardMapName);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                DefaultStoreShardMap dssm = new DefaultStoreShardMap(
                    Guid.NewGuid(),
                    shardMapName,
                    ShardMapType.List,
                    ShardKey.ShardKeyTypeFromType(typeof(TKey)));

                ListShardMap<TKey> listShardMap = new ListShardMap<TKey>(this, dssm);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "CreateListShardMap",
                    "Start; ShardMap: {0}",
                    shardMapName);

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.AddShardMapToStore("CreateListShardMap", dssm);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "CreateListShardMap",
                    "Added ShardMap to Store; ShardMap: {0} Duration: {1}",
                    shardMapName,
                    stopwatch.Elapsed);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "CreateListShardMap",
                    "Complete; ShardMap: {0} Duration: {1}",
                    shardMapName,
                    stopwatch.Elapsed);

                return listShardMap;
            }
        }

        /// <summary>
        /// Create a range based <see cref="RangeShardMap{TKey}"/>.
        /// </summary>
        /// <typeparam name="TKey">Type of keys.</typeparam>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <returns>Range shard map with the specified name.</returns>
        public RangeShardMap<TKey> CreateRangeShardMap<TKey>(string shardMapName)
        {
            ShardMapManager.ValidateShardMapName(shardMapName);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                DefaultStoreShardMap dssm = new DefaultStoreShardMap(
                    Guid.NewGuid(),
                    shardMapName,
                    ShardMapType.Range,
                    ShardKey.ShardKeyTypeFromType(typeof(TKey)));

                RangeShardMap<TKey> rangeShardMap = new RangeShardMap<TKey>(this, dssm);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "CreateRangeShardMap",
                    "Start; ShardMap: {0}",
                    shardMapName);

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.AddShardMapToStore("CreateRangeShardMap", dssm);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "CreateRangeShardMap", "Added ShardMap to Store; ShardMap: {0} Duration: {1}",
                shardMapName,
                    stopwatch.Elapsed);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "CreateRangeShardMap",
                    "Complete; ShardMap: {0} Duration: {1}",
                    shardMapName,
                    stopwatch.Elapsed);

                return rangeShardMap;
            }
        }

        /// <summary>
        /// Removes the specified shard map.
        /// </summary>
        /// <param name="shardMap">Shardmap to be removed.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void DeleteShardMap(ShardMap shardMap)
        {
            this.ValidateShardMap(shardMap);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "DeleteShardMap",
                    "Start; ShardMap: {0}",
                    shardMap.Name);

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.RemoveShardMapFromStore(shardMap.StoreShardMap);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "DeleteShardMap",
                    "Complete; ShardMap: {0}; Duration: {1}",
                    shardMap.Name,
                    stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Obtains all shard maps associated with the shard map manager.
        /// </summary>
        /// <returns>Collection of shard maps associated with the shard map manager.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Method is appropriate here because we're not just returning object state")]
        public IEnumerable<ShardMap> GetShardMaps()
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetShardMaps",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                IEnumerable<ShardMap> result = this.GetShardMapsFromStore();

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetShardMaps",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Obtains a <see cref="ShardMap"/> given the name.
        /// </summary>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <returns>Shardmap with the specificed name.</returns>
        public ShardMap GetShardMap(string shardMapName)
        {
            ShardMapManager.ValidateShardMapName(shardMapName);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetShardMap",
                    "Start; ShardMap: {0}",
                    shardMapName);

                ShardMap shardMap = this.LookupAndConvertShardMapHelper<ShardMap>(
                    "GetShardMap",
                shardMapName,
                    (sm, t) => sm,
                    true);

                Debug.Assert(shardMap != null);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetShardMap",
                    "Complete; ShardMap: {0}",
                    shardMapName);

                return shardMap;
            }
        }

        /// <summary>
        /// Tries to obtains a <see cref="ShardMap"/> given the name.
        /// </summary>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <param name="shardMap">Shard map with the specified name.</param>
        /// <returns><c>true</c> if shard map with the specified name was found, <c>false</c> otherwise.</returns>
        public bool TryGetShardMap(string shardMapName, out ShardMap shardMap)
        {
            ShardMapManager.ValidateShardMapName(shardMapName);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "TryGetShardMap",
                    "Start; ShardMap: {0}",
                    shardMapName);

                shardMap = this.LookupAndConvertShardMapHelper<ShardMap>(
                    "TryGetShardMap",
                    shardMapName,
                    (sm, t) => sm,
                    false);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "TryGetShardMap",
                    "Complete; ShardMap: {0}",
                    shardMapName);

                return shardMap != null;
            }
        }

        /// <summary>
        /// Obtains a <see cref="ListShardMap{TKey}"/> given the name. 
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <returns>Resulting ShardMap.</returns>
        public ListShardMap<TKey> GetListShardMap<TKey>(string shardMapName)
        {
            ShardMapManager.ValidateShardMapName(shardMapName);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetListShardMap",
                    "Start; ShardMap: {0}",
                    shardMapName);

                ListShardMap<TKey> shardMap = this.LookupAndConvertShardMapHelper<ListShardMap<TKey>>(
                    "GetListShardMap",
                shardMapName,
                ShardMapExtensions.AsListShardMap<TKey>,
                    true);

                Debug.Assert(shardMap != null);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetListShardMap",
                    "Complete; ShardMap: {0}",
                    shardMapName);

                return shardMap;
            }
        }

        /// <summary>
        /// Tries to obtains a <see cref="ListShardMap{TKey}"/> given the name. 
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <param name="shardMap">Shard map with the specified name.</param>
        /// <returns><c>true</c> if shard map with the specified name was found, <c>false</c> otherwise.</returns>
        public bool TryGetListShardMap<TKey>(string shardMapName, out ListShardMap<TKey> shardMap)
        {
            ShardMapManager.ValidateShardMapName(shardMapName);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "TryGetListShardMap",
                    "Start; ShardMap: {0}",
                    shardMapName);

                shardMap = this.LookupAndConvertShardMapHelper<ListShardMap<TKey>>(
                    "TryGetListShardMap",
                    shardMapName,
                    ShardMapExtensions.AsListShardMap<TKey>,
                    false);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "TryGetListShardMap",
                    "Complete; ShardMap: {0}",
                    shardMapName);

                return shardMap != null;
            }
        }

        /// <summary>
        /// Obtains a <see cref="RangeShardMap{TKey}"/> given the name. 
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <returns>Resulting ShardMap.</returns>
        public RangeShardMap<TKey> GetRangeShardMap<TKey>(string shardMapName)
        {
            ShardMapManager.ValidateShardMapName(shardMapName);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetRangeShardMap",
                    "Start; ShardMap: {0}",
                    shardMapName);

                RangeShardMap<TKey> shardMap = this.LookupAndConvertShardMapHelper<RangeShardMap<TKey>>(
                    "GetRangeShardMap",
                shardMapName,
                ShardMapExtensions.AsRangeShardMap<TKey>,
                    true);

                Debug.Assert(shardMap != null);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetRangeShardMap",
                    "Complete; ShardMap: {0}",
                    shardMapName);

                return shardMap;
            }
        }

        /// <summary>
        /// Tries to obtains a <see cref="RangeShardMap{TKey}"/> given the name. 
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <param name="shardMap">Shard map with the specified name.</param>
        /// <returns><c>true</c> if shard map with the specified name was found, <c>false</c> otherwise.</returns>
        public bool TryGetRangeShardMap<TKey>(string shardMapName, out RangeShardMap<TKey> shardMap)
        {
            ShardMapManager.ValidateShardMapName(shardMapName);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "TryGetRangeShardMap",
                    "Start; ShardMap: {0}",
                    shardMapName);

                shardMap = this.LookupAndConvertShardMapHelper<RangeShardMap<TKey>>(
                    "TryGetRangeShardMap",
                    shardMapName,
                    ShardMapExtensions.AsRangeShardMap<TKey>,
                    false);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "TryGetRangeShardMap",
                    "Complete; ShardMap: {0}",
                    shardMapName);

                return shardMap != null;
            }
        }

        /// <summary>
        /// Obtains distinct shard locations from the shard map manager.
        /// </summary>
        /// <returns>Collection of shard locations associated with the shard map manager.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Method is appropriate here because we're not just returning object state")]
        public IEnumerable<ShardLocation> GetDistinctShardLocations()
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetDistinctShardLocations",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                IEnumerable<ShardLocation> result = this.GetDistinctShardLocationsFromStore();

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "GetDistinctShardLocations",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return result;
            }
        }

        /// <summary>
        /// Upgrades store hosting global shard map to the latest version supported by library.
        /// </summary>
        public void UpgradeGlobalStore()
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "UpgradeGlobalShardMapManager",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.UpgradeStoreGlobal(GlobalConstants.GsmVersionClient);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "UpgradeGlobalShardMapManager",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Upgrades store location to the latest version supported by library.
        /// </summary>
        /// <param name="location">Shard location to upgrade.</param>
        public void UpgradeLocalStore(ShardLocation location)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "UpgradeGlobalShardMapManager",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.UpgradeStoreLocal(location, GlobalConstants.LsmVersionClient);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "UpgradeGlobalShardMapManager",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Obtains the recovery manager for the current shard map manager instance.
        /// </summary>
        /// <returns>
        /// Recovery manager for the shard map manager.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Method is appropriate here because we're not just returning object state")]
        public RecoveryManager GetRecoveryManager()
        {
            return new RecoveryManager(this);
        }

        /// <summary>
        /// Obtains the schema info collection object for the current shard map manager instance.
        /// </summary>
        /// <returns>schema info collection for shard map manager.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Method is appropriate here because we're not just returning object state")]
        public SchemaInfoCollection GetSchemaInfoCollection()
        {
            return new SchemaInfoCollection(this);
        }

        #region Internal Lookup functions

        /// <summary>
        /// Finds a shard map from cache if requested and if necessary from global shard map.
        /// </summary>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <param name="lookInCacheFirst">Whether to skip first lookup in cache.</param>
        /// <returns>Shard map object corresponding to one being searched.</returns>
        internal ShardMap LookupShardMapByName(string operationName, string shardMapName, bool lookInCacheFirst)
        {
            IStoreShardMap ssm = null;

            if (lookInCacheFirst)
            {
                // Typical scenario will result in immediate lookup succeeding.
                ssm = this.Cache.LookupShardMapByName(shardMapName);
            }

            ShardMap shardMap;

            // Cache miss. Go to store and add entry to cache.
            if (ssm == null)
            {
                Stopwatch stopwatch = Stopwatch.StartNew();

                shardMap = this.LookupShardMapByNameInStore(operationName, shardMapName);

                stopwatch.Stop();

                Tracer.TraceInfo(TraceSourceConstants.ComponentNames.ShardMapManager,
                    "LookupShardMapByName", "Lookup ShardMap: {0} in store complete; Duration: {1}",
                    shardMapName, stopwatch.Elapsed);
            }
            else
            {
                shardMap = ShardMapUtils.CreateShardMapFromStoreShardMap(this, ssm);
            }

            return shardMap;
        }

        #endregion Internal Lookup functions

        /// <summary>
        /// Subscriber function to RetryPolicy.Retrying event.
        /// </summary>
        /// <param name="sender">Sender object (RetryPolicy)</param>
        /// <param name="arg">Event argument.</param>
        internal void ShardMapManagerRetryingEventHandler(object sender, TransientFaultHandling.RetryingEventArgs arg)
        {
            // Trace out retry event.
            Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.ShardMapManager,
                "ShardMapManagerRetryingEvent",
                "Retry Count: {0}; Delay: {1}",
                arg.CurrentRetryCount,
                arg.Delay
                );

            this.OnShardMapManagerRetryingEvent(new RetryingEventArgs(arg));
        }

        /// <summary>
        /// Publisher for ShardMapManagerRetryingEvent event.
        /// </summary>
        /// <param name="arg">Event argument.</param>
        internal void OnShardMapManagerRetryingEvent(RetryingEventArgs arg)
        {
            EventHandler<RetryingEventArgs> handler = this.ShardMapManagerRetrying;
            if (handler != null)
            {
                handler(this, arg);
            }
        }

        /// <summary>
        /// Upgrades store hosting global shard map to specified version. This will be used for upgrade testing.
        /// </summary>
        /// <param name="targetVersion">Target store version to deploy.</param>
        internal void UpgradeGlobalStore(Version targetVersion)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "UpgradeGlobalShardMapManager",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.UpgradeStoreGlobal(targetVersion);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "UpgradeGlobalShardMapManager",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Upgrades store location to the specified version. This will be used for upgrade testing.
        /// </summary>
        /// <param name="location">Shard location to upgrade.</param>
        /// <param name="targetVersion">Target store version to deploy.</param>
        internal void UpgradeLocalStore(ShardLocation location, Version targetVersion)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "UpgradeGlobalShardMapManager",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                this.UpgradeStoreLocal(location, targetVersion);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "UpgradeGlobalShardMapManager",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Performs lookup and conversion operation for shard map with given name.
        /// </summary>
        /// <typeparam name="TShardMap">Type to convert shard map to.</typeparam>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapName">Shard map name.</param>
        /// <param name="converter">Function to downcast a shard map to List/Range/Hash.</param>
        /// <param name="throwOnFailure">Whether to throw exception or return null on failure.</param>
        /// <returns>The converted shard map.</returns>
        private TShardMap LookupAndConvertShardMapHelper<TShardMap>(
            string operationName,
            string shardMapName,
            Func<ShardMap, bool, TShardMap> converter,
            bool throwOnFailure) where TShardMap : class
        {
            ShardMap sm = this.LookupShardMapByName(operationName, shardMapName, true);

            if (sm == null)
            {
                if (throwOnFailure)
                {
                    throw new ShardManagementException(
                        ShardManagementErrorCategory.ShardMapManager,
                        ShardManagementErrorCode.ShardMapLookupFailure,
                        Errors._ShardMapManager_ShardMapLookupFailed,
                        shardMapName,
                        this.Credentials.ShardMapManagerLocation);
                }

                return (TShardMap)null;
            }

            return converter(sm, throwOnFailure);
        }

        /// <summary>
        /// Loads the shard map manager and shards from Store.
        /// </summary>
        private void LoadFromStore()
        {
            this.Cache.Clear();

            using (IStoreOperationGlobal op = this.StoreOperationFactory.CreateLoadShardMapManagerGlobalOperation(this, "GetShardMapManager"))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Adds a shard to global shard map.
        /// </summary>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="ssm">Storage representation of shard map object.</param>
        private void AddShardMapToStore(string operationName, IStoreShardMap ssm)
        {
            using (IStoreOperationGlobal op = this.StoreOperationFactory.CreateAddShardMapGlobalOperation(this, operationName, ssm))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Removes a shard map from global shard map.
        /// </summary>
        /// <param name="ssm">Shard map to remove.</param>
        private void RemoveShardMapFromStore(IStoreShardMap ssm)
        {
            using (IStoreOperationGlobal op = this.StoreOperationFactory.CreateRemoveShardMapGlobalOperation(this, "DeleteShardMap", ssm))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Obtains all ShardMaps associated with the shard map manager.
        /// </summary>
        /// <returns>Collection of shard maps associated with the shard map manager.</returns>
        private IEnumerable<ShardMap> GetShardMapsFromStore()
        {
            IStoreResults result;

            using (IStoreOperationGlobal op = this.StoreOperationFactory.CreateGetShardMapsGlobalOperation(this, "GetShardMaps"))
            {
                result = op.Do();
            }

            return result.StoreShardMaps
                         .Select(ssm => ShardMapUtils.CreateShardMapFromStoreShardMap(this, ssm));
        }

        /// <summary>
        /// Get distinct locations for the shard map manager from store.
        /// </summary>
        /// <returns>Distinct locations from shard map manager.</returns>
        private IEnumerable<ShardLocation> GetDistinctShardLocationsFromStore()
        {
            IStoreResults result;

            using (IStoreOperationGlobal op = this.StoreOperationFactory.CreateGetDistinctShardLocationsGlobalOperation(this, "GetDistinctShardLocations"))
            {
                result = op.Do();
            }

            return result.StoreLocations
                         .Select(sl => sl.Location);
        }

        /// <summary>
        /// Upgrades store hosting GSM.
        /// </summary>
        /// <param name="targetVersion">Target version for store to upgrade to.</param>
        private void UpgradeStoreGlobal(Version targetVersion)
        {
            using (IStoreOperationGlobal op = this.StoreOperationFactory.CreateUpgradeStoreGlobalOperation(this, "UpgradeStoreGlobal", targetVersion))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Upgrades store at specified location.
        /// </summary>
        /// <param name="location">Store location to upgrade.</param>
        /// <param name="targetVersion">Target version for store to upgrade to.</param>
        /// <returns></returns>
        private void UpgradeStoreLocal(ShardLocation location, Version targetVersion)
        {
            using (IStoreOperationLocal op = this.StoreOperationFactory.CreateUpgradeStoreLocalOperation(this, location, "UpgradeStoreLocal", targetVersion))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Finds shard map with given name in global shard map.
        /// </summary>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapName">Name of shard map to search.</param>
        /// <returns>Shard map corresponding to given Id.</returns>
        private ShardMap LookupShardMapByNameInStore(string operationName, string shardMapName)
        {
            IStoreResults result;

            using (IStoreOperationGlobal op = this.StoreOperationFactory.CreateFindShardMapByNameGlobalOperation(this, operationName, shardMapName))
            {
                result = op.Do();
            }

            return result.StoreShardMaps
                         .Select(ssm => ShardMapUtils.CreateShardMapFromStoreShardMap(this, ssm))
                         .SingleOrDefault();
        }

        /// <summary>
        /// Validates the input shard map. This includes:
        /// * Ensuring that shard map belongs to this instance of shard map manager.
        /// </summary>
        /// <param name="shardMap">Input shard map.</param>
        private void ValidateShardMap(ShardMap shardMap)
        {
            ExceptionUtils.DisallowNullArgument(shardMap, "shardMap");

            if (shardMap.Manager != this)
            {
                throw new InvalidOperationException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMapManager_DifferentShardMapManager,
                        shardMap.Name,
                        this.Credentials.ShardMapManagerLocation));
            }
        }

        /// <summary>
        /// Ensures that the given shard map name is valid.
        /// </summary>
        /// <param name="shardMapName">Input shard map name.</param>
        private static void ValidateShardMapName(string shardMapName)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

            // Disallow non-alpha-numeric characters.
            foreach (char c in shardMapName)
            {
                if (!Char.IsLetter(c) && !Char.IsNumber(c) && !Char.IsPunctuation(c))
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._ShardMapManager_UnsupportedShardMapName,
                            shardMapName),
                        "shardMapName");
                }
            }

            // Ensure that length is within bounds.
            if (shardMapName.Length > GlobalConstants.MaximumShardMapNameLength)
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMapManager_UnsupportedShardMapNameLength,
                        shardMapName,
                        GlobalConstants.MaximumShardMapNameLength),
                    "shardMapName");
            }
        }
    }
}
