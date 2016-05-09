// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Client side cache store.
    /// </summary>
    internal class CacheStore : ICacheStore
    {
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
        /// Root of the cache tree.
        /// </summary>
        private CacheRoot _cacheRoot;

        /// <summary>
        /// Constructs an instance of client side cache object.
        /// </summary>
        protected internal CacheStore()
        {
            _cacheRoot = new CacheRoot();
        }

        /// <summary>
        /// Invoked for refreshing shard map in cache from store.
        /// </summary>
        /// <param name="shardMap">Storage representation of shard map.</param>
        public virtual void AddOrUpdateShardMap(IStoreShardMap shardMap)
        {
            using (WriteLockScope wls = _cacheRoot.GetWriteLockScope())
            {
                _cacheRoot.AddOrUpdate(shardMap);

                Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "OnAddOrUpdateShardMap",
                    "Cache Add/Update complete. ShardMap: {0}",
                    shardMap.Name);
            }
        }

        /// <summary>
        /// Invoked for deleting shard map in cache becase it no longer exists in store.
        /// </summary>
        /// <param name="shardMap">Storage representation of shard map.</param>
        public virtual void DeleteShardMap(IStoreShardMap shardMap)
        {
            using (WriteLockScope wls = _cacheRoot.GetWriteLockScope())
            {
                _cacheRoot.Remove(shardMap);

                Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManager,
                    "OnDeleteShardMap",
                    "Cache delete complete. ShardMap: {0}",
                    shardMap.Name);
            }
        }

        /// <summary>
        /// Looks up a given shard map in cache based on it's name.
        /// </summary>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <returns>The shard being searched.</returns>
        public virtual IStoreShardMap LookupShardMapByName(string shardMapName)
        {
            IStoreShardMap shardMap;

            using (ReadLockScope rls = _cacheRoot.GetReadLockScope(false))
            {
                // Typical scenario will result in immediate lookup succeeding.
                _cacheRoot.LookupByName(shardMapName, out shardMap);
            }

            Tracer.TraceVerbose(
                TraceSourceConstants.ComponentNames.ShardMapManager,
                "LookupShardMapByNameInCache",
                "Cache {0}; ShardMap: {1}",
                shardMap == null ? "miss" : "hit",
                shardMapName);

            return shardMap;
        }

        /// <summary>
        /// Invoked for refreshing mapping in cache from store.
        /// </summary>
        /// <param name="mapping">Storage representation of mapping.</param>
        /// <param name="policy">Policy to use for preexisting cache entries during update.</param>
        public virtual void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
        {
            using (ReadLockScope rls = _cacheRoot.GetReadLockScope(false))
            {
                CacheShardMap csm = _cacheRoot.LookupById(mapping.ShardMapId);

                if (csm != null)
                {
                    using (WriteLockScope wlscsm = csm.GetWriteLockScope())
                    {
                        csm.Mapper.AddOrUpdate(mapping, policy);

                        // Update perf counters for add or update operation and mappings count.
                        csm.IncrementPerformanceCounter(PerformanceCounterName.MappingsAddOrUpdatePerSec);
                        csm.SetPerformanceCounter(PerformanceCounterName.MappingsCount, csm.Mapper.GetMappingsCount());

                        Tracer.TraceVerbose(
                            TraceSourceConstants.ComponentNames.ShardMapManager,
                            "OnAddOrUpdateMapping",
                            "Cache Add/Update mapping complete. Mapping Id: {0}",
                            mapping.Id);
                    }
                }
            }
        }

        /// <summary>
        /// Invoked for deleting mapping in cache becase it no longer exists in store.
        /// </summary>
        /// <param name="mapping">Storage representation of mapping.</param>
        public virtual void DeleteMapping(IStoreMapping mapping)
        {
            using (ReadLockScope rls = _cacheRoot.GetReadLockScope(false))
            {
                CacheShardMap csm = _cacheRoot.LookupById(mapping.ShardMapId);

                if (csm != null)
                {
                    using (WriteLockScope wlscsm = csm.GetWriteLockScope())
                    {
                        csm.Mapper.Remove(mapping);

                        // Update perf counters for remove mapping operation and mappings count.
                        csm.IncrementPerformanceCounter(PerformanceCounterName.MappingsRemovePerSec);
                        csm.SetPerformanceCounter(PerformanceCounterName.MappingsCount, csm.Mapper.GetMappingsCount());

                        Tracer.TraceVerbose(
                            TraceSourceConstants.ComponentNames.ShardMapManager,
                            "OnDeleteMapping",
                            "Cache delete mapping complete. Mapping Id: {0}",
                            mapping.Id);
                    }
                }
            }
        }

        /// <summary>
        /// Looks up a given key in given shard map.
        /// </summary>
        /// <param name="shardMap">Storage representation of shard map.</param>
        /// <param name="key">Key value.</param>
        /// <returns>Mapping corresponding to <paramref name="key"/> or null.</returns>
        public virtual ICacheStoreMapping LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
        {
            ICacheStoreMapping sm = null;

            using (ReadLockScope rls = _cacheRoot.GetReadLockScope(false))
            {
                CacheShardMap csm = _cacheRoot.LookupById(shardMap.Id);

                if (csm != null)
                {
                    using (ReadLockScope rlsShardMap = csm.GetReadLockScope(false))
                    {
                        IStoreMapping smDummy;
                        sm = csm.Mapper.LookupByKey(key, out smDummy);

                        // perf counter can not be updated in csm.Mapper.LookupByKey() as this function is also called from csm.Mapper.AddOrUpdate()
                        // so updating perf counter value here instead.
                        csm.IncrementPerformanceCounter(sm == null ? PerformanceCounterName.MappingsLookupFailedPerSec : PerformanceCounterName.MappingsLookupSucceededPerSec);
                    }
                }
            }

            return sm;
        }

        /// <summary>
        /// Invoked for updating specified performance counter for a cached shard map object.
        /// </summary>
        /// <param name="shardMap">Storage representation of a shard map.</param>
        /// <param name="name">Performance counter to increment.</param>
        public void IncrementPerformanceCounter(IStoreShardMap shardMap, PerformanceCounterName name)
        {
            using (ReadLockScope rls = _cacheRoot.GetReadLockScope(false))
            {
                CacheShardMap csm = _cacheRoot.LookupById(shardMap.Id);

                if (csm != null)
                {
                    using (ReadLockScope rlsShardMap = csm.GetReadLockScope(false))
                    {
                        csm.IncrementPerformanceCounter(name);
                    }
                }
            }
        }

        /// <summary>
        /// Clears the cache.
        /// </summary>
        public virtual void Clear()
        {
            using (WriteLockScope wls = _cacheRoot.GetWriteLockScope())
            {
                _cacheRoot.Clear();
            }
        }

        #region IDisposable

        /// <summary>
        /// Public dispose method. 
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Protected vitual member of the dispose pattern.
        /// </summary>
        /// <param name="disposing">Call came from Dispose.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _cacheRoot.Dispose();
            }
        }

        #endregion IDisposable
    }
}
