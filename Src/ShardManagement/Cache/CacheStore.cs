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
        private CacheRoot cacheRoot;

        /// <summary>
        /// Constructs an instance of client side cache object.
        /// </summary>
        protected internal CacheStore()
        {
            this.cacheRoot = new CacheRoot();
        }

        /// <summary>
        /// Invoked for refreshing shard map in cache from store.
        /// </summary>
        /// <param name="shardMap">Storage representation of shard map.</param>
        public virtual void AddOrUpdateShardMap(IStoreShardMap shardMap)
        {
            using (WriteLockScope wls = this.cacheRoot.GetWriteLockScope())
            {
                this.cacheRoot.AddOrUpdate(shardMap);

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
            using (WriteLockScope wls = this.cacheRoot.GetWriteLockScope())
            {
                this.cacheRoot.Remove(shardMap);

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

            using (ReadLockScope rls = this.cacheRoot.GetReadLockScope(false))
            {
                // Typical scenario will result in immediate lookup succeeding.
                this.cacheRoot.LookupByName(shardMapName, out shardMap);
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
            using (ReadLockScope rls = this.cacheRoot.GetReadLockScope(false))
            {
                CacheShardMap csm = this.cacheRoot.LookupById(mapping.ShardMapId);

                if (csm != null)
                {
                    using (WriteLockScope wlscsm = csm.GetWriteLockScope())
                    {
                        csm.Mapper.AddOrUpdate(mapping, policy);

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
            using (ReadLockScope rls = this.cacheRoot.GetReadLockScope(false))
            {
                CacheShardMap csm = this.cacheRoot.LookupById(mapping.ShardMapId);

                if (csm != null)
                {
                    using (WriteLockScope wlscsm = csm.GetWriteLockScope())
                    {
                        csm.Mapper.Remove(mapping);
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

            using (ReadLockScope rls = this.cacheRoot.GetReadLockScope(false))
            {
                CacheShardMap csm = this.cacheRoot.LookupById(shardMap.Id);

                if (csm != null)
                {
                    using (ReadLockScope rlsShardMap = csm.GetReadLockScope(false))
                    {
                        IStoreMapping smDummy;
                        sm = csm.Mapper.LookupByKey(key, out smDummy);
                    }
                }
            }

            return sm;
        }

        /// <summary>
        /// Clears the cache.
        /// </summary>
        public virtual void Clear()
        {
            using (WriteLockScope wls = this.cacheRoot.GetWriteLockScope())
            {
                this.cacheRoot.Clear();
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
                this.cacheRoot.Dispose();
            }
        }

        #endregion IDisposable
    }
}
