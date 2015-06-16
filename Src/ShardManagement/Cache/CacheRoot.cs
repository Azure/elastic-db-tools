using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    ///  Cached representation of shard map manager.
    /// </summary>
    internal class CacheRoot : CacheObject
    {
        /// <summary>
        /// Contained shard maps. Look up to be done by name.
        /// </summary>
        private SortedDictionary<string, CacheShardMap> shardMapsByName;

        /// <summary>
        /// Contained shard maps. Lookup to be done by Id.
        /// </summary>
        private SortedDictionary<Guid, CacheShardMap> shardMapsById;

        /// <summary>
        /// Constructs the cached shard map manager.
        /// </summary>
        internal CacheRoot() : base()
        {
            this.shardMapsByName = new SortedDictionary<string, CacheShardMap>(StringComparer.OrdinalIgnoreCase);
            this.shardMapsById = new SortedDictionary<Guid, CacheShardMap>();
        }

        /// <summary>
        /// Adds a shard map to the cache given storage representation.
        /// </summary>
        /// <param name="ssm">Storage representation of shard map.</param>
        /// <returns>Cached shard map object.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification="The CacheShardMap objects are disposed in the Remove/Clear methods")]
        internal CacheShardMap AddOrUpdate(IStoreShardMap ssm)
        {
            CacheShardMap csm = new CacheShardMap(ssm);
            CacheShardMap csmOldByName = null;
            CacheShardMap csmOldById = null;

            if (this.shardMapsByName.TryGetValue(ssm.Name, out csmOldByName))
            {
                this.shardMapsByName.Remove(ssm.Name);
            }

            if (this.shardMapsById.TryGetValue(ssm.Id, out csmOldById))
            {
                this.shardMapsById.Remove(ssm.Id);
            }

            // Both should be found or none should be found.
            Debug.Assert((csmOldByName == null && csmOldById == null) ||
                         (csmOldByName != null && csmOldById != null));

            // Both should point to same cached copy.
            Debug.Assert(Object.ReferenceEquals(csmOldByName, csmOldById));

            if (csmOldByName != null)
            {
                csm.TransferStateFrom(csmOldByName);

                // Dispose off the old cached shard map
                csmOldByName.Dispose();
            }

            this.shardMapsByName.Add(ssm.Name, csm);

            this.shardMapsById.Add(ssm.Id, csm);
            return csm;
        }

        /// <summary>
        /// Removes shard map from cache given the name.
        /// </summary>
        /// <param name="ssm">Storage representation of shard map.</param>
        internal void Remove(IStoreShardMap ssm)
        {
            if (this.shardMapsByName.ContainsKey(ssm.Name))
            {
                CacheShardMap csm = this.shardMapsByName[ssm.Name];
                this.shardMapsByName.Remove(ssm.Name);

                // Dispose off the cached map
                if (csm != null)
                {
                    csm.Dispose();
                }
            }

            if (this.shardMapsById.ContainsKey(ssm.Id))
            {
                CacheShardMap csm = this.shardMapsById[ssm.Id];
                this.shardMapsById.Remove(ssm.Id);

                // Dispose off the cached map
                if (csm != null)
                {
                    csm.Dispose();
                }
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
            CacheShardMap csm;

            this.shardMapsByName.TryGetValue(name, out csm);

            if (csm != null)
            {
                shardMap = csm.StoreShardMap;
            }
            else
            {
                shardMap = null;
            }

            return csm;
        }

        /// <summary>
        /// Finds shard map in cache given the name.
        /// </summary>
        /// <param name="shardMapId">Id of shard map.</param>
        /// <returns>Cached shard map object.</returns>
        internal CacheShardMap LookupById(Guid shardMapId)
        {
            CacheShardMap csm;

            this.shardMapsById.TryGetValue(shardMapId, out csm);

            return csm;
        }

        /// <summary>
        /// Clears the cache of shard maps.
        /// </summary>
        internal void Clear()
        {
            foreach (KeyValuePair<string, CacheShardMap> kvp in shardMapsByName)
            {
                if (kvp.Value != null)
                {
                    kvp.Value.Dispose();
                }
            }

            shardMapsByName.Clear();

            foreach (KeyValuePair<Guid, CacheShardMap> kvp in shardMapsById)
            {
                if (kvp.Value != null)
                {
                    kvp.Value.Dispose();
                }
            }

            shardMapsById.Clear();
        }
    }
}
