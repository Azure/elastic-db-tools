// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    ///  Cached representation of collection of mappings within shard map.
    ///  The items consist of a single point values.
    /// </summary>
    internal class CacheListMapper : CacheMapper
    {
        /// <summary>
        /// Mappings organized by Key.
        /// </summary>
        private SortedDictionary<ShardKey, CacheMapping> _mappingsByKey;

        /// <summary>
        /// Constructs the mapper, notes the key type for lookups.
        /// </summary>
        /// <param name="keyType">Key type.</param>
        internal CacheListMapper(ShardKeyType keyType)
            : base(keyType)
        {
            _mappingsByKey = new SortedDictionary<ShardKey, CacheMapping>();
        }

        /// <summary>
        /// Add or update a mapping in cache.
        /// </summary>
        /// <param name="sm">Storage mapping object.</param>
        /// <param name="policy">Policy to use for preexisting cache entries during update.</param>
        internal override void AddOrUpdate(IStoreMapping sm, CacheStoreMappingUpdatePolicy policy)
        {
            // Make key out of mapping key.
            ShardKey key = ShardKey.FromRawValue(this.KeyType, sm.MinValue);

            CacheMapping cm;

            // We need to update TTL and update entry if:
            // a) We are in update TTL mode
            // b) Mapping exists and same as the one we already have 
            // c) Entry is beyond the TTL limit
            if (policy == CacheStoreMappingUpdatePolicy.UpdateTimeToLive &&
                _mappingsByKey.TryGetValue(key, out cm) &&
                cm.Mapping.Id == sm.Id /*&&
                TimerUtils.ElapsedMillisecondsSince(cm.CreationTime) >= cm.TimeToLiveMilliseconds */)
            {
                cm = new CacheMapping(sm, CacheMapper.CalculateNewTimeToLiveMilliseconds(cm));
            }
            else
            {
                cm = new CacheMapping(sm);
            }

            // Remove existing entry.
            this.Remove(sm);

            // Add the entry to lookup table by Key.
            _mappingsByKey.Add(key, cm);
        }

        /// <summary>
        /// Remove a mapping object from cache.
        /// </summary>
        /// <param name="sm">Storage maping object.</param>
        internal override void Remove(IStoreMapping sm)
        {
            // Make key value out of mapping key.
            ShardKey key = ShardKey.FromRawValue(this.KeyType, sm.MinValue);

            // Remove existing entry.
            if (_mappingsByKey.ContainsKey(key))
            {
                _mappingsByKey.Remove(key);
            }
        }

        /// <summary>
        /// Looks up a mapping by key.
        /// </summary>
        /// <param name="key">Key value.</param>
        /// <param name="sm">Storage mapping object.</param>
        /// <returns>Mapping object which has the key value.</returns>
        internal override ICacheStoreMapping LookupByKey(ShardKey key, out IStoreMapping sm)
        {
            CacheMapping cm;

            _mappingsByKey.TryGetValue(key, out cm);

            if (cm != null)
            {
                sm = cm.Mapping;
            }
            else
            {
                sm = null;
            }

            return cm;
        }

        /// <summary>
        /// Get number of point mappings cached in this mapper.
        /// </summary>
        /// <returns>Number of cached point mappings.</returns>
        internal override long GetMappingsCount()
        {
            return _mappingsByKey.Count;
        }

        /// <summary>
        /// Clears all the mappings in the lookup by Id table as well
        /// as lookup by key table.
        /// </summary>
        protected override void Clear()
        {
            _mappingsByKey.Clear();
        }
    }
}
