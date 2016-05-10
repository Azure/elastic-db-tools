// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    ///  Cached representation of collection of mappings within shard map.
    ///  The items consist of a ranges of key values.
    /// </summary>
    internal class CacheRangeMapper : CacheMapper
    {
        /// <summary>
        /// Mappings organized by Key Ranges.
        /// </summary>
        private SortedList<ShardRange, CacheMapping> _mappingsByRange;

        /// <summary>
        /// Constructs the mapper, notes the key type for lookups.
        /// </summary>
        /// <param name="keyType">Key type.</param>
        internal CacheRangeMapper(ShardKeyType keyType)
            : base(keyType)
        {
            _mappingsByRange = new SortedList<ShardRange, CacheMapping>(Comparer<ShardRange>.Default);
        }

        /// <summary>
        /// Add or update a mapping in cache.
        /// </summary>
        /// <param name="sm">Storage mapping object.</param>
        /// <param name="policy">Policy to use for preexisting cache entries during update.</param>
        internal override void AddOrUpdate(IStoreMapping sm, CacheStoreMappingUpdatePolicy policy)
        {
            ShardKey min = ShardKey.FromRawValue(this.KeyType, sm.MinValue);

            // Make range out of mapping key ranges.
            ShardRange range = new ShardRange(
                min,
                ShardKey.FromRawValue(this.KeyType, sm.MaxValue));

            CacheMapping cm;
            ICacheStoreMapping csm;

            IStoreMapping smDummy;

            // We need to update TTL and update entry if:
            // a) We are in update TTL mode
            // b) Mapping exists and same as the one we already have 
            // c) Entry is beyond the TTL limit
            if (policy == CacheStoreMappingUpdatePolicy.UpdateTimeToLive &&
                (csm = this.LookupByKey(min, out smDummy)) != null &&
                csm.Mapping.Id == sm.Id /*&&
                TimerUtils.ElapsedMillisecondsSince(csm.CreationTime) >= csm.TimeToLiveMilliseconds */)
            {
                cm = new CacheMapping(sm, CacheMapper.CalculateNewTimeToLiveMilliseconds(csm));
            }
            else
            {
                cm = new CacheMapping(sm);
            }

            this.Remove(sm);

            // Add the entry to lookup table by Range.
            _mappingsByRange.Add(range, cm);
        }

        /// <summary>
        /// Remove a mapping object from cache.
        /// </summary>
        /// <param name="sm">Storage maping object.</param>
        /// <remarks>
        /// Q: Do we ever need to remove multiple entries from the cache which cover the same range?
        /// A: Yes. Imagine that you have some stale mapping in the cache, user just simply performs 
        /// an AddRangeMapping operation on a subset of stale mapping range, now you should remove the 
        /// stale mapping.
        /// </remarks>
        internal override void Remove(IStoreMapping sm)
        {
            ShardKey minKey = ShardKey.FromRawValue(this.KeyType, sm.MinValue);
            ShardKey maxKey = ShardKey.FromRawValue(this.KeyType, sm.MaxValue);

            // Make range out of mapping key.
            ShardRange range = new ShardRange(minKey, maxKey);

            // Fast code path, where cache does contain the exact range.
            if (_mappingsByRange.ContainsKey(range))
            {
                _mappingsByRange.Remove(range);
            }
            else
            {
                int indexMin = this.GetIndexOfMappingWithClosestMinLessThanOrEqualToMinKey(minKey);
                int indexMax = this.GetIndexOfMappingWithClosestMaxGreaterThanOrEqualToMaxKey(maxKey);

                if (indexMin < 0)
                {
                    indexMin = 0;
                }

                if (indexMax >= _mappingsByRange.Keys.Count)
                {
                    indexMax = _mappingsByRange.Keys.Count - 1;
                }

                // Find first range with max greater than min key.
                for (; indexMin <= indexMax; indexMin++)
                {
                    ShardRange currentRange = _mappingsByRange.Keys[indexMin];
                    if (currentRange.High > minKey)
                    {
                        break;
                    }
                }

                // Find first range with min less than or equal to max key.
                for (; indexMax >= indexMin; indexMax--)
                {
                    ShardRange currentRange = _mappingsByRange.Keys[indexMax];
                    if (currentRange.Low <= maxKey)
                    {
                        break;
                    }
                }

                List<ShardRange> rangesToRemove = new List<ShardRange>();

                for (; indexMin <= indexMax; indexMin++)
                {
                    rangesToRemove.Add(_mappingsByRange.Keys[indexMin]);
                }

                foreach (ShardRange rangeToRemove in rangesToRemove)
                {
                    _mappingsByRange.Remove(rangeToRemove);
                }
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

            // Performs a binary search in the ranges for key value and 
            // then return the result.
            int rangeIndex = this.GetIndexOfMappingContainingShardKey(key);

            if (rangeIndex != -1)
            {
                ShardRange range = _mappingsByRange.Keys[rangeIndex];

                cm = _mappingsByRange[range];

                // DEVNOTE(wbasheer): We should clone the mapping.
                sm = cm.Mapping;
            }
            else
            {
                cm = null;
                sm = null;
            }

            return cm;
        }

        /// <summary>
        /// Get number of range mappings cached in this mapper.
        /// </summary>
        /// <returns>Number of cached range mappings.</returns>
        internal override long GetMappingsCount()
        {
            return _mappingsByRange.Count;
        }

        /// <summary>
        /// Clears all the mappings in the lookup by Id table as well
        /// as lookup by range table.
        /// </summary>
        protected override void Clear()
        {
            _mappingsByRange.Clear();
        }

        /// <summary>
        /// Performs binary search on the cached mappings and returns the
        /// index of mapping object which contains the given key.
        /// </summary>
        /// <param name="key">Input key.</param>
        /// <returns>Index of range in the cache which contains the given key.</returns>
        private int GetIndexOfMappingContainingShardKey(ShardKey key)
        {
            IList<ShardRange> rangeKeys = _mappingsByRange.Keys;

            int lb = 0;
            int ub = rangeKeys.Count - 1;

            while (lb <= ub)
            {
                int mid = lb + (ub - lb) / 2;

                ShardRange current = rangeKeys[mid];

                if (current.Contains(key))
                {
                    return mid;
                }
                else if (key < current.Low)
                {
                    ub = mid - 1;
                }
                else
                {
                    lb = mid + 1;
                }
            }

            return -1;
        }

        /// <summary>
        /// Performs binary search on the cached mappings and returns the
        /// index of mapping object whose min-value is less than and closest 
        /// to given key value.
        /// </summary>
        /// <param name="key">Input key.</param>
        /// <returns>Index of range in the cache which contains the given key.</returns>
        private int GetIndexOfMappingWithClosestMinLessThanOrEqualToMinKey(ShardKey key)
        {
            IList<ShardRange> rangeKeys = _mappingsByRange.Keys;

            int lb = 0;
            int ub = rangeKeys.Count - 1;

            while (lb <= ub)
            {
                int mid = lb + (ub - lb) / 2;

                ShardRange current = rangeKeys[mid];

                if (current.Low <= key)
                {
                    if (current.High > key)
                    {
                        return mid;
                    }
                    else
                    {
                        lb = mid + 1;
                    }
                }
                else
                {
                    ub = mid - 1;
                }
            }

            return ub;
        }

        /// <summary>
        /// Performs binary search on the cached mappings and returns the
        /// index of mapping object whose min-value is less than and closest 
        /// to given key value.
        /// </summary>
        /// <param name="key">Input key.</param>
        /// <returns>Index of range in the cache which contains the given key.</returns>
        private int GetIndexOfMappingWithClosestMaxGreaterThanOrEqualToMaxKey(ShardKey key)
        {
            IList<ShardRange> rangeKeys = _mappingsByRange.Keys;

            int lb = 0;
            int ub = rangeKeys.Count - 1;

            while (lb <= ub)
            {
                int mid = lb + (ub - lb) / 2;

                ShardRange current = rangeKeys[mid];

                if (current.High > key)
                {
                    if (current.Low <= key)
                    {
                        return mid;
                    }
                    else
                    {
                        ub = mid - 1;
                    }
                }
                else
                {
                    lb = mid + 1;
                }
            }

            return lb;
        }
    }
}
