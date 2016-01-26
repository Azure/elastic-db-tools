// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    ///  Cached representation of collection of mappings within shard map.
    ///  Derived classes implement either list or range functionality.
    /// </summary>
    internal abstract class CacheMapper
    {
        /// <summary>
        /// Constructs the mapper, notes the key type for lookups.
        /// </summary>
        /// <param name="keyType">Key type.</param>
        internal CacheMapper(ShardKeyType keyType)
        {
            this.KeyType = keyType;
        }

        /// <summary>
        /// Key type, usable by lookups by key in derived classes.
        /// </summary>
        protected ShardKeyType KeyType
        {
            get;
            private set;
        }

        /// <summary>
        /// Add or update a mapping in cache.
        /// </summary>
        /// <param name="sm">Storage mapping object.</param>
        /// <param name="policy">Policy to use for preexisting cache entries during update.</param>
        internal abstract void AddOrUpdate(IStoreMapping sm, CacheStoreMappingUpdatePolicy policy);

        /// <summary>
        /// Remove a mapping object from cache.
        /// </summary>
        /// <param name="sm">Storage maping object.</param>
        internal abstract void Remove(IStoreMapping sm);

        /// <summary>
        /// Looks up a mapping by key.
        /// </summary>
        /// <param name="key">Key value.</param>
        /// <param name="sm">Storage mapping object.</param>
        /// <returns>Mapping object which has the key value.</returns>
        internal abstract ICacheStoreMapping LookupByKey(ShardKey key, out IStoreMapping sm);

        /// <summary>
        /// Gets mappings dictionary size.
        /// </summary>
        /// <returns>Number of mappings cached in the dictionary.</returns>
        internal abstract long GetMappingsCount();

        /// <summary>
        /// Clears all the mappings in the lookup by Id table.
        /// </summary>
        protected abstract void Clear();

        /// <summary>
        /// Given current value of TTL, calculates the next TTL value in milliseconds.
        /// </summary>
        /// <param name="csm">Current cached mapping object.</param>
        /// <returns>New TTL value.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "currentValue")]
        protected static long CalculateNewTimeToLiveMilliseconds(ICacheStoreMapping csm)
        {
            if (csm.TimeToLiveMilliseconds <= 0)
            {
                return 5000;
            }

            // Exponentially increase the time up to a limit of 30 seconds, after which we keep 
            // returning 30 seconds as the TTL for the mapping entry.
            return Math.Min(30000, csm.TimeToLiveMilliseconds * 2);
        }
    }
}
