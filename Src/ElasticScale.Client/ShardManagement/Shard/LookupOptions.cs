// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Specifies where mapping lookup operations will search for mappings.
    /// </summary>
    [Flags]
    public enum LookupOptions : int
    {
        /// <summary>
        /// Default invalid kind of lookup options.
        /// </summary>
        None = 0,

        /// <summary>
        /// Attempt to lookup in the local cache.
        /// </summary>
        /// <remarks>
        /// If LookupInCache and LookupInStore are both specified, the cache will be searched first, then the store.
        /// </remarks>
        LookupInCache = 1 << 0,

        /// <summary>
        /// Attempt to lookup in the global shard map store.
        /// </summary>
        /// <remarks>
        /// If LookupInCache and LookupInStore are both specified, the cache will be searched first, then the store.
        /// </remarks>
        LookupInStore = 1 << 1,
    }
}
