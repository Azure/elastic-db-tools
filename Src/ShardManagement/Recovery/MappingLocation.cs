// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Location where the different mappings exist.
    /// </summary>
    public enum MappingLocation
    {
        /// <summary>
        /// Mapping is present in global store, but absent on the shard.
        /// </summary>
        MappingInShardMapOnly,

        /// <summary>
        /// Mapping is absent in global store, but present on the shard.
        /// </summary>
        MappingInShardOnly,

        /// <summary>
        /// Mapping present at both global store and shard.
        /// </summary>
        MappingInShardMapAndShard
    }
}
