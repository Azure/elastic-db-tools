// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Resolution strategy for resolving mapping differences.
    /// </summary>
    public enum MappingDifferenceResolution
    {
        /// <summary>
        /// Ignore the difference for now.
        /// </summary>
        Ignore,

        /// <summary>
        /// Use the mapping present in shard map.
        /// </summary>
        KeepShardMapMapping,

        /// <summary>
        /// Use the mapping in the shard.
        /// </summary>
        KeepShardMapping
    }
}
