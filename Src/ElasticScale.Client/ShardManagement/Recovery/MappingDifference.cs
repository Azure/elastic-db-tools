// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Class for mapping differences.
    /// </summary>
    internal class MappingDifference
    {
        internal MappingDifference(
            MappingDifferenceType type,
            MappingLocation location,
            IStoreShardMap shardMap,
            IStoreMapping mappingForShardMap,
            IStoreMapping mappingForShard
            )
        {
            this.Type = type;
            this.Location = location;
            this.ShardMap = shardMap;
            this.MappingForShardMap = mappingForShardMap;
            this.MappingForShard = mappingForShard;
        }

        /// <summary>
        /// Type of mapping difference. Either List or Range.
        /// </summary>
        public MappingDifferenceType Type
        {
            get;
            private set;
        }

        /// <summary>
        /// Location where the mappings that differ exist.
        /// </summary>
        public MappingLocation Location
        {
            get;
            private set;
        }

        /// <summary>
        /// ShardMap which has the consistency violation.
        /// </summary>
        public IStoreShardMap ShardMap
        {
            get;
            private set;
        }

        /// <summary>
        /// Mapping found in shard map.
        /// </summary>
        public IStoreMapping MappingForShardMap
        {
            get;
            private set;
        }

        /// <summary>
        /// Mapping found in shard.
        /// </summary>
        public IStoreMapping MappingForShard
        {
            get;
            private set;
        }
    }
}
