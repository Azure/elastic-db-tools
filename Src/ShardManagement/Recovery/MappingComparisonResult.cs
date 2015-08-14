// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Result of comparison b/w the given range mappings.
    /// </summary>
    internal class MappingComparisonResult
    {
        /// <summary>
        /// Instantiates a new instance of range mapping comparison result.
        /// </summary>
        /// <param name="ssm">Store representation of shard map.</param>
        /// <param name="range">Range being considered.</param>
        /// <param name="mappingLocation">Location of mapping.</param>
        /// <param name="gsmMapping">Storage representation of GSM mapping.</param>
        /// <param name="lsmMapping">Storange representation of LSM mapping.</param>
        internal MappingComparisonResult(
            IStoreShardMap ssm,
            ShardRange range,
            MappingLocation mappingLocation,
            IStoreMapping gsmMapping,
            IStoreMapping lsmMapping)
        {
            this.ShardMap = ssm;
            this.Range = range;
            this.MappingLocation = mappingLocation;
            this.ShardMapManagerMapping = gsmMapping;
            this.ShardMapping = lsmMapping;
        }

        /// <summary>
        /// Shard map to which mappings belong.
        /// </summary>
        internal IStoreShardMap ShardMap
        {
            get;
            private set;
        }

        /// <summary>
        /// Current range.
        /// </summary>
        internal ShardRange Range
        {
            get;
            private set;
        }

        /// <summary>
        /// Location of the mapping.
        /// </summary>
        internal MappingLocation MappingLocation
        {
            get;
            private set;
        }

        /// <summary>
        /// Mappings corresponding to current range in GSM.
        /// </summary>
        internal IStoreMapping ShardMapManagerMapping
        {
            get;
            private set;
        }

        /// <summary>
        /// Mapping corresponding to current range in LSM.
        /// </summary>
        internal IStoreMapping ShardMapping
        {
            get;
            private set;
        }
    }
}
