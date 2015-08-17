// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents updates to a mapping between a <see cref="Range{TKey}"/> of values and the <see cref="Shard"/> that stores its data. 
    /// Also see <see cref="RangeMapping{TKey}"/>.
    /// </summary>
    public sealed class RangeMappingUpdate : BaseMappingUpdate<MappingStatus>
    {
        /// <summary>
        /// Instantiates a new range mapping update object.
        /// </summary>
        public RangeMappingUpdate()
            : base()
        {
        }

        /// <summary>
        /// Detects if the current mapping is being taken offline.
        /// </summary>
        /// <param name="originalStatus">Original status.</param>
        /// <param name="updatedStatus">Updated status.</param>
        /// <returns>Detects in the derived types if the mapping is being taken offline.</returns>
        protected override bool IsBeingTakenOffline(MappingStatus originalStatus, MappingStatus updatedStatus)
        {
            return originalStatus == MappingStatus.Online && updatedStatus == MappingStatus.Offline;
        }
    }
}
