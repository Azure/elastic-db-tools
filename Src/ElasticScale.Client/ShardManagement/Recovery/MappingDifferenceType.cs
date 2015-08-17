// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Type of mapping difference. Useful for downcasting.
    /// </summary>
    public enum MappingDifferenceType
    {
        /// <summary>
        /// Violation associated with ListShardMap.
        /// </summary>
        List,

        /// <summary>
        /// Violation associated with RangeShardMap.
        /// </summary>
        Range
    }
}
