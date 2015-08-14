// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Operation codes identifying various store operations.
    /// </summary>
    internal enum StoreOperationCode
    {
        AddShard = 1,
        RemoveShard = 2,
        UpdateShard = 3,

        AddPointMapping = 4,
        RemovePointMapping = 5,
        UpdatePointMapping = 6,
        UpdatePointMappingWithOffline = 7,

        AddRangeMapping = 8,
        RemoveRangeMapping = 9,
        UpdateRangeMapping = 10,
        UpdateRangeMappingWithOffline = 11,

        SplitMapping = 14,
        MergeMappings = 15,

        AttachShard = 16
    }
}
