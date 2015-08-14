// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Records the updated properties for a mapping update object.
    /// </summary>
    [Flags]
    internal enum MappingUpdatedProperties : int
    {
        Status = 1,
        // Only applicable for point and range update.
        Shard = 2,
        All = Status | Shard
    }

    /// <summary>
    /// Common interface for point/range mapping updates.
    /// </summary>
    /// <typeparam name="TStatus">Status type.</typeparam>
    internal interface IMappingUpdate<TStatus>
    {
        /// <summary>
        /// Status property.
        /// </summary>
        TStatus Status
        {
            get;
        }

        /// <summary>
        /// Shard property.
        /// </summary>
        Shard Shard
        {
            get;
        }

        /// <summary>
        /// Checks if any property is set in the given bitmap.
        /// </summary>
        /// <param name="properties">Properties bitmap.</param>
        /// <returns>True if any of the properties is set, false otherwise.</returns>
        bool IsAnyPropertySet(MappingUpdatedProperties properties);

        /// <summary>
        /// Checks if the mapping is being taken offline.
        /// </summary>
        /// <param name="originalStatus">Original status.</param>
        /// <returns>True of the update will take the mapping offline.</returns>
        bool IsMappingBeingTakenOffline(TStatus originalStatus);
    }
}
