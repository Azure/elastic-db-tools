// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Numeric storage operation result.
    /// Keep these in sync with GSM and LSM stored procs.
    /// </summary>
    internal enum StoreResult
    {
        Failure = 0, // Generic failure. 

        Success = 1,

        MissingParametersForStoredProcedure = 50,
        StoreVersionMismatch = 51,
        ShardPendingOperation = 52,
        UnexpectedStoreError = 53,

        ShardMapExists = 101,
        ShardMapDoesNotExist = 102,
        ShardMapHasShards = 103,

        ShardExists = 201,
        ShardDoesNotExist = 202,
        ShardHasMappings = 203,
        ShardVersionMismatch = 204,
        ShardLocationExists = 205,

        MappingDoesNotExist = 301,
        MappingRangeAlreadyMapped = 302,
        MappingPointAlreadyMapped = 303,
        MappingNotFoundForKey = 304,
        UnableToKillSessions = 305,
        MappingIsNotOffline = 306,
        MappingLockOwnerIdDoesNotMatch = 307,
        MappingIsAlreadyLocked = 308,
        MappingIsOffline = 309,

        SchemaInfoNameDoesNotExist = 401,
        SchemaInfoNameConflict = 402,
    }

    /// <summary>
    /// Representation of storage results from storage API execution.
    /// </summary>
    internal interface IStoreResults
    {
        /// <summary>
        /// Storage operation result.
        /// </summary>
        StoreResult Result
        {
            get;
        }

        /// <summary>
        /// Collection of shard maps.
        /// </summary>
        IEnumerable<IStoreShardMap> StoreShardMaps
        {
            get;
        }

        /// <summary>
        /// Collection of shards.
        /// </summary>
        IEnumerable<IStoreShard> StoreShards
        {
            get;
        }

        /// <summary>
        /// Collection of mappings.
        /// </summary>
        IEnumerable<IStoreMapping> StoreMappings
        {
            get;
        }

        /// <summary>
        /// Collection of locations.
        /// </summary>
        IEnumerable<IStoreLocation> StoreLocations
        {
            get;
        }

        /// <summary>
        /// Collection of operations.
        /// </summary>
        IEnumerable<IStoreLogEntry> StoreOperations
        {
            get;
        }

        /// <summary>
        /// Collection of SchemaInfo objects.
        /// </summary>
        IEnumerable<IStoreSchemaInfo> StoreSchemaInfoCollection
        {
            get;
        }

        /// <summary>
        /// Version of store.
        /// </summary>
        IStoreVersion StoreVersion
        {
            get;
        }
    }
}
