// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Utility class for handling SqlOperation errors returned from stored procedures.
    /// </summary>
    internal class StoreOperationErrorHandler
    {
        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMapManager operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="shardMap">Shard map object.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnShardMapManagerErrorGlobal(
            IStoreResults result,
            IStoreShardMap shardMap,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.ShardMapExists:
                    Debug.Assert(shardMap != null);
                    return new ShardManagementException(
                        ShardManagementErrorCategory.ShardMapManager,
                        ShardManagementErrorCode.ShardMapAlreadyExists,
                        Errors._Store_ShardMap_AlreadyExistsGlobal,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.ShardMapHasShards:
                    Debug.Assert(shardMap != null);
                    return new ShardManagementException(
                        ShardManagementErrorCategory.ShardMapManager,
                        ShardManagementErrorCode.ShardMapHasShards,
                        Errors._Store_ShardMap_ContainsShardsGlobal,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                default:
                    return StoreOperationErrorHandler.OnCommonErrorGlobal(
                        result,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMap operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="shardMap">Shard map object.</param>
        /// <param name="shard">Shard object.</param>
        /// <param name="errorCategory">Error category to use for raised errors.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnShardMapErrorGlobal(
            IStoreResults result,
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardManagementErrorCategory errorCategory,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.ShardMapDoesNotExist:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardMapDoesNotExist,
                        Errors._Store_ShardMap_DoesNotExistGlobal,
                        shardMap.Name,
                        storedProcName,
                        operationName,
                        shard != null ? shard.Location.ToString() : "*");

                case StoreResult.ShardExists:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardAlreadyExists,
                        Errors._Store_Shard_AlreadyExistsGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.ShardLocationExists:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardLocationAlreadyExists,
                        Errors._Store_Shard_LocationAlreadyExistsGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.ShardDoesNotExist:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardDoesNotExist,
                        Errors._Store_Shard_DoesNotExistGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.ShardVersionMismatch:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardVersionMismatch,
                        Errors._Store_Shard_VersionMismatchGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.ShardHasMappings:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardHasMappings,
                        Errors._Store_Shard_HasMappingsGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                default:
                    return StoreOperationErrorHandler.OnCommonErrorGlobal(
                        result,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMap operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="shardMap">Shard map object.</param>
        /// <param name="location">Location of LSM operation.</param>
        /// <param name="errorCategory">Error category to use for raised errors.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnShardMapErrorLocal(
            IStoreResults result,
            IStoreShardMap shardMap,
            ShardLocation location,
            ShardManagementErrorCategory errorCategory,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.UnableToKillSessions:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.MappingsKillConnectionFailure,
                        Errors._Store_ShardMapper_UnableToKillSessions,
                        location,
                        shardMap.Name,
                        operationName,
                        storedProcName,
                        location);

                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                case StoreResult.ShardDoesNotExist:
                    // ShardDoesNotExist on local shard map can only occur in Recovery scenario.
                    // For normal UpdateShard operation, we will get this error from GSM operation first.
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Recovery,
                        ShardManagementErrorCode.ShardDoesNotExist,
                        Errors._Store_Validate_ShardDoesNotExist,
                        location,
                        shardMap.Name,
                        operationName,
                        storedProcName);
                default:
                    return StoreOperationErrorHandler.OnCommonErrorLocal(
                        result,
                        location,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMapper operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="shardMap">Shard map object.</param>
        /// <param name="shard">Shard object.</param>
        /// <param name="errorCategory">Error category to use for raised errors.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnShardMapperErrorGlobal(
            IStoreResults result,
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardManagementErrorCategory errorCategory,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.ShardMapDoesNotExist:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardMapDoesNotExist,
                        Errors._Store_ShardMap_DoesNotExistGlobal,
                        shardMap.Name,
                        storedProcName,
                        operationName,
                        shard != null ? shard.Location.ToString() : "*");

                case StoreResult.ShardDoesNotExist:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardDoesNotExist,
                        Errors._Store_Shard_DoesNotExistGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.ShardVersionMismatch:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardVersionMismatch,
                        Errors._Store_Shard_VersionMismatchGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.MappingDoesNotExist:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.MappingDoesNotExist,
                        Errors._Store_ShardMapper_MappingDoesNotExistGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.MappingRangeAlreadyMapped:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.MappingRangeAlreadyMapped,
                        Errors._Store_ShardMapper_MappingPointOrRangeAlreadyMapped,
                        shard.Location,
                        shardMap.Name,
                        "Range",
                        storedProcName,
                        operationName);

                case StoreResult.MappingPointAlreadyMapped:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.MappingPointAlreadyMapped,
                        Errors._Store_ShardMapper_MappingPointOrRangeAlreadyMapped,
                        shard.Location,
                        shardMap.Name,
                        "Point",
                        storedProcName,
                        operationName);

                case StoreResult.MappingNotFoundForKey:
                    Debug.Fail("MappingNotFoundForKey should not be raised during SqlOperation.");
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.MappingNotFoundForKey,
                        Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.MappingIsAlreadyLocked:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.MappingIsAlreadyLocked,
                        Errors._Store_ShardMapper_LockMappingAlreadyLocked,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.MappingLockOwnerIdDoesNotMatch:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch,
                        Errors._Store_ShardMapper_LockOwnerDoesNotMatch,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.MappingIsNotOffline:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.MappingIsNotOffline,
                        Errors._Store_ShardMapper_MappingIsNotOffline,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                default:
                    return StoreOperationErrorHandler.OnCommonErrorGlobal(
                        result,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMapper operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="location">Location of LSM operation.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnShardMapperErrorLocal(
            IStoreResults result,
            ShardLocation location,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                default:
                    return StoreOperationErrorHandler.OnCommonErrorLocal(
                        result,
                        location,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMap operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="shardMap">Shard map object.</param>
        /// <param name="location">Location of LSM operation.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnValidationErrorLocal(
            IStoreResults result,
            IStoreShardMap shardMap,
            ShardLocation location,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.ShardMapDoesNotExist:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.ShardMapDoesNotExist,
                        Errors._Store_Validate_ShardMapDoesNotExist,
                        shardMap.Name,
                        location,
                        operationName,
                        storedProcName);

                case StoreResult.ShardDoesNotExist:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.ShardDoesNotExist,
                        Errors._Store_Validate_ShardDoesNotExist,
                        location,
                        shardMap.Name,
                        operationName,
                        storedProcName);

                case StoreResult.ShardVersionMismatch:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.ShardVersionMismatch,
                        Errors._Store_Validate_ShardVersionMismatch,
                        location,
                        shardMap.Name,
                        operationName,
                        storedProcName);

                case StoreResult.MappingDoesNotExist:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.MappingDoesNotExist,
                        Errors._Store_Validate_MappingDoesNotExist,
                        location,
                        shardMap.Name,
                        operationName,
                        storedProcName);

                case StoreResult.MappingIsOffline:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.MappingIsOffline,
                        Errors._Store_Validate_MappingIsOffline,
                        location,
                        shardMap.Name,
                        operationName,
                        storedProcName);

                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                default:
                    return StoreOperationErrorHandler.OnCommonErrorLocal(
                        result,
                        location,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMapper operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns></returns>
        internal static ShardManagementException OnShardSchemaInfoErrorGlobal(
            IStoreResults result,
            string shardMapName,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.SchemaInfoNameConflict:
                    throw new SchemaInfoException(
                        SchemaInfoErrorCode.SchemaInfoNameConflict,
                        Errors._Store_SchemaInfo_NameConflict,
                        shardMapName);

                case StoreResult.SchemaInfoNameDoesNotExist:
                    throw new SchemaInfoException(
                        SchemaInfoErrorCode.SchemaInfoNameDoesNotExist,
                        Errors._Store_SchemaInfo_NameDoesNotExist,
                        operationName,
                        shardMapName);

                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                default:
                    return StoreOperationErrorHandler.OnCommonErrorGlobal(
                        result,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMap operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="shardMap">Shard map object.</param>
        /// <param name="shard">Shard object.</param>
        /// <param name="errorCategory">Error category to use for raised errors.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnRecoveryErrorGlobal(
            IStoreResults result,
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardManagementErrorCategory errorCategory,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.ShardLocationExists:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardLocationAlreadyExists,
                        Errors._Store_Shard_LocationAlreadyExistsGlobal,
                        shard.Location,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.ShardMapExists:
                    Debug.Assert(shardMap != null);
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardMapAlreadyExists,
                        Errors._Store_ShardMap_AlreadyExistsGlobal,
                        shardMap.Name,
                        storedProcName,
                        operationName);

                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                default:
                    return StoreOperationErrorHandler.OnCommonErrorGlobal(
                        result,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given error code in 
        /// <paramref name="result"/> for ShardMap operations.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="shardMap">Shard map object.</param>
        /// <param name="location">Location of operation.</param>
        /// <param name="errorCategory">Error category to use for raised errors.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnRecoveryErrorLocal(
            IStoreResults result,
            IStoreShardMap shardMap,
            ShardLocation location,
            ShardManagementErrorCategory errorCategory,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.ShardMapDoesNotExist:
                    return new ShardManagementException(
                        errorCategory,
                        ShardManagementErrorCode.ShardMapDoesNotExist,
                        Errors._Store_ShardMap_DoesNotExistLocal,
                        shardMap.Name,
                        location,
                        storedProcName,
                        operationName);

                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                default:
                    return StoreOperationErrorHandler.OnCommonErrorLocal(
                        result,
                        location,
                        operationName,
                        storedProcName);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given common error code 
        /// in <paramref name="result"/>.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        internal static ShardManagementException OnCommonErrorGlobal(
            IStoreResults result,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.StoreVersionMismatch:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.GlobalStoreVersionMismatch,
                        Errors._Store_UnsupportedLibraryVersionGlobal,
                        (result.StoreVersion != null) ? result.StoreVersion.Version.ToString() : "",
                        GlobalConstants.GsmVersionClient,
                        (result.StoreVersion != null) ? (result.StoreVersion.Version > GlobalConstants.GsmVersionClient ? "library" : "store") : "store");

                case StoreResult.MissingParametersForStoredProcedure:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.GlobalStoreOperationInsufficientParameters,
                        Errors._Store_MissingSprocParametersGlobal,
                        operationName,
                        storedProcName);

                default:
                    Debug.Fail("Unexpected error code found.");
                    return new ShardManagementException(
                        ShardManagementErrorCategory.General,
                        ShardManagementErrorCode.UnexpectedError,
                        Errors._Store_UnexpectedErrorGlobal);
            }
        }

        /// <summary>
        /// Returns the proper ShardManagementException corresponding to given common error code 
        /// in <paramref name="result"/>.
        /// </summary>
        /// <param name="result">Operation result object.</param>
        /// <param name="location">Location of LSM.</param>
        /// <param name="operationName">Operation being performed.</param>
        /// <param name="storedProcName">Stored procedure being executed.</param>
        /// <returns>ShardManagementException to be raised.</returns>
        private static ShardManagementException OnCommonErrorLocal(
            IStoreResults result,
            ShardLocation location,
            string operationName,
            string storedProcName)
        {
            switch (result.Result)
            {
                case StoreResult.StoreVersionMismatch:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.LocalStoreVersionMismatch,
                        Errors._Store_UnsupportedLibraryVersionLocal,
                        (result.StoreVersion != null) ? result.StoreVersion.Version.ToString() : "",
                        location,
                        GlobalConstants.LsmVersionClient,
                        (result.StoreVersion != null) ? (result.StoreVersion.Version > GlobalConstants.LsmVersionClient ? "library" : "store") : "store");

                case StoreResult.MissingParametersForStoredProcedure:
                    return new ShardManagementException(
                        ShardManagementErrorCategory.Validation,
                        ShardManagementErrorCode.LocalStoreOperationInsufficientParameters,
                        Errors._Store_MissingSprocParametersLocal,
                        operationName,
                        location,
                        storedProcName);

                default:
                    Debug.Fail("Unexpected error code found.");
                    return new ShardManagementException(
                        ShardManagementErrorCategory.General,
                        ShardManagementErrorCode.UnexpectedError,
                        Errors._Store_UnexpectedErrorLocal,
                        location);
            }
        }

        /// <summary>
        /// Given an operation code, returns the corresponding operation name.
        /// </summary>
        /// <param name="operationCode">Operation code.</param>
        /// <returns>Operation name corresponding to given operation code.</returns>
        internal static string OperationNameFromStoreOperationCode(StoreOperationCode operationCode)
        {
            switch (operationCode)
            {
                case StoreOperationCode.AddShard:
                    return "CreateShard";
                case StoreOperationCode.RemoveShard:
                    return "DeleteShard";
                case StoreOperationCode.UpdateShard:
                    return "UpdateShard";
                case StoreOperationCode.AddPointMapping:
                    return "AddPointMapping";
                case StoreOperationCode.RemovePointMapping:
                    return "RemovePointMapping";
                case StoreOperationCode.UpdatePointMapping:
                    return "UpdatePointMapping";
                case StoreOperationCode.UpdatePointMappingWithOffline:
                    return "UpdatePointMappingMarkOffline";
                case StoreOperationCode.AddRangeMapping:
                    return "AddRangeMapping";
                case StoreOperationCode.RemoveRangeMapping:
                    return "RemoveRangeMapping";
                case StoreOperationCode.UpdateRangeMapping:
                    return "UpdateRangeMapping";
                case StoreOperationCode.UpdateRangeMappingWithOffline:
                    return "UpdateRangeMappingMarkOffline";
                case StoreOperationCode.SplitMapping:
                    return "SplitMapping";
                case StoreOperationCode.MergeMappings:
                    return "MergeMappings";
                case StoreOperationCode.AttachShard:
                    return "AttachShard";
                default:
                    Debug.Fail("Unexpected operation code found.");
                    return String.Empty;
            }
        }
    }
}
