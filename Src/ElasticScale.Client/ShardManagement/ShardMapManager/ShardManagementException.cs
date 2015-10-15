// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Globalization;
using System.Runtime.Serialization;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents error codes related to <see cref="ShardMapManager"/> operations.
    /// </summary>
    public enum ShardManagementErrorCode
    {
        /// <summary>
        /// Successful execution.
        /// </summary>
        Success,

        #region ShardMapManagerFactory

        /// <summary>
        /// Store already exists on target shard map manager database.
        /// </summary>
        ShardMapManagerStoreAlreadyExists,

        /// <summary>
        /// Store does not exist on target shard map manager database.
        /// </summary>
        ShardMapManagerStoreDoesNotExist,

        #endregion

        #region ShardMapManager

        /// <summary>
        /// Shardmap with specified name already exists.
        /// </summary>
        ShardMapAlreadyExists,

        /// <summary>
        /// Shardmap with specified name not found.
        /// </summary>
        ShardMapLookupFailure,

        /// <summary>
        /// Shardmap has shards associated with it.
        /// </summary>
        ShardMapHasShards,

        /// <summary>
        /// GSM store version does not match with client library.
        /// </summary>
        GlobalStoreVersionMismatch,

        /// <summary>
        /// LSM store version does not match with client library.
        /// </summary>
        LocalStoreVersionMismatch,

        /// <summary>
        /// All necessary parameters for GSM stored procedure are not supplied.
        /// </summary>
        GlobalStoreOperationInsufficientParameters,

        /// <summary>
        /// All necessary parameters for LSM stored procedure are not supplied.
        /// </summary>
        LocalStoreOperationInsufficientParameters,

        #endregion

        #region ShardMap

        /// <summary>
        /// Conversion of shard map failed.
        /// </summary>
        ShardMapTypeConversionError,

        /// <summary>
        /// Shard has mappings associated with it.
        /// </summary>
        ShardHasMappings,

        /// <summary>
        /// Shard already exists.
        /// </summary>
        ShardAlreadyExists,

        /// <summary>
        /// Shard location already exists.
        /// </summary>
        ShardLocationAlreadyExists,

        /// <summary>
        /// Shard has been updated by concurrent user.
        /// </summary>
        ShardVersionMismatch,

        #endregion

        #region PointMapping

        /// <summary>
        /// Given point is already associated with a mapping.
        /// </summary>
        MappingPointAlreadyMapped,

        #endregion PointMapping

        #region RangeMapping

        /// <summary>
        /// Specified range is already associated with a mapping.
        /// </summary>
        MappingRangeAlreadyMapped,

        #endregion RangeMapping

        #region Common

        /// <summary>
        /// Storage operation failed.
        /// </summary>
        StorageOperationFailure,

        /// <summary>
        /// Shardmap does not exist any more.
        /// </summary>
        ShardMapDoesNotExist,

        /// <summary>
        /// Shard does not exist any more.
        /// </summary>
        ShardDoesNotExist,

        /// <summary>
        /// An application lock could not be acquired.
        /// </summary>
        LockNotAcquired,

        /// <summary>
        /// An application lock cound not be released. 
        /// </summary>
        LockNotReleased,

        /// <summary>
        /// An unexpected error has occurred.
        /// </summary>
        UnexpectedError,

        #endregion Common

        #region Common Mapper

        /// <summary>
        /// Specified mapping no longer exists.
        /// </summary>
        MappingDoesNotExist,

        /// <summary>
        /// Could not locate a mapping corresponding to given key.
        /// </summary>
        MappingNotFoundForKey,

        /// <summary>
        /// Specified mapping is offline.
        /// </summary>
        MappingIsOffline,

        /// <summary>
        /// Could not terminate connections associated with the Specified mapping.
        /// </summary>
        MappingsKillConnectionFailure,

        /// <summary>
        /// Specified mapping is not offline which certain management operations warrant.
        /// </summary>
        MappingIsNotOffline,

        /// <summary>
        /// Specified mapping is locked and the given lock owner id does not match
        /// the owner id in the store
        /// </summary>
        MappingLockOwnerIdDoesNotMatch,

        /// <summary>
        /// Specified mapping has already been locked
        /// </summary>
        MappingIsAlreadyLocked,

        #endregion Common Mapper

        #region Recovery

        /// <summary>
        /// Shard does not have storage structures.
        /// </summary>
        ShardNotValid,

        #endregion
    }

    /// <summary>
    /// Represents error categories related to Shard Management operations.
    /// </summary>
    public enum ShardManagementErrorCategory
    {
        /// <summary>
        /// Shardmap manager factory.
        /// </summary>
        ShardMapManagerFactory,

        /// <summary>
        /// Shardmap manager.
        /// </summary>
        ShardMapManager,

        /// <summary>
        /// Shardmap.
        /// </summary>
        ShardMap,

        /// <summary>
        /// List shard map.
        /// </summary>
        ListShardMap,

        /// <summary>
        /// Range shard map.
        /// </summary>
        RangeShardMap,

        /// <summary>
        /// Version validation.
        /// </summary>
        Validation,

        /// <summary>
        /// Recovery oriented errors.
        /// </summary>
        Recovery,

        /// <summary>
        /// Errors related to Schema Info Collection.
        /// </summary>
        SchemaInfoCollection,

        /// <summary>
        /// General failure category.
        /// </summary>
        General
    }

    /// <summary>
    /// Representation of exceptions that occur during storage operations.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design",
        "CA1032:ImplementStandardExceptionConstructors", Justification =
        "There is no valid default ErrorCategory/ErrorCode that makes sense. This is akin to SqlException"),
        Serializable]
    public sealed class ShardManagementException : Exception
    {
        /// <summary>
        /// Initializes a new instance with a specified error message. 
        /// </summary>
        /// <param name="category">Category of error.</param>
        /// <param name="code">Error code.</param>
        /// <param name="message">Error message.</param>
        internal ShardManagementException(ShardManagementErrorCategory category, ShardManagementErrorCode code, string message)
            : base(message)
        {
            this.ErrorCategory = category;
            this.ErrorCode = code;
        }

        /// <summary>
        /// Initializes a new instance with a specified formatted error message. 
        /// </summary>
        /// <param name="category">Category of error.</param>
        /// <param name="code">Error code.</param>
        /// <param name="format">The format message that describes the error</param>
        /// <param name="args">The arguments to the format string</param>
        internal ShardManagementException(ShardManagementErrorCategory category, ShardManagementErrorCode code, string format, params object[] args)
            : base(string.Format(CultureInfo.InvariantCulture, format, args))
        {
            this.ErrorCategory = category;
            this.ErrorCode = code;
        }

        /// <summary>
        /// Initializes a new instance with a specified error message and a reference to the inner exception 
        /// that is the cause of this exception.
        /// </summary>
        /// <param name="category">Category of error.</param>
        /// <param name="code">Error code.</param>
        /// <param name="message">A message that describes the error</param>
        /// <param name="inner">The exception that is the cause of the current exception</param>
        internal ShardManagementException(ShardManagementErrorCategory category, ShardManagementErrorCode code, string message, Exception inner)
            : base(message, inner)
        {
            this.ErrorCategory = category;
            this.ErrorCode = code;
        }

        /// <summary>
        /// Initializes a new instance with a specified formatted error message and a reference to the 
        /// inner exception that is the cause of this exception. 
        /// </summary>
        /// <param name="category">Category of error.</param>
        /// <param name="code">Error code.</param>
        /// <param name="format">The format message that describes the error</param>
        /// <param name="inner">The exception that is the cause of the current exception</param>
        /// <param name="args">The arguments to the format string</param>
        internal ShardManagementException(ShardManagementErrorCategory category, ShardManagementErrorCode code, string format, Exception inner, params object[] args)
            : base(string.Format(CultureInfo.InvariantCulture, format, args), inner)
        {
            this.ErrorCategory = category;
            this.ErrorCode = code;
        }

        /// <summary>
        /// Initializes a new instance with serialized data.
        /// </summary>
        /// <param name="info">The object that holds the serialized object data</param>
        /// <param name="context">The contextual information about the source or destination</param>
        private ShardManagementException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.ErrorCategory = (ShardManagementErrorCategory)info.GetValue("ErrorCategory", typeof(ShardManagementErrorCategory));
            this.ErrorCode = (ShardManagementErrorCode)info.GetValue("ErrorCode", typeof(ShardManagementErrorCode));
        }

        #region Serialization Support

        /// <summary>
        /// Populates a SerializationInfo with the data needed to serialize the target object.
        /// </summary>
        /// <param name="info">The SerializationInfo to populate with data.</param>
        /// <param name="context">The destination (see StreamingContext) for this serialization.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info != null)
            {
                info.AddValue("ErrorCategory", ErrorCategory);
                info.AddValue("ErrorCode", ErrorCode);
                base.GetObjectData(info, context);
            }
        }

        #endregion Serialization Support

        /// <summary>
        /// Error category.
        /// </summary>
        public ShardManagementErrorCategory ErrorCategory
        {
            get;
            private set;
        }

        /// <summary>
        /// Error code.
        /// </summary>
        public ShardManagementErrorCode ErrorCode
        {
            get;
            private set;
        }
    }
}
