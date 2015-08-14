// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Utility classes for exception and error handling.
    /// </summary>
    internal static class ExceptionUtils
    {
        /// <summary>
        /// Checks if given argument is null and if it is, throws an <see cref="ArgumentNullException"/>.
        /// </summary>
        /// <typeparam name="T">Type of value.</typeparam>
        /// <param name="value">Value provided.</param>
        /// <param name="argName">Name of argument whose value is provided in <paramref name="value"/>.</param>
        internal static void DisallowNullArgument<T>(T value, string argName)
        {
            if (value == null)
            {
                throw new ArgumentNullException(argName);
            }
        }

        /// <summary>
        /// Checks if given string argument is null or empty and if it is, throws an <see cref="ArgumentException"/>.
        /// </summary>
        /// <param name="s">Input string.</param>
        /// <param name="argName">Name of argument whose value is provided in <paramref name="s"/>.</param>
        internal static void DisallowNullOrEmptyStringArgument(string s, string argName)
        {
            if (String.IsNullOrEmpty(s))
            {
                throw new ArgumentException(argName);
            }
        }

        /// <summary>
        /// Ensures that the shard map and shard map manager information for given
        /// shard matches the one for current shard map.
        /// </summary>
        /// <param name="currentShardMapManager">Current shard map manager.</param>
        /// <param name="currentShardMap">Current shard map.</param>
        /// <param name="shard">Input shard.</param>
        /// <param name="operation">Operation being performed.</param>
        /// <param name="mappingType">Type of mapping.</param>
        internal static void EnsureShardBelongsToShardMap(
            ShardMapManager currentShardMapManager,
            ShardMap currentShardMap,
            Shard shard,
            string operation,
            string mappingType)
        {
            // Ensure that shard is associated with current shard map.
            if (shard.ShardMapId != currentShardMap.Id)
            {
                throw new InvalidOperationException(
                    StringUtils.FormatInvariant(
                        Errors._Shard_DifferentShardMap,
                        shard.Location,
                        mappingType,
                        currentShardMap.Name,
                        operation));
            }

            // Ensure that shard is associated with current shard map manager instance.
            if (shard.Manager != currentShardMapManager)
            {
                throw new InvalidOperationException(
                    StringUtils.FormatInvariant(
                        Errors._Shard_DifferentShardMapManager,
                        shard.Location,
                        mappingType,
                        currentShardMapManager.Credentials.ShardMapManagerLocation,
                        operation));
            }
        }

        /// <summary>
        /// Constructs a global store exception object based on the given input parameters.
        /// </summary>
        /// <param name="category">Error category.</param>
        /// <param name="storeException">Underlying store exception.</param>
        /// <param name="operationName">Operation name.</param>
        /// <returns>
        /// ShardManagementException corresponding to the given store exception.
        /// </returns>
        internal static ShardManagementException GetStoreExceptionGlobal(
            ShardManagementErrorCategory category,
            StoreException storeException,
            string operationName)
        {
            return new ShardManagementException(
                category,
                ShardManagementErrorCode.StorageOperationFailure,
                Errors._Store_SqlExceptionGlobal,
                storeException.InnerException != null ? storeException.InnerException.Message : storeException.Message,
                storeException,
                operationName);
        }

        /// <summary>
        /// Constructs a global store exception object based on the given input parameters.
        /// </summary>
        /// <param name="category">Error category.</param>
        /// <param name="storeException">Underlying store exception.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="location">Location of server where error occurred.</param>
        /// <returns>
        /// ShardManagementException corresponding to the given store exception.
        /// </returns>
        internal static ShardManagementException GetStoreExceptionLocal(
            ShardManagementErrorCategory category,
            StoreException storeException,
            string operationName,
            ShardLocation location)
        {
            return new ShardManagementException(
                category,
                ShardManagementErrorCode.StorageOperationFailure,
                Errors._Store_SqlExceptionLocal,
                storeException.InnerException != null ? storeException.InnerException.Message : storeException.Message,
                storeException,
                operationName,
                location);
        }
    }
}
