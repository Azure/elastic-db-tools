// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Definition of globally useful constants.
    /// </summary>
    internal class GlobalConstants
    {
        /// <summary>
        /// GSM version of store supported by this library.
        /// </summary>
        internal static Version GsmVersionClient = new Version(1, 2);

        /// <summary>
        /// LSM version of store supported by this library.
        /// </summary>
        internal static Version LsmVersionClient = new Version(1, 2);

        /// <summary>
        /// Default locking timeout value for application locks.
        /// </summary>
        internal const int DefaultLockTimeOut = 60 * 1000;

        /// <summary>
        /// Version information for Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement code
        /// </summary>
        internal static string ShardManagementVersionInfo = ElasticScaleVersionInfo.ProductVersion;

        /// <summary>
        /// Prefix for ShardMapManager in ApplicationName for user connections.
        /// </summary>
        internal static readonly string ShardMapManagerPrefix = "ESC_SMMv" + ShardManagementVersionInfo + "_User";

        /// <summary>
        /// ShardMapManager ApplicationName for Storage connections.
        /// </summary>
        internal static readonly string ShardMapManagerInternalConnectionSuffixGlobal = "ESC_SMMv" + ShardManagementVersionInfo + "_GSM";

        /// <summary>
        /// ShardMapManager ApplicationName for Storage connections.
        /// </summary>
        internal static readonly string ShardMapManagerInternalConnectionSuffixLocal = "ESC_SMMv" + ShardManagementVersionInfo + "_LSM";

        /// <summary>
        /// Maximum length of shard map name.
        /// </summary>
        internal const int MaximumShardMapNameLength = 50;

        /// <summary>
        /// Maximum length of ApplicationName.
        /// </summary>
        internal const int MaximumApplicationNameLength = 128;

        /// <summary>
        /// Maximum size of shard map name.
        /// </summary>
        internal const int MaximumShardMapNameSize = MaximumShardMapNameLength * 2;

        /// <summary>
        /// Maximum length of shard key.
        /// </summary>
        internal const int MaximumShardKeyLength = 128;

        /// <summary>
        /// Maximum size of shard key.
        /// </summary>
        internal const int MaximumShardKeySize = MaximumShardKeyLength;

        /// <summary>
        /// Maximum length for a server.
        /// </summary>
        internal const int MaximumServerLength = 128;

        /// <summary>
        /// Maximum size for a server.
        /// </summary>
        internal const int MaximumServerSize = MaximumServerLength * 2;

        /// <summary>
        /// Maximum length for a database.
        /// </summary>
        internal const int MaximumDatabaseLength = 128;

        /// <summary>
        /// Maximum size for a database.
        /// </summary>
        internal const int MaximumDatabaseSize = MaximumDatabaseLength * 2;
    }
}
