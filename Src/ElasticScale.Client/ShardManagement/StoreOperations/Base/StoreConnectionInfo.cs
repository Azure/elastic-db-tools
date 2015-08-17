// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Provides information regarding LSM connections.
    /// </summary>
    internal class StoreConnectionInfo
    {
        /// <summary>
        /// Optional source shard location.
        /// </summary>
        internal ShardLocation SourceLocation
        {
            get;
            set;
        }

        /// <summary>
        /// Optional target shard location.
        /// </summary>
        internal ShardLocation TargetLocation
        {
            get;
            set;
        }
    }
}
