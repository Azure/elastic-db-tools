// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Storage representation of a single shard.
    /// </summary>
    internal interface IStoreShard
    {
        /// <summary>
        /// Shard Id.
        /// </summary>
        Guid Id
        {
            get;
        }

        /// <summary>
        /// Shard version.
        /// </summary>
        Guid Version
        {
            get;
        }

        /// <summary>
        /// Containing shard map's Id.
        /// </summary>
        Guid ShardMapId
        {
            get;
        }

        /// <summary>
        /// Data source location.
        /// </summary>
        ShardLocation Location
        {
            get;
        }

        /// <summary>
        /// Shard status.
        /// </summary>
        int Status
        {
            get;
        }
    }
}
