// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Storage representation of shard map manager version
    /// </summary>
    internal interface IStoreVersion
    {
        /// <summary>
        /// Store version information.
        /// </summary>
        Version Version
        {
            get;
        }
    }
}
