// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Storage representation of a single location.
    /// </summary>
    internal interface IStoreLocation
    {
        /// <summary>
        /// Data source location.
        /// </summary>
        ShardLocation Location
        {
            get;
        }
    }
}
