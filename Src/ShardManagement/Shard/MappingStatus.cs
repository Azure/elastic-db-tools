// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Status of a mapping. 
    /// </summary>
    public enum MappingStatus : int
    {
        /// <summary>
        /// Mapping is Offline.
        /// </summary>
        Offline = 0,

        /// <summary>
        /// Mapping is Online.
        /// </summary>
        Online = 1,
    }
}
