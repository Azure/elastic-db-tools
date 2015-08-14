// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Encapsulates extension methods for CacheMappings
    /// </summary>
    public static class CacheMappingExtensions
    {
        /// <summary>
        /// Resets the mapping entry expiration time to 0 if necessary
        /// </summary>
        /// <param name="csm"></param>
        internal static void ResetTimeToLiveIfNecessary(this ICacheStoreMapping csm)
        {
            // Reset TTL on successful connection.
            if (csm != null && csm.TimeToLiveMilliseconds > 0)
            {
                csm.ResetTimeToLive();
            }
        }
    }
}
