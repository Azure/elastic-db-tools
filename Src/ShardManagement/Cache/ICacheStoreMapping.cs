// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents a cache entry for a mapping.
    /// </summary>
    internal interface ICacheStoreMapping
    {
        /// <summary>
        /// Store representation of mapping.
        /// </summary>
        IStoreMapping Mapping
        {
            get;
        }

        /// <summary>
        /// Mapping entry creation time.
        /// </summary>
        long CreationTime
        {
            get;
        }

        /// <summary>
        /// Mapping entry expiration time.
        /// </summary>
        long TimeToLiveMilliseconds
        {
            get;
        }

        /// <summary>
        /// Resets the mapping entry expiration time to 0.
        /// </summary>
        void ResetTimeToLive();

        /// <summary>
        /// Whether TimeToLiveMilliseconds have elapsed
        /// since CreationTime
        /// </summary>
        /// <returns></returns>
        bool HasTimeToLiveExpired();
    }
}
