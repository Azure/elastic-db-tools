// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    ///  Cached representation of a single mapping.
    /// </summary>
    internal class CacheMapping : ICacheStoreMapping
    {
        /// <summary>
        /// Time to live for the cache entry.
        /// </summary>
        private long _timeToLiveMilliseconds;

        /// <summary>
        /// Constructs cached representation of a mapping object.
        /// </summary>
        /// <param name="storeMapping">Storage representation of mapping.</param>
        internal CacheMapping(IStoreMapping storeMapping)
            : this(storeMapping, 0)
        {
        }

        /// <summary>
        /// Constructs cached representation of a mapping object.
        /// </summary>
        /// <param name="storeMapping">Storage representation of mapping.</param>
        /// <param name="timeToLiveMilliseconds">Mapping expiration time.</param>
        internal CacheMapping(IStoreMapping storeMapping, long timeToLiveMilliseconds)
        {
            this.Mapping = storeMapping;
            this.CreationTime = TimerUtils.GetTimestamp();
            this.TimeToLiveMilliseconds = timeToLiveMilliseconds;
        }

        /// <summary>
        /// Storage representation of the mapping.
        /// </summary>
        public IStoreMapping Mapping
        {
            get;
            private set;
        }

        /// <summary>
        /// Mapping entry creation time.
        /// </summary>
        public long CreationTime
        {
            get;
            private set;
        }

        /// <summary>
        /// Mapping entry expiration time.
        /// </summary>
        public long TimeToLiveMilliseconds
        {
            get
            {
                return _timeToLiveMilliseconds;
            }

            private set
            {
                _timeToLiveMilliseconds = value;
            }
        }

        /// <summary>
        /// Resets the mapping entry expiration time to 0.
        /// </summary>
        public void ResetTimeToLive()
        {
            Interlocked.CompareExchange(ref _timeToLiveMilliseconds, 0L, _timeToLiveMilliseconds);
        }

        /// <summary>
        /// Whether TimeToLiveMilliseconds have elapsed
        /// since the CreationTime
        /// </summary>
        /// <returns>True if they have</returns>
        public bool HasTimeToLiveExpired()
        {
            return TimerUtils.ElapsedMillisecondsSince(this.CreationTime) >= this.TimeToLiveMilliseconds;
        }
    }
}
