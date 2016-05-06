// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    ///  Cached representation of shard map.
    /// </summary>
    internal class CacheShardMap : CacheObject
    {
        /// <summary>
        /// Constructs the cached shard map.
        /// </summary>
        /// <param name="ssm">Storage representation of shard map.</param>
        internal CacheShardMap(IStoreShardMap ssm)
            : base()
        {
            this.StoreShardMap = ssm;

            switch (ssm.MapType)
            {
                case ShardMapType.List:
                    this.Mapper = new CacheListMapper(ssm.KeyType);
                    break;
                case ShardMapType.Range:
                    this.Mapper = new CacheRangeMapper(ssm.KeyType);
                    break;
            }

            this._perfCounters = new PerfCounterInstance(ssm.Name);
        }

        /// <summary>
        /// Storage representation of shard map.
        /// </summary>
        internal IStoreShardMap StoreShardMap
        {
            get;
            private set;
        }

        /// <summary>
        /// Mapper object. Exists only for List/Range/Hash shard maps.
        /// </summary>
        internal CacheMapper Mapper
        {
            get;
            private set;
        }

        /// <summary>
        /// Performance counter instance for this shard map.
        /// </summary>
        private PerfCounterInstance _perfCounters;

        /// <summary>
        /// Transfers the child cache objects to current instance from the source instance.
        /// Useful for mantaining the cache even in case of refreshes to shard map objects.
        /// </summary>
        /// <param name="source">Source cached shard map to copy child objects from.</param>
        internal void TransferStateFrom(CacheShardMap source)
        {
            this.Mapper = source.Mapper;
        }

        /// <summary>
        /// Increment value of performance counter by 1.
        /// </summary>
        /// <param name="name">Name of performance counter to increment.</param>
        internal void IncrementPerformanceCounter(PerformanceCounterName name)
        {
            this._perfCounters.IncrementCounter(name);
        }

        /// <summary>
        /// Set raw value of performance counter.
        /// </summary>
        /// <param name="name">Performance counter to update.</param>
        /// <param name="value">Raw value for the counter.</param>
        /// <remarks>This method is always called from CacheStore inside csm.GetWriteLockScope() so we do not have to 
        /// worry about multithreaded access here.</remarks>
        internal void SetPerformanceCounter(PerformanceCounterName name, long value)
        {
            this._perfCounters.SetCounter(name, value);
        }

        /// <summary>
        /// Protected vitual member of the dispose pattern.
        /// </summary>
        /// <param name="disposing">Call came from Dispose.</param>
        protected override void Dispose(bool disposing)
        {
            this._perfCounters.Dispose();
            base.Dispose(disposing);
        }

        ~CacheShardMap()
        {
            Dispose(false);
        }
    }
}
