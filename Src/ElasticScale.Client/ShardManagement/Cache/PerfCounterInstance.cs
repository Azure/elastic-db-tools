// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Shard management performance counter names.
    /// </summary>
    internal enum PerformanceCounterName
    {
        // counters to track cached mappings
        MappingsCount,
        MappingsAddOrUpdatePerSec,
        MappingsRemovePerSec,
        MappingsLookupSucceededPerSec,
        MappingsLookupFailedPerSec,
        // counters to track ShardManagement operations
        DdrOperationsPerSec,
    };

    /// <summary>
    /// Structure holding performance counter creation information
    /// </summary>
    internal struct PerfCounterCreationData
    {
    }

    /// <summary>
    /// Class represenging single instance of a all performance counters in shard management catagory
    /// </summary>
    internal class PerfCounterInstance : IDisposable
    {
        /// <summary>
        /// Initialize perf counter instance based on shard map name
        /// </summary>
        /// <param name="shardMapName"></param>
        public PerfCounterInstance(string shardMapName)
        {
        }

        /// <summary>
        /// Try to increment specified performance counter by 1 for current instance.
        /// </summary>
        /// <param name="counterName">Counter to increment.</param>
        internal void IncrementCounter(PerformanceCounterName counterName)
        {
        }

        /// <summary>
        ///  Try to update performance counter with speficied value.
        /// </summary>
        /// <param name="counterName">Counter to update.</param>
        /// <param name="value">New value.</param>
        internal void SetCounter(PerformanceCounterName counterName, long value)
        {
        }

        /// <summary>
        /// Static method to recreate Shard Management performance counter catagory with given counter list.
        /// </summary>
        internal static void CreatePerformanceCategoryAndCounters()
        {
        }

        /// <summary>
        /// Dispose performance counter instance
        /// </summary>
        public void Dispose()
        {
        }
    }
}
