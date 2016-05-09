// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Encapsulates various constants related to TraceSource
    /// </summary>
    internal static class TraceSourceConstants
    {
        /// <summary>
        /// The TraceSource name for the ShardMapManager library
        /// </summary>
        public const string ShardManagementTraceSource = "ShardManagementTraceSource";

        /// <summary>
        /// Component names to use while tracing
        /// </summary>
        internal static class ComponentNames
        {
            /// <summary>
            /// The ShardMapManagerFactory component name
            /// </summary>
            public const string ShardMapManagerFactory = "ShardMapManagerFactory";

            /// <summary>
            /// The ShardMapManager component name
            /// </summary>
            public const string ShardMapManager = "ShardMapManager";

            /// <summary>
            /// The ShardMap component name
            /// </summary>
            public const string ShardMap = "ShardMap";

            /// <summary>
            /// The ListShardMap component name
            /// </summary>
            public const string ListShardMap = "ListShardMap";

            /// <summary>
            /// The RangeShardMap component name
            /// </summary>
            public const string RangeShardMap = "RangeShardMap";

            /// <summary>
            /// The DefaultShardMapper component name
            /// </summary>
            public const string DefaultShardMapper = "DefaultShardMapper";

            /// <summary>
            /// The BaseShardMapper name
            /// </summary>
            public const string BaseShardMapper = "BaseShardMapper";

            /// <summary>
            /// The ListShardMaper name
            /// </summary>
            public const string ListShardMapper = "ListShardMapper";

            /// <summary>
            /// The RangeShardMapper name
            /// </summary>
            public const string RangeShardMapper = "RangeShardMapper";

            /// <summary>
            /// The SqlStore component name
            /// </summary>
            public const string SqlStore = "SqlStore";

            /// <summary>
            /// The Cache component name
            /// </summary>
            public const string Cache = "Cache";

            /// <summary>
            /// The Shard Component name
            /// </summary>
            public const string Shard = "Shard";

            /// <summary>
            /// The PointMapping component name
            /// </summary>
            public const string PointMapping = "PointMapping";

            /// <summary>
            /// The RangeMapping component name
            /// </summary>
            public const string RangeMapping = "RangeMapping";

            /// <summary>
            /// The performance counter component name
            /// </summary>
            public const string PerfCounter = "PerfCounter";
        }
    }
}
