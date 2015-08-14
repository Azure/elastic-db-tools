// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Extension methods on ShardMaps that allow down-casting.
    /// </summary>
    public static class ShardMapExtensions
    {
        /// <summary>
        /// Downcasts to ListShardMap of TKey.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="shardMap">Input shard map.</param>
        /// <returns>ListShardMap representation of this object.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ListShardMap<TKey> AsListShardMap<TKey>(this ShardMap shardMap)
        {
            ExceptionUtils.DisallowNullArgument(shardMap, "shardMap");

            return ShardMapExtensions.AsListShardMap<TKey>(shardMap, true);
        }

        /// <summary>
        /// Downcasts to ListShardMap of TKey.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="shardMap">Input shard map.</param>
        /// <param name="throwOnFailure">Whether to throw exception or return null on failure.</param>
        /// <returns>ListShardMap representation of this object.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        internal static ListShardMap<TKey> AsListShardMap<TKey>(this ShardMap shardMap, bool throwOnFailure)
        {
            Debug.Assert(shardMap != null);
            ListShardMap<TKey> lsm = null;

            if (shardMap.MapType == ShardMapType.List)
            {
                lsm = shardMap as ListShardMap<TKey>;
            }

            if (lsm == null && throwOnFailure)
            {
                throw ShardMapExtensions.GetConversionException<TKey>(shardMap.StoreShardMap, "List");
            }

            return lsm;
        }


        /// <summary>
        /// Downcasts to RangeShardMap of TKey.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="shardMap">Input shard map.</param>
        /// <returns>RangeShardMap representation of this object.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static RangeShardMap<TKey> AsRangeShardMap<TKey>(this ShardMap shardMap)
        {
            ExceptionUtils.DisallowNullArgument(shardMap, "shardMap");

            return ShardMapExtensions.AsRangeShardMap<TKey>(shardMap, true);
        }

        /// <summary>
        /// Downcasts to RangeShardMap of TKey.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="shardMap">Input shard map.</param>
        /// <param name="throwOnFailure">Whether to throw exception or return null on failure.</param>
        /// <returns>RangeShardMap representation of this object.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        internal static RangeShardMap<TKey> AsRangeShardMap<TKey>(this ShardMap shardMap, bool throwOnFailure)
        {
            Debug.Assert(shardMap != null);
            RangeShardMap<TKey> rsm = null;

            if (shardMap.MapType == ShardMapType.Range)
            {
                rsm = shardMap as RangeShardMap<TKey>;
            }


            if (rsm == null && throwOnFailure)
            {
                throw ShardMapExtensions.GetConversionException<TKey>(shardMap.StoreShardMap, "Range");
            }

            return rsm;
        }

        /// <summary>
        /// Raise conversion exception.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="ssm">Shard map whose conversion failed.</param>
        /// <param name="targetKind">Requested type of shard map.</param>
        private static ShardManagementException GetConversionException<TKey>(IStoreShardMap ssm, string targetKind)
        {
            return new ShardManagementException(
                    ShardManagementErrorCategory.ShardMapManager,
                    ShardManagementErrorCode.ShardMapTypeConversionError,
                    Errors._ShardMapExtensions_AsTypedShardMap_ConversionFailure,
                    ssm.Name,
                    targetKind,
                    typeof(TKey).Name,
                    ssm.MapType.ToString(),
                    ssm.KeyType == ShardKeyType.None ? string.Empty : ShardKey.TypeFromShardKeyType(ssm.KeyType).Name);
        }
    }
}
