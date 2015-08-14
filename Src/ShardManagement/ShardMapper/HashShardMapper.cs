// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
#if FUTUREWORK
    /// <summary>
    /// Mapper that maps ranges of hashed key values to shards.
    /// </summary>
    /// <typeparam name="T">Key type.</typeparam>
    /// <typeparam name="U">Hashed key type.</typeparam>
    internal sealed class HashShardMapper<T, U> : RangeShardMapper<T, U>
    {
        /// <summary>
        /// Hash shard mapper, which managers hashed ranges.
        /// </summary>
        /// <param name="manager">Reference to ShardMapManager.</param>
        /// <param name="sm">Containing shard map.</param>
        internal HashShardMapper(ShardMapManager manager, ShardMap sm)
            : base(manager, sm)
        {
        }

        /// <summary>
        /// Hash function.
        /// </summary>
        internal Func<T, U> HashFunction
        {
            get;
            set;
        }

        /// <summary>
        /// Function used to perform conversion of key type T to range type U.
        /// </summary>
        /// <param name="key">Input key.</param>
        /// <returns>Mapped value of key.</returns>
        protected override U MapKeyTypeToRangeType(T key)
        {
            Debug.Assert(this.HashFunction != null);
            return this.HashFunction(key);
        }
    }
#endif
}
