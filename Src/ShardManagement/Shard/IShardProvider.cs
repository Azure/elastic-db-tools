// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents capabilities to provide a Shard.
    /// </summary>
    internal interface IShardProvider
    {
        /// <summary>
        /// Shard for the ShardProvider object.
        /// </summary>
        Shard ShardInfo
        {
            get;
        }

        /// <summary>
        /// Performs validation that the local representation is as 
        /// up-to-date as the representation on the backing 
        /// data store.
        /// </summary>
        /// <param name="shardMap">Shard map to which the shard provider belongs.</param>
        /// <param name="conn">Connection used for validation.</param>
        void Validate(IStoreShardMap shardMap, SqlConnection conn);

        /// <summary>
        /// Asynchronously performs validation that the local representation is as 
        /// up-to-date as the representation on the backing 
        /// data store.
        /// </summary>
        /// <param name="shardMap">Shard map to which the shard provider belongs.</param>
        /// <param name="conn">Connection used for validation.</param>
        /// <returns>A task to await validation completion</returns>
        Task ValidateAsync(IStoreShardMap shardMap, SqlConnection conn);
    }

    /// <summary>
    /// Represents capabilities to provide a Shard along with an associated value.
    /// </summary>
    /// <typeparam name="TValue">Value type. Examples are primitive types, ranges or shards themselves.</typeparam>
    internal interface IShardProvider<TValue> : IShardProvider
    {
        /// <summary>
        /// Value corresponding to the Shard. Represents traits of the Shard 
        /// object provided by the ShardInfo property.
        /// </summary>
        TValue Value
        {
            get;
        }
    }
}
