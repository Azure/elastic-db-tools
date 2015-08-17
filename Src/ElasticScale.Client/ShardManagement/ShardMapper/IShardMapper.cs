// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Allows users to specify operations such as validation, to perform 
    /// on the connection opened by the shard map manager.
    /// </summary>
    [Flags]
    public enum ConnectionOptions
    {
        /// <summary>
        /// No operation will be performed on the opened connection.
        /// </summary>
        None = 0,

        /// <summary>
        /// Validation will be performed on the connection to ensure that
        /// the state of the corresponding mapping has not changed since
        /// the mapping information was last cached at the client.
        /// </summary>
        Validate
    }

    /// <summary>
    /// Container for a collection of keys to shards mappings.
    /// </summary>
    internal interface IShardMapper
    {
    }

    /// <summary>
    /// Container for a collection of keys to shards mappings. 
    /// Can provide connection to a shard given a key.
    /// </summary>
    /// <typeparam name="TKey">Key type.</typeparam>
    internal interface IShardMapper<TKey> : IShardMapper
    {
        /// <summary>
        /// Given a key value, obtains a SqlConnection to the shard in the mapping
        /// that contains the key value.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information, the DataSource and Database are 
        /// obtained from the results of the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>An opened SqlConnection.</returns>
        SqlConnection OpenConnectionForKey(TKey key, string connectionString, ConnectionOptions options = ConnectionOptions.Validate);

        /// <summary>
        /// Given a key value, asynchronously obtains a SqlConnection to the shard in the mapping
        /// that contains the key value.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information, the DataSource and Database are 
        /// obtained from the results of the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>An opened SqlConnection.</returns>
        Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, string connectionString, ConnectionOptions options = ConnectionOptions.Validate);
    }

    /// <summary>
    /// Holder of keys to shards mappings and provides operations over such mappings.
    /// </summary>
    /// <typeparam name="TMapping">Type of individual mapping.</typeparam>
    /// <typeparam name="TValue">Type of values mapped to shards in a mapping.</typeparam>
    /// <typeparam name="TKey">Key type.</typeparam>
    internal interface IShardMapper<TMapping, TValue, TKey> : IShardMapper<TKey> where TMapping : IShardProvider<TValue>
    {
        /// <summary>
        /// Adds a mapping.
        /// </summary>
        /// <param name="mapping">Mapping being added.</param>
        TMapping Add(TMapping mapping);

        /// <summary>
        /// Removes a mapping.
        /// </summary>
        /// <param name="mapping">Mapping being removed.</param>
        /// <param name="lockOwnerId">Lock owner id of the mapping</param>
        void Remove(TMapping mapping, Guid lockOwnerId);

        /// <summary>
        /// Looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="useCache">Whether to use cache for lookups.</param>
        /// <returns>Mapping that contains the key value.</returns>
        TMapping Lookup(TKey key, bool useCache);

        /// <summary>
        /// Tries to looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="useCache">Whether to use cache for lookups.</param>
        /// <param name="mapping">Mapping that contains the key value.</param>
        /// <returns><c>true</c> if mapping is found, <c>false</c> otherwise.</returns>
        bool TryLookup(TKey key, bool useCache, out TMapping mapping);
    }
}
