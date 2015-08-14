// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Default shard mapper, that basically is a container of shards with no keys.
    /// </summary>
    internal sealed class DefaultShardMapper : BaseShardMapper, IShardMapper<Shard, ShardLocation, Shard>
    {
        /// <summary>
        /// Default shard mapper, which just manages Shards.
        /// </summary>
        /// <param name="manager">Reference to ShardMapManager.</param>
        /// <param name="sm">Containing shard map.</param>
        internal DefaultShardMapper(ShardMapManager manager, ShardMap sm) : base(manager, sm)
        {
        }

        /// <summary>
        /// Given a shard, obtains a SqlConnection to the shard. The shard must exist in the mapper.
        /// </summary>
        /// <param name="key">Input shard.</param>
        /// <param name="connectionString">
        /// Connection string with credential information, the DataSource and Database are 
        /// obtained from the results of the lookup operation.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>An opened SqlConnection.</returns>
        public SqlConnection OpenConnectionForKey(
            Shard key,
            string connectionString,
            ConnectionOptions options = ConnectionOptions.Validate)
        {
            Debug.Assert(key != null);
            Debug.Assert(connectionString != null);

            return this.ShardMap.OpenConnection(this.Lookup(key, true), connectionString, options);
        }

        /// <summary>
        /// Given a shard, asynchronously obtains a SqlConnection to the shard. The shard must exist in the mapper.
        /// </summary>
        /// <param name="key">Input shard.</param>
        /// <param name="connectionString">
        /// Connection string with credential information, the DataSource and Database are 
        /// obtained from the results of the lookup operation.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>An opened SqlConnection.</returns>
        public async Task<SqlConnection> OpenConnectionForKeyAsync(
            Shard key,
            string connectionString,
            ConnectionOptions options = ConnectionOptions.Validate)
        {
            Debug.Assert(key != null);
            Debug.Assert(connectionString != null);

            return await this.ShardMap.OpenConnectionAsync(this.Lookup(key, true), connectionString, options).ConfigureAwait(false);
        }

        /// <summary>
        /// Adds a shard.
        /// </summary>
        /// <param name="shard">Shard being added.</param>
        /// <returns>The added shard object.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public Shard Add(Shard shard)
        {
            Debug.Assert(shard != null);

            ExceptionUtils.EnsureShardBelongsToShardMap(
                this.Manager,
                this.ShardMap,
                shard,
                "CreateShard",
                "Shard");

            using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateAddShardOperation(
                this.Manager,
                this.ShardMap.StoreShardMap,
                shard.StoreShard))
            {
                op.Do();
            }

            return shard;
        }

        /// <summary>
        /// Removes a shard.
        /// </summary>
        /// <param name="shard">Shard being removed.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void Remove(Shard shard, Guid lockOwnerId = default(Guid))
        {
            Debug.Assert(shard != null);

            ExceptionUtils.EnsureShardBelongsToShardMap(
                this.Manager,
                this.ShardMap,
                shard,
                "DeleteShard",
                "Shard");

            using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateRemoveShardOperation(
                this.Manager,
                this.ShardMap.StoreShardMap,
                shard.StoreShard))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Looks up the given shard in the mapper.
        /// </summary>
        /// <param name="shard">Input shard.</param>
        /// <param name="useCache">Whether to use cache for lookups.</param>
        /// <returns>Returns the shard after verifying that it is present in mapper.</returns>
        public Shard Lookup(Shard shard, bool useCache)
        {
            Debug.Assert(shard != null);

            return shard;
        }

        /// <summary>
        /// Tries to looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <param name="key">Input shard.</param>
        /// <param name="useCache">Whether to use cache for lookups.</param>
        /// <param name="shard">Shard that contains the key value.</param>
        /// <returns><c>true</c> if shard is found, <c>false</c> otherwise.</returns>
        public bool TryLookup(Shard key, bool useCache, out Shard shard)
        {
            Debug.Assert(key != null);

            shard = key;

            return true;
        }

        /// <summary>
        /// Gets all shards for a shard map.
        /// </summary>
        /// <returns>All the shards belonging to the shard map.</returns>
        internal IEnumerable<Shard> GetShards()
        {
            IStoreResults result;

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateGetShardsGlobalOperation(
                "GetShards",
                this.Manager,
                this.ShardMap.StoreShardMap))
            {
                result = op.Do();
            }

            return result.StoreShards
                         .Select(ss => new Shard(this.Manager, this.ShardMap, ss));
        }

        /// <summary>
        /// Gets shard object based on given location.
        /// </summary>
        /// <param name="location">Input location.</param>
        /// <returns>Shard belonging to ShardMap.</returns>
        internal Shard GetShardByLocation(ShardLocation location)
        {
            Debug.Assert(location != null);

            IStoreResults result;

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateFindShardByLocationGlobalOperation(
                this.Manager,
                "GetShardByLocation",
                this.ShardMap.StoreShardMap,
                location))
            {
                result = op.Do();
            }

            return result.StoreShards
                         .Select(ss => new Shard(this.Manager, this.ShardMap, ss)).SingleOrDefault();
        }

        /// <summary>
        /// Allows for update to a shard with the updates provided in the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentShard">Shard to be updated.</param>
        /// <param name="update">Updated properties of the Shard.</param>
        /// <returns>New Shard instance with updated information.</returns>
        internal Shard UpdateShard(Shard currentShard, ShardUpdate update)
        {
            Debug.Assert(currentShard != null);
            Debug.Assert(update != null);

            ExceptionUtils.EnsureShardBelongsToShardMap(
                this.Manager,
                this.ShardMap,
                currentShard,
                "UpdateShard",
                "Shard");

            // CONSIDER(wbasheer): Have refresh semantics for trivial case when nothing is modified.
            if (!update.IsAnyPropertySet(ShardUpdatedProperties.All))
            {
                return currentShard;
            }

            DefaultStoreShard sNew = new DefaultStoreShard(
                currentShard.Id,
                Guid.NewGuid(),
                currentShard.ShardMapId,
                currentShard.Location,
                update.IsAnyPropertySet(ShardUpdatedProperties.Status) ? (int)update.Status : currentShard.StoreShard.Status);

            using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateUpdateShardOperation(
                this.Manager,
                this.ShardMap.StoreShardMap,
                currentShard.StoreShard,
                sNew))
            {
                op.Do();
            }

            return new Shard(this.Manager, this.ShardMap, sNew);
        }
    }
}
