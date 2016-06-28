// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Mapper from single keys (points) to their corresponding shards.
    /// </summary>
    /// <typeparam name="TKey">Key type.</typeparam>
    internal sealed class ListShardMapper<TKey> : BaseShardMapper, IShardMapper<PointMapping<TKey>, TKey, TKey>
    {
        /// <summary>
        /// List shard mapper, which managers point mappings.
        /// </summary>
        /// <param name="manager">Reference to ShardMapManager.</param>
        /// <param name="sm">Containing shard map.</param>
        internal ListShardMapper(ShardMapManager manager, ShardMap sm) : base(manager, sm)
        {
        }

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
        public SqlConnection OpenConnectionForKey(TKey key, string connectionString, ConnectionOptions options = ConnectionOptions.Validate)
        {
            return this.OpenConnectionForKey<PointMapping<TKey>, TKey>(
                key,
                (smm, sm, ssm) => new PointMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.ListShardMap,
                connectionString,
                options);
        }

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
        /// <returns>A Task encapsulating an opened SqlConnection.</returns>
        /// <remarks>All non usage-error exceptions will be reported via the returned Task</remarks>
        public Task<SqlConnection> OpenConnectionForKeyAsync(TKey key, string connectionString, ConnectionOptions options = ConnectionOptions.Validate)
        {
            return this.OpenConnectionForKeyAsync<PointMapping<TKey>, TKey>(
                key,
                (smm, sm, ssm) => new PointMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.ListShardMap,
                connectionString,
                options);
        }

        /// <summary>
        /// Marks the given mapping offline.
        /// </summary>
        /// <param name="mapping">Input point mapping.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>An offline mapping.</returns>
        public PointMapping<TKey> MarkMappingOffline(PointMapping<TKey> mapping, Guid lockOwnerId = default(Guid))
        {
            return BaseShardMapper.SetStatus<PointMapping<TKey>, PointMappingUpdate, MappingStatus>(
                mapping,
                mapping.Status,
                s => MappingStatus.Offline,
                s => new PointMappingUpdate() { Status = s },
                this.Update,
                lockOwnerId);
        }

        /// <summary>
        /// Marks the given mapping online.
        /// </summary>
        /// <param name="mapping">Input point mapping.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>An online mapping.</returns>
        public PointMapping<TKey> MarkMappingOnline(PointMapping<TKey> mapping, Guid lockOwnerId = default(Guid))
        {
            return BaseShardMapper.SetStatus<PointMapping<TKey>, PointMappingUpdate, MappingStatus>(
                mapping,
                mapping.Status,
                s => MappingStatus.Online,
                s => new PointMappingUpdate() { Status = s },
                this.Update,
                lockOwnerId);
        }

        /// <summary>
        /// Adds a point mapping.
        /// </summary>
        /// <param name="mapping">Mapping being added.</param>
        /// <returns>The added mapping object.</returns>
        public PointMapping<TKey> Add(PointMapping<TKey> mapping)
        {
            return this.Add<PointMapping<TKey>>(
                mapping,
                (smm, sm, ssm) => new PointMapping<TKey>(smm, sm, ssm));
        }

        /// <summary>
        /// Removes a point mapping.
        /// </summary>
        /// <param name="mapping">Mapping being removed.</param>
        /// <param name="lockOwnerId">Lock owner id of the mapping</param>
        public void Remove(PointMapping<TKey> mapping, Guid lockOwnerId = default(Guid))
        {
            this.Remove<PointMapping<TKey>>(
                mapping,
                (smm, sm, ssm) => new PointMapping<TKey>(smm, sm, ssm),
                lockOwnerId);
        }

        /// <summary>
        /// Looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="useCache">Whether to use cache for lookups.</param>
        /// <returns>Mapping that contains the key value.</returns>
        public PointMapping<TKey> Lookup(TKey key, bool useCache)
        {
            PointMapping<TKey> p = this.Lookup<PointMapping<TKey>, TKey>(
                key,
                useCache,
                (smm, sm, ssm) => new PointMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.ListShardMap);

            if (p == null)
            {
                throw new ShardManagementException(
                    ShardManagementErrorCategory.ListShardMap,
                    ShardManagementErrorCode.MappingNotFoundForKey,
                    Errors._Store_ShardMapper_MappingNotFoundForKeyGlobal,
                    this.ShardMap.Name,
                    StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal,
                    "Lookup");
            }

            return p;
        }

        /// <summary>
        /// Tries to looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="useCache">Whether to use cache for lookups.</param>
        /// <param name="mapping">Mapping that contains the key value.</param>
        /// <returns><c>true</c> if mapping is found, <c>false</c> otherwise.</returns>
        public bool TryLookup(TKey key, bool useCache, out PointMapping<TKey> mapping)
        {
            PointMapping<TKey> p = this.Lookup<PointMapping<TKey>, TKey>(
                key,
                useCache,
                (smm, sm, ssm) => new PointMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.ListShardMap);

            mapping = p;

            return p != null;
        }

        /// <summary>
        /// Gets all the mappings that exist within given range.
        /// </summary>
        /// <param name="range">Optional range value, if null, we cover everything.</param>
        /// <param name="shard">Optional shard parameter, if null, we cover all shards.</param>
        /// <returns>Read-only collection of mappings that overlap with given range.</returns>
        public IReadOnlyList<PointMapping<TKey>> GetMappingsForRange(Range<TKey> range, Shard shard)
        {
            return this.GetMappingsForRange<PointMapping<TKey>, TKey>(
                range,
                shard,
                (smm, sm, ssm) => new PointMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.ListShardMap,
                "PointMapping");
        }

        /// <summary>
        /// Allows for update to a point mapping with the updates provided in 
        /// the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentMapping">Mapping being updated.</param>
        /// <param name="update">Updated properties of the Shard.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>New instance of mapping with updated information.</returns>
        internal PointMapping<TKey> Update(PointMapping<TKey> currentMapping, PointMappingUpdate update, Guid lockOwnerId = default(Guid))
        {
            return this.Update<PointMapping<TKey>, PointMappingUpdate, MappingStatus>(
                currentMapping,
                update,
                (smm, sm, ssm) => new PointMapping<TKey>(smm, sm, ssm),
                pms => (int)pms,
                i => (MappingStatus)i,
                lockOwnerId);
        }

        /// <summary>
        /// Gets the lock owner of a mapping.
        /// </summary>
        /// <param name="mapping">The mapping</param>
        /// <returns>Lock owner for the mapping.</returns>
        internal Guid GetLockOwnerForMapping(PointMapping<TKey> mapping)
        {
            return this.GetLockOwnerForMapping<PointMapping<TKey>>(mapping, ShardManagementErrorCategory.ListShardMap);
        }

        /// <summary>
        /// Locks or unlocks a given mapping or all mappings.
        /// </summary>
        /// <param name="mapping">Optional mapping</param>
        /// <param name="lockOwnerId">The lock onwer id</param>
        /// <param name="lockOwnerIdOpType">Operation to perform on this mapping with the given lockOwnerId</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity", Justification = "Follows GSM/LSM interaction archetype")]
        internal void LockOrUnlockMappings(PointMapping<TKey> mapping, Guid lockOwnerId, LockOwnerIdOpType lockOwnerIdOpType)
        {
            this.LockOrUnlockMappings<PointMapping<TKey>>(
                mapping,
                lockOwnerId,
                lockOwnerIdOpType,
                ShardManagementErrorCategory.ListShardMap);
        }
    }
}
