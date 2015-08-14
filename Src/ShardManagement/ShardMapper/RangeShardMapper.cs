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
    /// Mapper from a range of keys to their corresponding shards. 
    /// </summary>
    /// <typeparam name="TKey">Key type.</typeparam>
    internal class RangeShardMapper<TKey> : BaseShardMapper, IShardMapper<RangeMapping<TKey>, Range<TKey>, TKey>
    {
        /// <summary>
        /// Range shard mapper, which managers range mappings.
        /// </summary>
        /// <param name="manager">Reference to ShardMapManager.</param>
        /// <param name="sm">Containing shard map.</param>
        internal RangeShardMapper(ShardMapManager manager, ShardMap sm) : base(manager, sm)
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
        public SqlConnection OpenConnectionForKey(
            TKey key,
            string connectionString,
            ConnectionOptions options = ConnectionOptions.Validate)
        {
            return this.OpenConnectionForKey<RangeMapping<TKey>, TKey>(
                key,
                (smm, sm, ssm) => new RangeMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.RangeShardMap,
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
        public async Task<SqlConnection> OpenConnectionForKeyAsync(
            TKey key,
            string connectionString,
            ConnectionOptions options = ConnectionOptions.Validate)
        {
            return await this.OpenConnectionForKeyAsync<RangeMapping<TKey>, TKey>(
                key,
                (smm, sm, ssm) => new RangeMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.RangeShardMap,
                connectionString,
                options).ConfigureAwait(false);
        }

        /// <summary>
        /// Marks the given mapping offline.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>An offline mapping.</returns>
        public RangeMapping<TKey> MarkMappingOffline(RangeMapping<TKey> mapping, Guid lockOwnerId)
        {
            return BaseShardMapper.SetStatus<RangeMapping<TKey>, RangeMappingUpdate, MappingStatus>(
                mapping,
                mapping.Status,
                s => MappingStatus.Offline,
                s => new RangeMappingUpdate() { Status = s },
                this.Update,
                lockOwnerId);
        }

        /// <summary>
        /// Marks the given mapping online.
        /// </summary>
        /// <param name="mapping">Input range mapping.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>An online mapping.</returns>
        public RangeMapping<TKey> MarkMappingOnline(RangeMapping<TKey> mapping, Guid lockOwnerId)
        {
            return BaseShardMapper.SetStatus<RangeMapping<TKey>, RangeMappingUpdate, MappingStatus>(
                mapping,
                mapping.Status,
                s => MappingStatus.Online,
                s => new RangeMappingUpdate() { Status = s },
                this.Update,
                lockOwnerId);
        }

        /// <summary>
        /// Adds a range mapping.
        /// </summary>
        /// <param name="mapping">Mapping being added.</param>
        /// <returns>The added mapping object.</returns>
        public RangeMapping<TKey> Add(RangeMapping<TKey> mapping)
        {
            return this.Add<RangeMapping<TKey>>(
                mapping,
                (smm, sm, ssm) => new RangeMapping<TKey>(smm, sm, ssm));
        }

        /// <summary>
        /// Removes a range mapping.
        /// </summary>
        /// <param name="mapping">Mapping being removed.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        public void Remove(RangeMapping<TKey> mapping, Guid lockOwnerId)
        {
            this.Remove<RangeMapping<TKey>>(
                mapping,
                (smm, sm, ssm) => new RangeMapping<TKey>(smm, sm, ssm),
                lockOwnerId);
        }

        /// <summary>
        /// Looks up the key value and returns the corresponding mapping.
        /// </summary>
        /// <param name="key">Input key value.</param>
        /// <param name="useCache">Whether to use cache for lookups.</param>
        /// <returns>Mapping that contains the key value.</returns>
        public RangeMapping<TKey> Lookup(TKey key, bool useCache)
        {
            RangeMapping<TKey> p = this.Lookup<RangeMapping<TKey>, TKey>(
                key,
                useCache,
                (smm, sm, ssm) => new RangeMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.RangeShardMap);

            if (p == null)
            {
                throw new ShardManagementException(
                    ShardManagementErrorCategory.RangeShardMap,
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
        public bool TryLookup(TKey key, bool useCache, out RangeMapping<TKey> mapping)
        {
            RangeMapping<TKey> p = this.Lookup<RangeMapping<TKey>, TKey>(
                key,
                useCache,
                (smm, sm, ssm) => new RangeMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.RangeShardMap);

            mapping = p;

            return p != null;
        }

        /// <summary>
        /// Gets all the mappings that exist within given range.
        /// </summary>
        /// <param name="range">Optional range value, if null, we cover everything.</param>
        /// <param name="shard">Optional shard parameter, if null, we cover all shards.</param>
        /// <returns>Read-only collection of mappings that overlap with given range.</returns>
        internal IReadOnlyList<RangeMapping<TKey>> GetMappingsForRange(Range<TKey> range, Shard shard)
        {
            return this.GetMappingsForRange<RangeMapping<TKey>, TKey>(
                range,
                shard,
                (smm, sm, ssm) => new RangeMapping<TKey>(smm, sm, ssm),
                ShardManagementErrorCategory.RangeShardMap,
                "RangeMapping");
        }

        /// <summary>
        /// Allows for update to a range mapping with the updates provided in 
        /// the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentMapping">Mapping being updated.</param>
        /// <param name="update">Updated properties of the Shard.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>New instance of mapping with updated information.</returns>
        internal RangeMapping<TKey> Update(RangeMapping<TKey> currentMapping, RangeMappingUpdate update, Guid lockOwnerId)
        {
            return this.Update<RangeMapping<TKey>, RangeMappingUpdate, MappingStatus>(
                currentMapping,
                update,
                (smm, sm, ssm) => new RangeMapping<TKey>(smm, sm, ssm),
                rms => (int)rms,
                i => (MappingStatus)i,
                lockOwnerId);
        }

        /// <summary>
        /// Splits the given mapping into 2 at the given key. The new mappings point to the same shard
        /// as the existing mapping.
        /// </summary>
        /// <param name="existingMapping">Given existing mapping.</param>
        /// <param name="splitAt">Split point.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>Read-only collection of 2 new mappings thus created.</returns>
        internal IReadOnlyList<RangeMapping<TKey>> Split(RangeMapping<TKey> existingMapping, TKey splitAt, Guid lockOwnerId)
        {
            this.EnsureMappingBelongsToShardMap<RangeMapping<TKey>>(
                existingMapping,
                "Split",
                "existingMapping");

            ShardKey shardKey = new ShardKey(ShardKey.ShardKeyTypeFromType(typeof(TKey)), splitAt);

            if (!existingMapping.Range.Contains(shardKey) ||
                 existingMapping.Range.Low == shardKey ||
                 existingMapping.Range.High == shardKey)
            {
                throw new ArgumentOutOfRangeException(
                    "splitAt",
                    Errors._ShardMapping_SplitPointOutOfRange);
            }

            IStoreShard newShard = new DefaultStoreShard(
                                    existingMapping.Shard.StoreShard.Id,
                                    Guid.NewGuid(),
                                    existingMapping.ShardMapId,
                                    existingMapping.Shard.StoreShard.Location,
                                    existingMapping.Shard.StoreShard.Status);

            IStoreMapping mappingToRemove = new DefaultStoreMapping(
                existingMapping.StoreMapping.Id,
                existingMapping.StoreMapping.ShardMapId,
                newShard,
                existingMapping.StoreMapping.MinValue,
                existingMapping.StoreMapping.MaxValue,
                existingMapping.StoreMapping.Status,
                existingMapping.StoreMapping.LockOwnerId);

            IStoreMapping[] mappingsToAdd = new IStoreMapping[2]
            {
                new DefaultStoreMapping(
                    Guid.NewGuid(),
                    newShard.ShardMapId,
                    newShard,
                    existingMapping.Range.Low.RawValue,
                    shardKey.RawValue,
                    (int)existingMapping.Status,
                    lockOwnerId),
                new DefaultStoreMapping(
                    Guid.NewGuid(),
                    newShard.ShardMapId,
                    newShard,
                    shardKey.RawValue,
                    existingMapping.Range.High.RawValue,
                    (int)existingMapping.Status,
                    lockOwnerId)
            };

            using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateReplaceMappingsOperation(
                this.Manager,
                StoreOperationCode.SplitMapping,
                this.ShardMap.StoreShardMap,
                new[] { new Tuple<IStoreMapping, Guid>(mappingToRemove, lockOwnerId) },
                mappingsToAdd.Select(mappingToAdd => new Tuple<IStoreMapping, Guid>(mappingToAdd, lockOwnerId)).ToArray()))
            {
                op.Do();
            }

            return mappingsToAdd
                .Select(m => new RangeMapping<TKey>(this.Manager, this.ShardMap, m))
                .ToList()
                .AsReadOnly();
        }

        /// <summary>
        /// Merges 2 contiguous mappings into a single mapping. Both left and right mappings should point
        /// to the same location and must be contiguous.
        /// </summary>
        /// <param name="left">Left mapping.</param>
        /// <param name="right">Right mapping.</param>
        /// <param name="leftLockOwnerId">Lock owner id of the left mapping</param>
        /// <param name="rightLockOwnerId">Lock owner id of the right mapping</param>
        /// <returns>Mapping that results from the merge operation.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity", Justification = "Follows GSM/LSM interaction archetype")]
        internal RangeMapping<TKey> Merge(RangeMapping<TKey> left, RangeMapping<TKey> right, Guid leftLockOwnerId, Guid rightLockOwnerId)
        {
            this.EnsureMappingBelongsToShardMap<RangeMapping<TKey>>(left, "Merge", "left");
            this.EnsureMappingBelongsToShardMap<RangeMapping<TKey>>(right, "Merge", "right");

            if (!left.Shard.Location.Equals(right.Shard.Location))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMapping_MergeDifferentShards,
                        this.ShardMap.Name,
                        left.Shard.Location,
                        right.Shard.Location),
                    "left");
            }

            if (left.Range.Intersects(right.Range) || left.Range.High != right.Range.Low)
            {
                throw new ArgumentOutOfRangeException(
                    "left",
                    Errors._ShardMapping_MergeNotAdjacent);
            }

            if (left.Status != right.Status)
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMapping_DifferentStatus,
                        this.ShardMap.Name),
                    "left");
            }

            IStoreShard newShard = new DefaultStoreShard(
                                    left.Shard.StoreShard.Id,
                                    Guid.NewGuid(),
                                    left.Shard.StoreShard.ShardMapId,
                                    left.Shard.StoreShard.Location,
                                    left.Shard.StoreShard.Status);

            IStoreMapping mappingToRemoveLeft = new DefaultStoreMapping(
                left.StoreMapping.Id,
                left.StoreMapping.ShardMapId,
                newShard,
                left.StoreMapping.MinValue,
                left.StoreMapping.MaxValue,
                left.StoreMapping.Status,
                left.StoreMapping.LockOwnerId);

            IStoreMapping mappingToRemoveRight = new DefaultStoreMapping(
                right.StoreMapping.Id,
                right.StoreMapping.ShardMapId,
                newShard,
                right.StoreMapping.MinValue,
                right.StoreMapping.MaxValue,
                right.StoreMapping.Status,
                right.StoreMapping.LockOwnerId);

            IStoreMapping mappingToAdd = new DefaultStoreMapping(
                Guid.NewGuid(),
                newShard.ShardMapId,
                newShard,
                left.Range.Low.RawValue,
                right.Range.High.RawValue,
                (int)left.Status,
                leftLockOwnerId);

            using (IStoreOperation op = this.Manager.StoreOperationFactory.CreateReplaceMappingsOperation(
                this.Manager,
                StoreOperationCode.MergeMappings,
                this.ShardMap.StoreShardMap,
                new[]
                {
                    new Tuple<IStoreMapping, Guid> (mappingToRemoveLeft, leftLockOwnerId),
                    new Tuple<IStoreMapping, Guid> (mappingToRemoveRight, rightLockOwnerId)
                },
                new[]
                {
                    new Tuple<IStoreMapping, Guid>(mappingToAdd, leftLockOwnerId)
                }))
            {
                op.Do();
            }

            return new RangeMapping<TKey>(this.Manager, this.ShardMap, mappingToAdd);
        }

        /// <summary>
        /// Gets the lock owner of a mapping.
        /// </summary>
        /// <param name="mapping">The mapping</param>
        /// <returns>Lock owner for the mapping.</returns>
        internal Guid GetLockOwnerForMapping(RangeMapping<TKey> mapping)
        {
            return this.GetLockOwnerForMapping<RangeMapping<TKey>>(mapping, ShardManagementErrorCategory.RangeShardMap);
        }

        /// <summary>
        /// Locks or unlocks a given mapping or all mappings.
        /// </summary>
        /// <param name="mapping">Optional mapping</param>
        /// <param name="lockOwnerId">The lock onwer id</param>
        /// <param name="lockOwnerIdOpType">Operation to perform on this mapping with the given lockOwnerId</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity", Justification = "Follows GSM/LSM interaction archetype")]
        internal void LockOrUnlockMappings(RangeMapping<TKey> mapping, Guid lockOwnerId, LockOwnerIdOpType lockOwnerIdOpType)
        {
            this.LockOrUnlockMappings<RangeMapping<TKey>>(
                mapping,
                lockOwnerId,
                lockOwnerIdOpType,
                ShardManagementErrorCategory.RangeShardMap);
        }
    }
}
