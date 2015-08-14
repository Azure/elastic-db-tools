// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Used for generating storage representation from client side mapping objects.
    /// </summary>
    internal sealed class DefaultStoreMapping : IStoreMapping
    {
        /// <summary>
        /// Constructs the storage representation from client side objects.
        /// </summary>
        /// <param name="id">Identify of mapping.</param>
        /// <param name="s">Shard being converted.</param>
        /// <param name="minValue">Min key value.</param>
        /// <param name="maxValue">Max key value.</param>
        /// <param name="status">Mapping status.</param>
        internal DefaultStoreMapping(
            Guid id,
            Shard s,
            byte[] minValue,
            byte[] maxValue,
            int status)
        {
            this.Id = id;
            this.ShardMapId = s.ShardMapId;
            this.MinValue = minValue;
            this.MaxValue = maxValue;
            this.Status = status;
            this.LockOwnerId = default(Guid);

            this.StoreShard = s.StoreShard;
        }

        /// <summary>
        /// Constructs the storage representation from client side objects.
        /// </summary>
        /// <param name="id">Identify of mapping.</param>
        /// <param name="shardMapId">Id of parent shardmap.</param>
        /// <param name="storeShard">IStoreShard</param>
        /// <param name="minValue">Min key value.</param>
        /// <param name="maxValue">Max key value.</param>
        /// <param name="status">Mapping status.</param>
        /// <param name="lockOwnerId">Lock owner id.</param>
        internal DefaultStoreMapping(
            Guid id,
            Guid shardMapId,
            IStoreShard storeShard,
            byte[] minValue,
            byte[] maxValue,
            int status,
            Guid lockOwnerId)
        {
            this.Id = id;
            this.ShardMapId = shardMapId;
            this.MinValue = minValue;
            this.MaxValue = maxValue;
            this.Status = status;
            this.LockOwnerId = lockOwnerId;

            this.StoreShard = storeShard;
        }

        /// <summary>
        /// Mapping Id.
        /// </summary>
        public Guid Id
        {
            get;
            private set;
        }

        /// <summary>
        /// Shard map Id.
        /// </summary>
        public Guid ShardMapId
        {
            get;
            private set;
        }

        /// <summary>
        /// Min value.
        /// </summary>
        public byte[] MinValue
        {
            get;
            private set;
        }

        /// <summary>
        /// Max value.
        /// </summary>
        public byte[] MaxValue
        {
            get;
            private set;
        }

        /// <summary>
        /// Mapping status.
        /// </summary>
        public int Status
        {
            get;
            private set;
        }

        /// <summary>
        /// The lock owner id of the mapping
        /// </summary>
        public Guid LockOwnerId
        {
            get;
            private set;
        }

        /// <summary>
        /// Shard referenced by mapping.
        /// </summary>
        public IStoreShard StoreShard
        {
            get;
            private set;
        }
    }
}
