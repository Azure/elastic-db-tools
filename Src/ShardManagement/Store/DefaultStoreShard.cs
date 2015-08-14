// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Used for generating storage representation from client side shard objects.
    /// </summary>
    internal sealed class DefaultStoreShard : IStoreShard
    {
        /// <summary>
        /// Constructs the storage representation from client side objects.
        /// </summary>
        /// <param name="id">Shard Id.</param>
        /// <param name="version">Shard version.</param>
        /// <param name="shardMapId">Identify of shard map.</param>
        /// <param name="location">Data source location.</param>
        /// <param name="status">Status of the shard.</param>
        internal DefaultStoreShard(
            Guid id,
            Guid version,
            Guid shardMapId,
            ShardLocation location,
            int status)
        {
            this.Id = id;
            this.Version = version;
            this.ShardMapId = shardMapId;
            this.Location = location;
            this.Status = status;
        }

        /// <summary>
        /// Shard Id.
        /// </summary>
        public Guid Id
        {
            get;
            private set;
        }

        /// <summary>
        /// Shard version.
        /// </summary>
        public Guid Version
        {
            get;
            private set;
        }

        /// <summary>
        /// Containing shard map's Id.
        /// </summary>
        public Guid ShardMapId
        {
            get;
            private set;
        }

        /// <summary>
        /// Data source location.
        /// </summary>
        public ShardLocation Location
        {
            get;
            private set;
        }

        /// <summary>
        /// Shard status.
        /// </summary>
        public int Status
        {
            get;
            private set;
        }
    }
}
