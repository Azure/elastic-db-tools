// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// SQL backed storage representation of a shard.
    /// </summary>
    internal sealed class SqlShard : IStoreShard
    {
        /// <summary>
        /// Constructs an instance of IStoreShard using parts of a row from SqlDataReader.
        /// Used for creating the shard instance for a mapping.
        /// </summary>
        /// <param name="reader">SqlDataReader whose row has shard information.</param>
        /// <param name="offset">Reader offset for column that begins shard information.</param>
        internal SqlShard(SqlDataReader reader, int offset)
        {
            this.Id = reader.GetGuid(offset);
            this.Version = reader.GetGuid(offset + 1);
            this.ShardMapId = reader.GetGuid(offset + 2);
            this.Location = new SqlLocation(reader, offset + 3).Location;
            this.Status = reader.GetInt32(offset + 7);
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
