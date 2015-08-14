// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// SQL backed storage representation of a mapping b/w key ranges and shards.
    /// </summary>
    internal sealed class SqlMapping : IStoreMapping
    {
        /// <summary>
        /// Constructs an instance of IStoreMapping using a row from SqlDataReader.
        /// </summary>
        /// <param name="reader">SqlDataReader whose row has mapping information.</param>
        /// <param name="offset">Reader offset for column that begins mapping information.</param>
        internal SqlMapping(SqlDataReader reader, int offset)
        {
            this.Id = reader.GetGuid(offset);
            this.ShardMapId = reader.GetGuid(offset + 1);
            this.MinValue = SqlUtils.ReadSqlBytes(reader, offset + 2);
            this.MaxValue = SqlUtils.ReadSqlBytes(reader, offset + 3);
            this.Status = reader.GetInt32(offset + 4);
            this.LockOwnerId = reader.GetGuid(offset + 5);
            this.StoreShard = new SqlShard(reader, offset + 6);
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
        /// The lock owner id of this mapping
        /// </summary>
        public Guid LockOwnerId
        {
            get;
            private set;
        }

        /// <summary>
        /// Shard referenced by mapping. Null value means this mapping is local.
        /// </summary>
        public IStoreShard StoreShard
        {
            get;
            private set;
        }
    }
}
