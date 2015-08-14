// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// SQL based storage representation of a shard map.
    /// </summary>
    internal class SqlShardMap : IStoreShardMap
    {
        /// <summary>
        /// Constructs an instance of IStoreShardMap using a row from SqlDataReader starting at specified offset.
        /// </summary>
        /// <param name="reader">SqlDataReader whose row has shard map information.</param>
        /// <param name="offset">Reader offset for column that begins shard map information..</param>
        internal SqlShardMap(SqlDataReader reader, int offset)
        {
            this.Id = reader.GetGuid(offset);
            this.Name = reader.GetString(offset + 1);
            this.MapType = (ShardMapType)reader.GetInt32(offset + 2);
            this.KeyType = (ShardKeyType)reader.GetInt32(offset + 3);
        }

        /// <summary>
        /// Shard map's identity.
        /// </summary>
        public Guid Id
        {
            get;
            private set;
        }

        /// <summary>
        /// Shard map name.
        /// </summary>
        public string Name
        {
            get;
            private set;
        }

        /// <summary>
        /// Kind of shard map.
        /// </summary>
        public ShardMapType MapType
        {
            get;
            private set;
        }

        /// <summary>
        /// Key type.
        /// </summary>
        public ShardKeyType KeyType
        {
            get;
            private set;
        }
    }
}
