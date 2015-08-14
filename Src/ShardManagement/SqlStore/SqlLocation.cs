// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// SQL backed storage representation of a location.
    /// </summary>
    internal sealed class SqlLocation : IStoreLocation
    {
        /// <summary>
        /// Constructs an instance of IStoreLocation using parts of a row from SqlDataReader.
        /// Used for creating the shard location instance.
        /// </summary>
        /// <param name="reader">SqlDataReader whose row has shard information.</param>
        /// <param name="offset">Reader offset for column that begins shard information.</param>
        internal SqlLocation(SqlDataReader reader, int offset)
        {
            this.Location = new ShardLocation(
                reader.GetString(offset + 1),
                reader.GetString(offset + 3),
                (SqlProtocol)reader.GetInt32(offset),
                reader.GetInt32(offset + 2));
        }

        /// <summary>
        /// Data source location.
        /// </summary>
        public ShardLocation Location
        {
            get;
            private set;
        }
    }
}
