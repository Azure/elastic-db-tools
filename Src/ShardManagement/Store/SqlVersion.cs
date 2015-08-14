// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// SQL backed storage representation of shard map manager store version.
    /// </summary>
    internal class SqlVersion : IStoreVersion
    {
        /// <summary>
        /// Constructs an instance of IStoreVersion using parts of a row from SqlDataReader.
        /// </summary>
        /// <param name="reader">SqlDataReader whose row has shard information.</param>
        /// <param name="offset">Reader offset for column that begins shard information.</param>
        internal SqlVersion(SqlDataReader reader, int offset)
        {
            int Major = reader.GetInt32(offset);
            int Minor = (reader.FieldCount > offset + 1) ? reader.GetInt32(offset + 1) : 0;
            this.Version = new Version(Major, Minor);
        }

        /// <summary>
        /// Store version.
        /// </summary>
        public Version Version
        {
            get;
            private set;
        }
    }
}
