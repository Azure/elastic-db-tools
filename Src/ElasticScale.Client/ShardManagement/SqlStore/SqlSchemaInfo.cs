// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// SQL backed storage representation of a schema info object.
    /// </summary>
    internal sealed class SqlSchemaInfo : IStoreSchemaInfo
    {
        /// <summary>
        /// Constructs an instance of IStoreSchemaInfo using parts of a row from SqlDataReader.
        /// </summary>
        /// <param name="reader">SqlDataReader whose row has shard information.</param>
        /// <param name="offset">Reader offset for column that begins shard information.</param>
        internal SqlSchemaInfo(SqlDataReader reader, int offset)
        {
            this.Name = reader.GetString(offset);
            this.ShardingSchemaInfo = reader.GetSqlXml(offset + 1);
        }

        /// <summary>
        /// Schema info name.
        /// </summary>
        public string Name
        {
            get;
            private set;
        }

        /// <summary>
        /// Schema info represented in XML.
        /// </summary>
        public SqlXml ShardingSchemaInfo
        {
            get;
            private set;
        }
    }
}
