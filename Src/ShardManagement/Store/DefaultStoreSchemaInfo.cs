// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlTypes;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Used for generating storage representation from client side mapping objects.
    /// </summary>
    internal sealed class DefaultStoreSchemaInfo : IStoreSchemaInfo
    {
        /// <summary>
        /// Constructs the storage representation from client side objects.
        /// </summary>
        /// <param name="name">Schema info name.</param>
        /// <param name="shardingSchemaInfo">Schema info represented in XML.</param>
        internal DefaultStoreSchemaInfo(
            string name,
            SqlXml shardingSchemaInfo)
        {
            this.Name = name;
            this.ShardingSchemaInfo = shardingSchemaInfo;
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