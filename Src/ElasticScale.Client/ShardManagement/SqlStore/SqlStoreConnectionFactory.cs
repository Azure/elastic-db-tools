// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Constructs instance of Sql Store Connection.
    /// </summary>
    internal class SqlStoreConnectionFactory : IStoreConnectionFactory
    {
        /// <summary>
        /// Constructs an instance of the factory.
        /// </summary>
        protected internal SqlStoreConnectionFactory()
        {
        }

        /// <summary>
        /// Constructs a new instance of store connection.
        /// </summary>
        /// <param name="kind">Type of store connection.</param>
        /// <param name="connectionString">Connection string for store.</param>
        /// <returns>An unopened instance of the store connection.</returns>
        public virtual IStoreConnection GetConnection(StoreConnectionKind kind, string connectionString)
        {
            return new SqlStoreConnection(kind, connectionString);
        }

        /// <summary>
        /// Constructs a new instance of user connection.
        /// </summary>
        /// <param name="connectionString">Connection string of user.</param>
        /// <returns>An unopened instance of the user connection.</returns>
        public virtual IUserStoreConnection GetUserConnection(string connectionString)
        {
            return new SqlUserStoreConnection(connectionString);
        }
    }
}
