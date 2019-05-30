// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    using System.Data.SqlClient;

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
        /// <param name="connectionInfo">Connection info.</param>
        /// <returns>An unopened instance of the store connection.</returns>
        public virtual IStoreConnection GetConnection(
            StoreConnectionKind kind,
            SqlConnectionInfo connectionInfo)
        {
            return new SqlStoreConnection(kind, connectionInfo);
        }

        /// <summary>
        /// Constructs a new instance of user connection.
        /// </summary>
        /// <param name="connectionInfo">Connection info.</param>
        /// <returns>An unopened instance of the user connection.</returns>
        public virtual IUserStoreConnection GetUserConnection(SqlConnectionInfo connectionInfo)
        {
            return new SqlUserStoreConnection(connectionInfo);
        }
    }
}
