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
        /// <param name="connectionString">Connection string for store.</param>
        /// <returns>An unopened instance of the store connection.</returns>
        public virtual IStoreConnection GetConnection(
            StoreConnectionKind kind,
            string connectionString)
        {
            return this.GetConnection(kind, connectionString, null);
        }

        /// <summary>
        /// Constructs a new instance of store connection.
        /// </summary>
        /// <param name="kind">Type of store connection.</param>
        /// <param name="connectionString">Connection string for store.</param>
        /// <param name="secureCredential">Secure Credential for store.</param>
        /// <returns>An unopened instance of the store connection.</returns>
        public virtual IStoreConnection GetConnection(
            StoreConnectionKind kind,
            string connectionString,
            SqlCredential secureCredential)
        {
            return new SqlStoreConnection(kind, connectionString, secureCredential);
        }

        /// <summary>
        /// Constructs a new instance of user connection.
        /// </summary>
        /// <param name="connectionString">Connection string of user.</param>
        /// <returns>An unopened instance of the user connection.</returns>
        public virtual IUserStoreConnection GetUserConnection(string connectionString)
        {
            return this.GetUserConnection(connectionString, null);
        }

        /// <summary>
        /// Constructs a new instance of user connection.
        /// </summary>
        /// <param name="connectionString">Connection string of user.</param>
        /// <param name="secureCredential">Secure Credential of user</param>
        /// <returns>An unopened instance of the user connection.</returns>
        public virtual IUserStoreConnection GetUserConnection(string connectionString, SqlCredential secureCredential)
        {
            return new SqlUserStoreConnection(connectionString, secureCredential);
        }
    }
}
