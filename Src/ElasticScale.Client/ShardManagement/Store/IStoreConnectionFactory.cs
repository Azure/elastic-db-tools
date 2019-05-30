// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    using System.Data.SqlClient;

    /// <summary>
    /// Factory for store connections.
    /// </summary>
    internal interface IStoreConnectionFactory
    {
        /// <summary>
        /// Constructs a new instance of store connection.
        /// </summary>
        /// <param name="kind">Type of store connection.</param>
        /// <param name="connectionInfo">Connection info.</param>
        /// <returns>An unopened instance of the store connection.</returns>
        IStoreConnection GetConnection(
            StoreConnectionKind kind,
            SqlConnectionInfo connectionInfo);

        /// <summary>
        /// Constructs a new instance of user connection.
        /// </summary>
        /// <param name="connectionInfo">Connection info.</param>
        /// <returns>An unopened instance of the user connection.</returns>
        IUserStoreConnection GetUserConnection(
            SqlConnectionInfo connectionInfo);
    }
}
