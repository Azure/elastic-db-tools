﻿// Copyright (c) Microsoft. All rights reserved.
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
        /// <param name="connectionString">Connection string for store.</param>
        /// <param name="secureCredential">Secure credential for store.</param>
        /// <returns>An unopened instance of the store connection.</returns>
        IStoreConnection GetConnection(
            StoreConnectionKind kind,
            string connectionString,
            SqlCredential secureCredential);

        /// <summary>
        /// Constructs a new instance of store connection.
        /// </summary>
        /// <param name="kind">Type of store connection.</param>
        /// <param name="connectionString">Connection string for store.</param>
        /// <returns>An unopened instance of the store connection.</returns>
        IStoreConnection GetConnection(
            StoreConnectionKind kind,
            string connectionString);


        /// <summary>
        /// Constructs a new instance of store connection.
        /// </summary>
        /// <param name="kind">Type of store connection.</param>
        /// <param name="connectionString">Connection string for store.</param>
        /// <param name="accessToken">Secure SQL Access token</param>
        /// <returns>An unopened instance of the store connection.</returns>
        IStoreConnection GetConnection(
            StoreConnectionKind kind,
            string connectionString,
            string accessToken);


        /// <summary>
        /// Constructs a new instance of user connection.
        /// </summary>
        /// <param name="connectionString">Connection string of user.</param>
        /// <param name="secureCredential">Secure credential of user.</param>
        /// <returns>An unopened instance of the user connection.</returns>
        IUserStoreConnection GetUserConnection(
            string connectionString,
            SqlCredential secureCredential);

        /// <summary>
        /// Constructs a new instance of user connection.
        /// </summary>
        /// <param name="connectionString">Connection string of user.</param>
        /// <returns>An unopened instance of the user connection.</returns>
        IUserStoreConnection GetUserConnection(
            string connectionString);

        /// <summary>
        /// Constructs a new instance of user connection.
        /// </summary>
        /// <param name="connectionString">Connection string of user.</param>
        /// <param name="accessToken">Secure SQL Access token</param>
        /// <returns>An unopened instance of the user connection.</returns>
        IUserStoreConnection GetUserConnection(string connectionString, string accessToken);
    }
}
