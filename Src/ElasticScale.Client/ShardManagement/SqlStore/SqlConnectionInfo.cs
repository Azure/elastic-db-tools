// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Arguments used to create a <see cref="SqlConnection"/>.
    /// </summary>
    public  sealed class SqlConnectionInfo
    {
        /// <summary>
        /// Gets the connection string.
        /// </summary>
        /// <remarks>
        /// When creating <see cref="SqlConnection"/>, this value will be used for <see cref="SqlConnection.ConnectionString"/>.
        /// </remarks>
        internal string ConnectionString { get; private set; }

        /// <summary>
        /// Gets the secure SQL Credential.
        /// </summary>
        /// <remarks>
        /// When creating <see cref="SqlConnection"/>, this value will be used for <see cref="SqlConnection.Credential"/>.
        /// </remarks>
        internal SqlCredential Credential { get; private set; }

        /// <summary>
        /// Gets the access token.
        /// </summary>
        /// <remarks>
        /// When creating <see cref="SqlConnection"/>, this value will be used for <see cref="SqlConnection.AccessToken"/>.
        /// </remarks>
        internal string AccessToken { get; private set; }

        /// <summary>
        /// Creates an instance of <see cref="SqlConnectionInfo"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public static SqlConnectionInfo Create(string connectionString)
        {
            return new SqlConnectionInfo(connectionString, null, null);
        }

        /// <summary>
        /// Creates an instance of <see cref="SqlConnectionInfo"/> with a secure credential.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="secureCredential">The secure SQL credential.</param>
        public static SqlConnectionInfo CreateWithCredential(string connectionString, SqlCredential secureCredential)
        {
            return new SqlConnectionInfo(connectionString, secureCredential, null);
        }

        /// <summary>
        /// Creates an instance of <see cref="SqlConnectionInfo"/> with a secure credential.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="accessToken">The access token.</param>
        public static SqlConnectionInfo CreateWithAccessToken(string connectionString, string accessToken)
        {
            return new SqlConnectionInfo(connectionString, null, accessToken);
        }

        /// <summary>
        /// Creates an instance of <see cref="SqlConnectionInfo"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="secureCredential">The secure SQL credential.</param>
        /// <param name="accessToken">The access token.</param>
        internal SqlConnectionInfo(
            string connectionString,
            SqlCredential secureCredential = null,
            string accessToken = null)
        {
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

            this.ConnectionString = connectionString;
            this.Credential = secureCredential;
            this.AccessToken = accessToken;
        }

        internal SqlConnection CreateConnection()
        {
            return new SqlConnection
            {
                ConnectionString = ConnectionString,
                Credential = Credential,
#if NET46 || NETCORE
                AccessToken = AccessToken
#endif
            };
        }

        /// <summary>
        /// Creates an instance of <see cref="SqlConnectionInfo"/> which has an updated connection string.
        /// </summary>
        /// <param name="connectionString">The new connection string</param>
        /// <returns>The new connection info.</returns>
        internal SqlConnectionInfo CloneWithUpdatedConnectionString(string connectionString)
        {
            return new SqlConnectionInfo(
                connectionString,
                this.Credential,
                this.AccessToken);
        }
    }
}
