// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Arguments used to create a <see cref="SqlStoreConnection"/> or <see cref="SqlUserStoreConnection"/>.
    /// </summary>
    internal sealed class SqlStoreConnectionInfo
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
        /// Creates an instance of <see cref="SqlStoreConnectionInfo"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="secureCredential">The secure SQL credential.</param>
        internal SqlStoreConnectionInfo(
            string connectionString,
            SqlCredential secureCredential)
        {
            this.ConnectionString = connectionString;
            this.Credential = secureCredential;
        }
    }
}
