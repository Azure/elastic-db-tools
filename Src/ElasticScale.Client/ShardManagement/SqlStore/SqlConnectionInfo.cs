// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.SqlStore
{
    /// <summary>
    /// Instance of a Sql Connection Info.
    /// </summary>
    public sealed class SqlConnectionInfo
    {
        /// <summary>
        /// Input connection string
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// The secure SQL Credential
        /// </summary>
        public SqlCredential SecureCredential { get; set; }

        /// <summary>
        /// The secure Sql Access token
        /// </summary>
        public string AccessToken{ get; set;}

        /// <summary>
        /// The ConnectionInfoConstructor
        /// </summary>
        /// <param name="ConnectionString">
        /// Input connection string
        /// </param>
        /// <param name="SecureCredential">
        /// The secure SQL Credential
        /// </param>
        /// <param name="AccessToken">
        ///  The secure Sql Access token
        /// </param>
        public SqlConnectionInfo(string ConnectionString, SqlCredential SecureCredential, string AccessToken)
        {
            this.ConnectionString = ConnectionString;
            this.SecureCredential = SecureCredential;
            this.AccessToken = AccessToken;
        }
        /// <summary>
        /// The ConnectionInfoConstructor
        /// </summary>
        public SqlConnectionInfo()
        {
        }


     
    }
}
