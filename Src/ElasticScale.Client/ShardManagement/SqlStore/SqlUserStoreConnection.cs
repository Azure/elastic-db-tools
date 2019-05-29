// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Instance of a User Sql Store Connection.
    /// </summary>
    internal class SqlUserStoreConnection : IUserStoreConnection
    {
        /// <summary>
        /// Underlying connection.
        /// </summary>
        private SqlConnection _conn;

        internal SqlUserStoreConnection(string connectionString)
        {
            _conn = new SqlConnection { ConnectionString = connectionString };
        }

        /// <summary>
        /// Creates a new instance of user store connection.
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        /// <param name="secureCredential">Secure SQL Credential.</param>
        internal SqlUserStoreConnection(string connectionString, SqlCredential secureCredential)
        {
            _conn = new SqlConnection { ConnectionString = connectionString, Credential = secureCredential };
        }

        /// <summary>
        /// Creates a new instance of user store connection.
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        /// <param name="accessToken">Secure SQL Access token</param>

        internal SqlUserStoreConnection(string connectionString, string accessToken)
        {
            _conn = new SqlConnection { ConnectionString = connectionString };
            _conn.AccessToken = accessToken;
        }

        /// <summary>
        /// Underlying SQL server connection.
        /// </summary>
        public SqlConnection Connection
        {
            get
            {
                return _conn;
            }
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        public void Open()
        {
            _conn.Open();
        }

        /// <summary>
        /// Asynchronously opens the connection.
        /// </summary>
        /// <returns>Task to await completion of the Open</returns>
        public Task OpenAsync()
        {
            return _conn.OpenAsync();
        }

        #region IDisposable

        /// <summary>
        /// Disposes the object.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Performs actual Dispose of resources.
        /// </summary>
        /// <param name="disposing">Whether the invocation was from IDisposable.Dipose method.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _conn.Dispose();
                _conn = null;
            }
        }

        #endregion IDisposable
    }
}
