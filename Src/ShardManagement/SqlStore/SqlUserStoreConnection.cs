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
        private SqlConnection conn;

        /// <summary>
        /// Creates a new instance of user store connection.
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        internal SqlUserStoreConnection(string connectionString)
        {
            this.conn = new SqlConnection(connectionString);
        }

        /// <summary>
        /// Underlying SQL server connection.
        /// </summary>
        public SqlConnection Connection
        {
            get 
            { 
                return this.conn; 
            }
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        public void Open()
        {
            this.conn.Open();
        }

        /// <summary>
        /// Asynchronously opens the connection.
        /// </summary>
        /// <returns>Task to await completion of the Open</returns>
        public async Task OpenAsync()
        {
            await this.conn.OpenAsync();
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
                this.conn.Dispose();
                this.conn = null;
            }
        }

        #endregion IDisposable
    }
}
