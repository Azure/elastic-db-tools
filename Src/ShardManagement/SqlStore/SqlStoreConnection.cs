using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Instance of a Sql Store Connection.
    /// </summary>
    internal class SqlStoreConnection : IStoreConnection
    {
        /// <summary>
        /// Underlying SQL connection object.
        /// </summary>
        private SqlConnection conn;

        /// <summary>
        /// Constructs an instance of Sql Store Connection.
        /// </summary>
        /// <param name="kind">Type of store connection.</param>
        /// <param name="connectionString"></param>
        protected internal SqlStoreConnection(StoreConnectionKind kind, string connectionString)
        {
            this.Kind = kind;
            this.conn = new SqlConnection();
            this.conn.ConnectionString = connectionString;
        }

        /// <summary>
        /// Type of store connection.
        /// </summary>
        public virtual StoreConnectionKind Kind
        {
            get;
            private set;
        }

        /// <summary>
        /// Open the store connection.
        /// </summary>
        public virtual void Open()
        {
            SqlUtils.WithSqlExceptionHandling(() =>
            {
                this.conn.Open();
            });
        }

        /// <summary>
        /// Asynchronously open the store connection.
        /// </summary>
        /// <returns>A task to await completion of the Open</returns>
        public virtual async Task OpenAsync()
        {
            await SqlUtils.WithSqlExceptionHandlingAsync(async () =>
            {
                await this.conn.OpenAsync();
            });
        }

        /// <summary>
        /// Open the store connection, and acquire a lock on the store.
        /// </summary>
        /// <param name="lockId">Lock Id.</param>
        public virtual void OpenWithLock(Guid lockId)
        {
            SqlUtils.WithSqlExceptionHandling(() =>
            {
                this.conn.Open();
                this.GetAppLock(lockId);
            });
        }

        /// <summary>
        /// Closes the store connection.
        /// </summary>
        public virtual void Close()
        {
            SqlUtils.WithSqlExceptionHandling(() =>
            {
                if (this.conn != null)
                {
                    this.conn.Dispose();
                    this.conn = null;
                }
            });
        }

        /// <summary>
        /// Acquires a transactional scope on the connection.
        /// </summary>
        /// <param name="kind">Type of transaction scope.</param>
        /// <returns>Transaction scope on the store connection.</returns>
        public virtual IStoreTransactionScope GetTransactionScope(StoreTransactionScopeKind kind)
        {
            return new SqlStoreTransactionScope(kind, this.conn);
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed", MessageId = "conn", Justification = "Connection is being disposed.")]
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.Close();
            }
        }

        #endregion IDisposable

        /// <summary>
        /// Acquires an application level lock on the connection which is session scoped.
        /// </summary>
        /// <param name="lockId">Identity of the lock.</param>
        private void GetAppLock(Guid lockId)
        {
            using (SqlCommand cmdGetAppLock = this.conn.CreateCommand())
            {
                cmdGetAppLock.CommandText = @"sp_getapplock";
                cmdGetAppLock.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(
                    cmdGetAppLock, 
                    "@Resource", 
                    SqlDbType.NVarChar, 
                    ParameterDirection.Input, 
                    255 * 2, 
                    lockId.ToString());

                SqlUtils.AddCommandParameter(
                    cmdGetAppLock, 
                    "@LockMode", 
                    SqlDbType.NVarChar, 
                    ParameterDirection.Input, 
                    32 * 2, 
                    "Exclusive");

                SqlUtils.AddCommandParameter(
                    cmdGetAppLock, 
                    "@LockOwner", 
                    SqlDbType.NVarChar, 
                    ParameterDirection.Input, 
                    32 * 2, 
                    "Session");

                SqlUtils.AddCommandParameter(
                    cmdGetAppLock, 
                    "@LockTimeout", 
                    SqlDbType.Int, 
                    ParameterDirection.Input, 
                    0, 
                    GlobalConstants.DefaultLockTimeOut);

                SqlParameter returnValue = SqlUtils.AddCommandParameter(
                    cmdGetAppLock,
                    "@RETURN_VALUE",
                    SqlDbType.Int,
                    ParameterDirection.ReturnValue,
                    0,
                    0);

                cmdGetAppLock.ExecuteNonQuery();

                // If time-out or other errors happen.
                if ((int)returnValue.Value < 0)
                {
                    throw new ShardManagementException(
                        ShardManagementErrorCategory.General,
                        ShardManagementErrorCode.LockNotAcquired,
                        Errors._Store_SqlOperation_LockNotAcquired,
                        lockId);
                }
            }
        }
    }
}
