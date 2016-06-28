// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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
        private SqlConnection _conn;

        /// <summary>
        /// Constructs an instance of Sql Store Connection.
        /// </summary>
        /// <param name="kind">Type of store connection.</param>
        /// <param name="connectionString"></param>
        protected internal SqlStoreConnection(StoreConnectionKind kind, string connectionString)
        {
            this.Kind = kind;
            _conn = new SqlConnection();
            _conn.ConnectionString = connectionString;
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
                _conn.Open();
            });
        }

        /// <summary>
        /// Asynchronously open the store connection.
        /// </summary>
        /// <returns>A task to await completion of the Open</returns>
        public virtual Task OpenAsync()
        {
            return SqlUtils.WithSqlExceptionHandlingAsync(() =>
            {
                return _conn.OpenAsync();
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
                _conn.Open();
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
                if (_conn != null)
                {
                    _conn.Dispose();
                    _conn = null;
                }
            });
        }

        /// <summary>
        /// Closes the store connection after releasing lock.
        /// <param name="lockId">Lock Id.</param>
        /// </summary>
        public virtual void CloseWithUnlock(Guid lockId)
        {
            SqlUtils.WithSqlExceptionHandling(() =>
            {
                if (_conn != null)
                {
                    if (_conn.State == ConnectionState.Open)
                    {
                        this.ReleaseAppLock(lockId);
                    }
                    _conn.Dispose();
                    _conn = null;
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
            return new SqlStoreTransactionScope(kind, _conn);
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed", MessageId = "_conn", Justification = "Connection is being disposed.")]
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
            using (SqlCommand cmdGetAppLock = _conn.CreateCommand())
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

        /// <summary>
        /// Releases an application level lock on the connection which is session scoped.
        /// </summary>
        /// <param name="lockId">Identity of the lock.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification="We can ignore request failure.")]
        private void ReleaseAppLock(Guid lockId)
        {
            using (SqlCommand cmdReleaseAppLock = _conn.CreateCommand())
            {
                cmdReleaseAppLock.CommandText = @"sp_releaseapplock";
                cmdReleaseAppLock.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(
                    cmdReleaseAppLock,
                    "@Resource",
                    SqlDbType.NVarChar,
                    ParameterDirection.Input,
                    255 * 2,
                    lockId.ToString());

                SqlUtils.AddCommandParameter(
                    cmdReleaseAppLock,
                    "@LockOwner",
                    SqlDbType.NVarChar,
                    ParameterDirection.Input,
                    32 * 2,
                    "Session");

                SqlParameter returnValue = SqlUtils.AddCommandParameter(
                    cmdReleaseAppLock,
                    "@RETURN_VALUE",
                    SqlDbType.Int,
                    ParameterDirection.ReturnValue,
                    0,
                    0);

                try
                {
                    cmdReleaseAppLock.ExecuteNonQuery();
                }
                catch(Exception)
                {
                    // ignore all exceptions.
                    return;
                }

                // If parameter validation or other errors happen.
                if ((int)returnValue.Value < 0)
                {
                    throw new ShardManagementException(
                        ShardManagementErrorCategory.General,
                        ShardManagementErrorCode.LockNotReleased,
                        Errors._Store_SqlOperation_LockNotReleased,
                        lockId);
                }
            }
        }
    }
}
