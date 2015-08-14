// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Performs a SqlOperation against an LSM.
    /// </summary>
    internal abstract class StoreOperationLocal : IStoreOperationLocal
    {
        /// <summary>
        /// LSM connection.
        /// </summary>
        private IStoreConnection _localConnection;

        /// <summary>
        /// Credentials for connection establishment.
        /// </summary>
        private SqlShardMapManagerCredentials _credentials;

        /// <summary>
        /// Retry policy.
        /// </summary>
        private TransientFaultHandling.RetryPolicy _retryPolicy;

        /// <summary>
        /// Constructs an instance of SqlOperationLocal.
        /// </summary>
        /// <param name="credentials">Credentials for connecting to SMM databases.</param>
        /// <param name="retryPolicy">Retry policy for requests.</param>
        /// <param name="location">Shard location where the operation is to be performed.</param>
        /// <param name="operationName">Operation name.</param>
        internal StoreOperationLocal(
            SqlShardMapManagerCredentials credentials,
            TransientFaultHandling.RetryPolicy retryPolicy,
            ShardLocation location,
            string operationName)
        {
            _credentials = credentials;
            _retryPolicy = retryPolicy;
            this.OperationName = operationName;
            this.Location = location;
        }

        protected string OperationName
        {
            get;
            private set;
        }

        /// <summary>
        /// Location of LSM.
        /// </summary>
        protected ShardLocation Location
        {
            get;
            private set;
        }

        /// <summary>
        /// Whether this is a read-only operation.
        /// </summary>
        public abstract bool ReadOnly
        {
            get;
        }

        /// <summary>
        /// Performs the store operation.
        /// </summary>
        /// <returns>Results of the operation.</returns>
        public IStoreResults Do()
        {
            try
            {
                return _retryPolicy.ExecuteAction(() =>
                {
                    IStoreResults r;
                    try
                    {
                        // Open connection.
                        this.EstablishConnnection();

                        using (IStoreTransactionScope ts = this.GetTransactionScope())
                        {
                            r = this.DoLocalExecute(ts);

                            ts.Success = r.Result == StoreResult.Success;
                        }

                        if (r.Result != StoreResult.Success)
                        {
                            this.HandleDoLocalExecuteError(r);
                        }

                        return r;
                    }
                    finally
                    {
                        // Close connection.
                        this.TeardownConnection();
                    }
                });
            }
            catch (StoreException se)
            {
                throw this.OnStoreException(se);
            }
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
            if (_localConnection != null)
            {
                _localConnection.Dispose();
                _localConnection = null;
            }
        }

        #endregion IDisposable

        /// <summary>
        /// Execute the operation against LSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        public abstract IStoreResults DoLocalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the LSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public abstract void HandleDoLocalExecuteError(IStoreResults result);

        /// <summary>
        /// Returns the ShardManagementException to be thrown corresponding to a StoreException.
        /// </summary>
        /// <param name="se">Store exception that has been raised.</param>
        /// <returns>ShardManagementException to be thrown.</returns>
        public virtual ShardManagementException OnStoreException(StoreException se)
        {
            return ExceptionUtils.GetStoreExceptionLocal(
                ShardManagementErrorCategory.Recovery,
                se,
                this.OperationName,
                this.Location);
        }

        /// <summary>
        /// Establishes connection to the target shard.
        /// </summary>
        private void EstablishConnnection()
        {
            // Open connection.
            SqlConnectionStringBuilder localConnectionString =
                new SqlConnectionStringBuilder(_credentials.ConnectionStringShard)
                {
                    DataSource = this.Location.DataSource,
                    InitialCatalog = this.Location.Database
                };

            _localConnection = new SqlStoreConnection(StoreConnectionKind.LocalSource, localConnectionString.ConnectionString);
            _localConnection.Open();
        }

        /// <summary>
        /// Acquires the transaction scope.
        /// </summary>
        /// <returns>Transaction scope, operations within the scope excute atomically.</returns>
        private IStoreTransactionScope GetTransactionScope()
        {
            return _localConnection.GetTransactionScope(this.ReadOnly ? StoreTransactionScopeKind.ReadOnly : StoreTransactionScopeKind.ReadWrite);
        }

        /// <summary>
        /// Terminates the connections after finishing the operation.
        /// </summary>
        private void TeardownConnection()
        {
            // Close connection.
            if (_localConnection != null)
            {
                _localConnection.Close();
            }
        }
    }
}
