// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Performs a GSM only store operation.
    /// </summary>
    internal abstract class StoreOperationGlobal : IStoreOperationGlobal
    {
        /// <summary>
        /// GSM connection.
        /// </summary>
        private IStoreConnection _globalConnection;

        /// <summary>
        /// Credentials for connection establishment.
        /// </summary>
        private SqlShardMapManagerCredentials _credentials;

        /// <summary>
        /// Retry policy.
        /// </summary>
        private TransientFaultHandling.RetryPolicy _retryPolicy;

        /// <summary>
        /// Constructs an instance of SqlOperationGlobal.
        /// </summary>
        /// <param name="credentials">Credentials for connecting to SMM GSM database.</param>
        /// <param name="retryPolicy">Retry policy for requests.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        internal StoreOperationGlobal(SqlShardMapManagerCredentials credentials, TransientFaultHandling.RetryPolicy retryPolicy, string operationName)
        {
            this.OperationName = operationName;
            _credentials = credentials;
            _retryPolicy = retryPolicy;
        }

        /// <summary>
        /// Operation name, useful for diagnostics.
        /// </summary>
        protected string OperationName
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
            IStoreResults result;

            try
            {
                do
                {
                    result = _retryPolicy.ExecuteAction(() =>
                    {
                        IStoreResults r;
                        try
                        {
                            // Open connection.
                            this.EstablishConnnection();

                            using (IStoreTransactionScope ts = this.GetTransactionScope())
                            {
                                r = this.DoGlobalExecute(ts);

                                ts.Success = r.Result == StoreResult.Success;
                            }

                            if (!r.StoreOperations.Any())
                            {
                                if (r.Result != StoreResult.Success)
                                {
                                    this.DoGlobalUpdateCachePre(r);

                                    this.HandleDoGlobalExecuteError(r);
                                }

                                this.DoGlobalUpdateCachePost(r);
                            }

                            return r;
                        }
                        finally
                        {
                            // Close connection.
                            this.TeardownConnection();
                        }
                    });

                    // If pending operation, deserialize the pending operation and perform Undo.
                    if (result.StoreOperations.Any())
                    {
                        Debug.Assert(result.StoreOperations.Count() == 1);

                        this.UndoPendingStoreOperations(result.StoreOperations.Single());
                    }
                }
                while (result.StoreOperations.Any());
            }
            catch (StoreException se)
            {
                throw this.OnStoreException(se);
            }

            Debug.Assert(result != null);
            return result;
        }

        /// <summary>
        /// Asynchronously performs the store operation.
        /// </summary>
        /// <returns>Task encapsulating the results of the operation.</returns>
        public async Task<IStoreResults> DoAsync()
        {
            IStoreResults result;

            try
            {
                do
                {
                    result = await _retryPolicy.ExecuteAsync<IStoreResults>(async () =>
                    {
                        IStoreResults r;
                        try
                        {
                            // Open connection.
                            await this.EstablishConnnectionAsync().ConfigureAwait(false);

                            using (IStoreTransactionScope ts = this.GetTransactionScope())
                            {
                                r = await this.DoGlobalExecuteAsync(ts).ConfigureAwait(false);

                                ts.Success = r.Result == StoreResult.Success;
                            }

                            if (!r.StoreOperations.Any())
                            {
                                if (r.Result != StoreResult.Success)
                                {
                                    this.DoGlobalUpdateCachePre(r);

                                    this.HandleDoGlobalExecuteError(r);
                                }

                                this.DoGlobalUpdateCachePost(r);
                            }

                            return r;
                        }
                        finally
                        {
                            // Close connection.
                            this.TeardownConnection();
                        }
                    }).ConfigureAwait(false);

                    // If pending operation, deserialize the pending operation and perform Undo.
                    if (result.StoreOperations.Any())
                    {
                        Debug.Assert(result.StoreOperations.Count() == 1);

                        await this.UndoPendingStoreOperationsAsync(result.StoreOperations.Single()).ConfigureAwait(false);
                    }
                }
                while (result.StoreOperations.Any());
            }
            catch (StoreException se)
            {
                throw this.OnStoreException(se);
            }

            Debug.Assert(result != null);
            return result;
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
            if (_globalConnection != null)
            {
                _globalConnection.Dispose();
                _globalConnection = null;
            }
        }

        #endregion IDisposable

        /// <summary>
        /// Execute the operation against GSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        public abstract IStoreResults DoGlobalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Asynchronously execute the operation against GSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Task encapsulating results of the operation.
        /// </returns>
        public virtual Task<IStoreResults> DoGlobalExecuteAsync(IStoreTransactionScope ts)
        {
            // Currently only implemented by FindMappingByKeyGlobalOperation
            throw new NotImplementedException();
        }

        /// <summary>
        /// Invalidates the cache on unsuccessful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public virtual void DoGlobalUpdateCachePre(IStoreResults result)
        {
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public abstract void HandleDoGlobalExecuteError(IStoreResults result);

        /// <summary>
        /// Refreshes the cache on successful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public virtual void DoGlobalUpdateCachePost(IStoreResults result)
        {
        }

        /// <summary>
        /// Returns the ShardManagementException to be thrown corresponding to a StoreException.
        /// </summary>
        /// <param name="se">Store exception that has been raised.</param>
        /// <returns>ShardManagementException to be thrown.</returns>
        public virtual ShardManagementException OnStoreException(StoreException se)
        {
            return ExceptionUtils.GetStoreExceptionGlobal(
                this.ErrorCategory,
                se,
                this.OperationName);
        }

        /// <summary>
        /// Error category for store exception.
        /// </summary>
        protected abstract ShardManagementErrorCategory ErrorCategory
        {
            get;
        }

        /// <summary>
        /// Performs undo of the storage operation that is pending.
        /// </summary>
        /// <param name="logEntry">Log entry for the pending operation.</param>
        protected virtual void UndoPendingStoreOperations(IStoreLogEntry logEntry)
        {
            // Will only be implemented by LockOrUnLockMapping operation
            // which will actually perform the undo operation.
            throw new NotImplementedException();
        }

        /// <summary>
        /// Asynchronously performs undo of the storage operation that is pending.
        /// </summary>
        /// <param name="logEntry">Log entry for the pending operation.</param>
        /// <returns>Task to await Undo of the operation</returns>
        /// <remarks>Currently not used anywhere since the Async APIs were added
        /// in support of the look-up operations</remarks>
        protected virtual Task UndoPendingStoreOperationsAsync(IStoreLogEntry logEntry)
        {
            // Currently async APIs are only used by FindMappingByKeyGlobalOperation
            // which doesn't require Undo
            throw new NotImplementedException();
        }

        /// <summary>
        /// Establishes connection to the SMM GSM database.
        /// </summary>
        private void EstablishConnnection()
        {
            _globalConnection = new SqlStoreConnection(StoreConnectionKind.Global, _credentials.ConnectionStringShardMapManager);
            _globalConnection.Open();
        }

        /// <summary>
        /// Asynchronously establishes connection to the SMM GSM database.
        /// </summary>
        /// <returns>Task to await connection establishment</returns>
        private Task EstablishConnnectionAsync()
        {
            _globalConnection = new SqlStoreConnection(StoreConnectionKind.Global, _credentials.ConnectionStringShardMapManager);
            return _globalConnection.OpenAsync();
        }

        /// <summary>
        /// Acquires the transaction scope.
        /// </summary>
        /// <returns>Transaction scope, operations within the scope excute atomically.</returns>
        private IStoreTransactionScope GetTransactionScope()
        {
            return _globalConnection.GetTransactionScope(this.ReadOnly ? StoreTransactionScopeKind.ReadOnly : StoreTransactionScopeKind.ReadWrite);
        }

        /// <summary>
        /// Terminates the connections after finishing the operation.
        /// </summary>
        private void TeardownConnection()
        {
            if (_globalConnection != null)
            {
                _globalConnection.Close();
                _globalConnection = null;
            }
        }
    }
}
