// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents a SQL store operation.
    /// </summary>
    internal abstract class StoreOperation : IStoreOperation
    {
        /// <summary>
        /// GSM connection.
        /// </summary>
        private IStoreConnection _globalConnection;

        /// <summary>
        /// Source LSM connection.
        /// </summary>
        private IStoreConnection _localConnectionSource;

        /// <summary>
        /// Target LSM connection.
        /// </summary>
        private IStoreConnection _localConnectionTarget;


        /// <summary>
        /// State of the operation.
        /// </summary>
        private StoreOperationState _operationState;

        /// <summary>
        /// Maximum state reached during Do operation.
        /// </summary>
        private StoreOperationState _maxDoState;

        /// <summary>
        /// Constructs an instance of StoreOperation.
        /// </summary>
        /// <param name="shardMapManager">ShardMapManager object.</param>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">State from which Undo operation starts.</param>
        /// <param name="opCode">Operation code.</param>
        /// <param name="originalShardVersionRemoves">Original shard version for removes.</param>
        /// <param name="originalShardVersionAdds">Original shard version for adds.</param>
        protected StoreOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            StoreOperationCode opCode,
            Guid originalShardVersionRemoves,
            Guid originalShardVersionAdds
            )
        {
            this.Id = operationId;
            this.OperationCode = opCode;
            this.Manager = shardMapManager;
            this.UndoStartState = undoStartState;
            _operationState = StoreOperationState.DoBegin;
            _maxDoState = StoreOperationState.DoBegin;
            this.OriginalShardVersionRemoves = originalShardVersionRemoves;
            this.OriginalShardVersionAdds = originalShardVersionAdds;
        }

        /// <summary>
        /// ShardMapManager object.
        /// </summary>
        protected ShardMapManager Manager
        {
            get;
            private set;
        }

        /// <summary>
        /// Operation Id.
        /// </summary>
        protected Guid Id
        {
            get;
            private set;
        }

        /// <summary>
        /// Operation code.
        /// </summary>
        protected StoreOperationCode OperationCode
        {
            get;
            private set;
        }

        /// <summary>
        /// Operation Name.
        /// </summary>
        protected string OperationName
        {
            get
            {
                return StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode);
            }
        }

        /// <summary>
        /// Earliest point to start Undo operation.
        /// </summary>
        protected StoreOperationState UndoStartState
        {
            get;
            private set;
        }

        /// <summary>
        /// Original shard version on source.
        /// </summary>
        protected Guid OriginalShardVersionRemoves
        {
            get;
            private set;
        }

        /// <summary>
        /// Original shard version on target.
        /// </summary>
        protected Guid OriginalShardVersionAdds
        {
            get;
            private set;
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
                    result = this.Manager.RetryPolicy.ExecuteAction(() =>
                    {
                        IStoreResults r;

                        try
                        {
                            // Open connections & acquire the necessary app locks.
                            this.EstablishConnnections(false);

                            // Execute & commit the Global pre-Local operations.
                            r = this.DoGlobalPreLocal();

                            // If pending operation, we need to release the locks.
                            if (!r.StoreOperations.Any())
                            {
                                // Execute & commit the Local operations on source.
                                this.DoLocalSource();

                                // Execute & commit the Local operations on target.
                                this.DoLocalTarget();

                                // Execute & commit the Global post-Local operations.
                                r = this.DoGlobalPostLocal();

                                Debug.Assert(r != null);

                                _operationState = StoreOperationState.DoEnd;
                            }
                        }
                        finally
                        {
                            // Figure out the maximum of the progress made yet during Do operation.
                            if (_maxDoState < _operationState)
                            {
                                _maxDoState = _operationState;
                            }

                            // Close connections & release the necessary app locks.
                            this.TeardownConnections();
                        }

                        return r;
                    });

                    // If pending operation, deserialize the pending operation and perform Undo.
                    if (result.StoreOperations.Any())
                    {
                        Debug.Assert(result.StoreOperations.Count() == 1);

                        using (IStoreOperation op = this.Manager.StoreOperationFactory.FromLogEntry(this.Manager, result.StoreOperations.Single()))
                        {
                            op.Undo();
                        }
                    }
                }
                while (result.StoreOperations.Any());
            }
            catch (StoreException se)
            {
                // If store exception was thrown, we will attempt to undo the current operation.
                this.AttemptUndo();

                throw this.OnStoreException(se, _operationState);
            }
            catch (ShardManagementException)
            {
                // If shard map manager exception was thrown, we will attempt to undo the operation. 
                this.AttemptUndo();

                throw;
            }

            Debug.Assert(result != null);
            return result;
        }

        /// <summary>
        /// Performs the undo store operation.
        /// </summary>
        public void Undo()
        {
            try
            {
                this.Manager.RetryPolicy.ExecuteAction(
                () =>
                {
                    try
                    {
                        // Open connections & acquire the necessary app locks.
                        this.EstablishConnnections(true);

                        if (this.UndoGlobalPreLocal())
                        {
                            if (this.UndoStartState <= StoreOperationState.UndoLocalTargetBeginTransaction)
                            {
                                // Execute & commit the Local operations on target.
                                this.UndoLocalTarget();
                            }

                            if (this.UndoStartState <= StoreOperationState.UndoLocalSourceBeginTransaction)
                            {
                                // Execute & commit the Local undo operations on source.
                                this.UndoLocalSource();
                            }

                            if (this.UndoStartState <= StoreOperationState.UndoGlobalPostLocalBeginTransaction)
                            {
                                // Execute & commit the Global post-Local operations.
                                this.UndoGlobalPostLocal();
                            }
                        }

                        _operationState = StoreOperationState.UndoEnd;
                    }
                    finally
                    {
                        // Close connections & release the necessary app locks.
                        this.TeardownConnections();
                    }
                });
            }
            catch (StoreException se)
            {
                throw this.OnStoreException(se, _operationState);
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
            if (_localConnectionTarget != null)
            {
                _localConnectionTarget.Dispose();
                _localConnectionTarget = null;
            }

            if (_localConnectionSource != null)
            {
                _localConnectionSource.Dispose();
                _localConnectionSource = null;
            }

            if (_globalConnection != null)
            {
                _globalConnection.Dispose();
                _globalConnection = null;
            }
        }

        #endregion IDisposable

        /// <summary>
        /// Requests the derived class to provide information regarding the connections
        /// needed for the operation.
        /// </summary>
        /// <returns>Information about shards involved in the operation.</returns>
        public abstract StoreConnectionInfo GetStoreConnectionInfo();

        /// <summary>
        /// Performs the initial GSM operation prior to LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        public abstract IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the initial GSM operation prior to LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public abstract void HandleDoGlobalPreLocalExecuteError(IStoreResults result);

        /// <summary>
        /// Performs the LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public abstract IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the the LSM operation on the source shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public abstract void HandleDoLocalSourceExecuteError(IStoreResults result);

        /// <summary>
        /// Performs the LSM operation on the target shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public virtual IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            return new SqlResults();
        }

        /// <summary>
        /// Performs the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public virtual void HandleDoLocalTargetExecuteError(IStoreResults result)
        {
            Debug.Assert(result.Result == StoreResult.Success);
        }

        /// <summary>
        /// Performs the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        public abstract IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public abstract void HandleDoGlobalPostLocalExecuteError(IStoreResults result);

        /// <summary>
        /// Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public virtual void DoGlobalPostLocalUpdateCache(IStoreResults result)
        {
        }

        /// <summary>
        /// Performs undo of LSM operation on the target shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public virtual IStoreResults UndoLocalTargetExecute(IStoreTransactionScope ts)
        {
            return new SqlResults();
        }

        /// <summary>
        /// Performs undo of LSM operation on the target shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public virtual void HandleUndoLocalTargetExecuteError(IStoreResults result)
        {
            Debug.Assert(result.Result == StoreResult.Success);
        }

        /// <summary>
        /// Performs the undo of GSM operation prior to LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public virtual IStoreResults UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpFindAndUpdateOperationLogEntryByIdGlobal,
                StoreOperationRequestBuilder.FindAndUpdateOperationLogEntryByIdGlobal(this.Id, this.UndoStartState));
        }

        /// <summary>
        /// Handles errors from the undo of GSM operation prior to LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public virtual void HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnCommonErrorGlobal(
                result,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpFindAndUpdateOperationLogEntryByIdGlobal);
        }

        /// <summary>
        /// Performs the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public abstract IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public abstract void HandleUndoLocalSourceExecuteError(IStoreResults result);

        /// <summary>
        /// Performs the undo of GSM operation after LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        public abstract IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the undo of GSM operation after LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public abstract void HandleUndoGlobalPostLocalExecuteError(IStoreResults result);

        /// <summary>
        /// Returns the ShardManagementException to be thrown corresponding to a StoreException.
        /// </summary>
        /// <param name="se">Store Exception that has been raised.</param>
        /// <param name="state">SQL operation state.</param>
        /// <returns>ShardManagementException to be thrown.</returns>
        public virtual ShardManagementException OnStoreException(StoreException se, StoreOperationState state)
        {
            switch (state)
            {
                case StoreOperationState.DoGlobalConnect:

                case StoreOperationState.DoGlobalPreLocalBeginTransaction:
                case StoreOperationState.DoGlobalPreLocalExecute:
                case StoreOperationState.DoGlobalPreLocalCommitTransaction:

                case StoreOperationState.DoGlobalPostLocalBeginTransaction:
                case StoreOperationState.DoGlobalPostLocalExecute:
                case StoreOperationState.DoGlobalPostLocalCommitTransaction:

                case StoreOperationState.UndoGlobalConnect:

                case StoreOperationState.UndoGlobalPreLocalBeginTransaction:
                case StoreOperationState.UndoGlobalPreLocalExecute:
                case StoreOperationState.UndoGlobalPreLocalCommitTransaction:

                case StoreOperationState.UndoGlobalPostLocalBeginTransaction:
                case StoreOperationState.UndoGlobalPostLocalExecute:
                case StoreOperationState.UndoGlobalPostLocalCommitTransaction:
                    return ExceptionUtils.GetStoreExceptionGlobal(
                        this.ErrorCategory,
                        se,
                        this.OperationName);

                case StoreOperationState.DoLocalSourceConnect:
                case StoreOperationState.DoLocalSourceBeginTransaction:
                case StoreOperationState.DoLocalSourceExecute:
                case StoreOperationState.DoLocalSourceCommitTransaction:

                case StoreOperationState.UndoLocalSourceConnect:
                case StoreOperationState.UndoLocalSourceBeginTransaction:
                case StoreOperationState.UndoLocalSourceExecute:
                case StoreOperationState.UndoLocalSourceCommitTransaction:
                    return ExceptionUtils.GetStoreExceptionLocal(
                        this.ErrorCategory,
                        se,
                        this.OperationName,
                        this.ErrorSourceLocation);

                case StoreOperationState.DoLocalTargetConnect:
                case StoreOperationState.DoLocalTargetBeginTransaction:
                case StoreOperationState.DoLocalTargetExecute:
                case StoreOperationState.DoLocalTargetCommitTransaction:

                case StoreOperationState.UndoLocalTargetConnect:
                case StoreOperationState.UndoLocalTargetBeginTransaction:
                case StoreOperationState.UndoLocalTargetExecute:
                case StoreOperationState.UndoLocalTargetCommitTransaction:

                default:
                    return ExceptionUtils.GetStoreExceptionLocal(
                        this.ErrorCategory,
                        se,
                        this.OperationName,
                        this.ErrorTargetLocation);
            }
        }

        /// <summary>
        /// Source location of error.
        /// </summary>
        protected abstract ShardLocation ErrorSourceLocation
        {
            get;
        }

        /// <summary>
        /// Target location of error.
        /// </summary>
        protected abstract ShardLocation ErrorTargetLocation
        {
            get;
        }

        /// <summary>
        /// Error category for error.
        /// </summary>
        protected abstract ShardManagementErrorCategory ErrorCategory
        {
            get;
        }

        /// <summary>
        /// Obtains the connection string for an LSM location.
        /// </summary>
        /// <returns>Connection string for LSM given its location.</returns>
        protected string GetConnectionStringForShardLocation(ShardLocation location)
        {
            return new SqlConnectionStringBuilder(this.Manager.Credentials.ConnectionStringShard)
            {
                DataSource = location.DataSource,
                InitialCatalog = location.Database
            }
            .ConnectionString;
        }

        /// <summary>
        /// Given a state of the Do operation progress, gets the corresponding starting point
        /// for Undo operations.
        /// </summary>
        /// <param name="doState">State at which Do operation was executing.</param>
        /// <returns>Corresponding state for Undo operation.</returns>
        private static StoreOperationState UndoStateForDoState(StoreOperationState doState)
        {
            switch (doState)
            {
                case StoreOperationState.DoGlobalConnect:
                case StoreOperationState.DoLocalSourceConnect:
                case StoreOperationState.DoLocalTargetConnect:
                case StoreOperationState.DoGlobalPreLocalBeginTransaction:
                case StoreOperationState.DoGlobalPreLocalExecute:
                    return StoreOperationState.UndoEnd;

                case StoreOperationState.DoGlobalPreLocalCommitTransaction:
                case StoreOperationState.DoLocalSourceBeginTransaction:
                case StoreOperationState.DoLocalSourceExecute:
                    return StoreOperationState.UndoGlobalPostLocalBeginTransaction;

                case StoreOperationState.DoLocalSourceCommitTransaction:
                case StoreOperationState.DoLocalTargetBeginTransaction:
                case StoreOperationState.DoLocalTargetExecute:
                    return StoreOperationState.UndoLocalSourceBeginTransaction;

                case StoreOperationState.DoLocalTargetCommitTransaction:
                case StoreOperationState.DoGlobalPostLocalBeginTransaction:
                case StoreOperationState.DoGlobalPostLocalExecute:
                case StoreOperationState.DoGlobalPostLocalCommitTransaction:
                    return StoreOperationState.UndoLocalTargetBeginTransaction;

                case StoreOperationState.DoBegin:
                case StoreOperationState.DoEnd:
                default:
                    Debug.Fail("Unexpected Do states for corresponding Undo operation.");
                    return StoreOperationState.UndoBegin;
            }
        }

        /// <summary>
        /// Attempts to Undo the current operation which actually had caused an exception.
        /// </summary>
        /// <remarks>This is basically a best effort attempt.</remarks>
        private void AttemptUndo()
        {
            // Identify the point from which we shall start the undo operation.
            this.UndoStartState = UndoStateForDoState(_maxDoState);

            // If there is something to Undo.
            if (this.UndoStartState < StoreOperationState.UndoEnd)
            {
                try
                {
                    this.Undo();
                }
                catch (StoreException)
                {
                    // Do nothing, since we are raising the original Do operation exception.
                }
                catch (ShardManagementException)
                {
                    // Do nothing, since we are raising the original Do operation exception.
                }
            }
        }

        /// <summary>
        /// Established connections to the target databases.
        /// </summary>
        /// <param name="undo">Is this undo operation.</param>
        private void EstablishConnnections(bool undo)
        {
            _operationState = undo ? StoreOperationState.UndoGlobalConnect : StoreOperationState.DoGlobalConnect;

            // Find the necessary information for connections.
            StoreConnectionInfo sci = this.GetStoreConnectionInfo();

            Debug.Assert(sci != null);

            // Open global & local connections and acquire application level locks for the corresponding scope.
            _globalConnection = this.Manager.StoreConnectionFactory.GetConnection(
                StoreConnectionKind.Global,
                this.Manager.Credentials.ConnectionStringShardMapManager);

            _globalConnection.OpenWithLock(this.Id);

            if (sci.SourceLocation != null)
            {
                _operationState = undo ? StoreOperationState.UndoLocalSourceConnect : StoreOperationState.DoLocalSourceConnect;

                _localConnectionSource = this.Manager.StoreConnectionFactory.GetConnection(
                    StoreConnectionKind.LocalSource,
                    this.GetConnectionStringForShardLocation(sci.SourceLocation));

                _localConnectionSource.OpenWithLock(this.Id);
            }

            if (sci.TargetLocation != null)
            {
                Debug.Assert(sci.SourceLocation != null);

                _operationState = undo ? StoreOperationState.UndoLocalTargetConnect : StoreOperationState.DoLocalTargetConnect;

                _localConnectionTarget = this.Manager.StoreConnectionFactory.GetConnection(
                    StoreConnectionKind.LocalTarget,
                    this.GetConnectionStringForShardLocation(sci.TargetLocation));

                _localConnectionTarget.OpenWithLock(this.Id);
            }
        }

        /// <summary>
        /// Acquires the transaction scope.
        /// </summary>
        /// <returns>Transaction scope, operations within the scope excute atomically.</returns>
        private IStoreTransactionScope GetTransactionScope(StoreOperationTransactionScopeKind scopeKind)
        {
            switch (scopeKind)
            {
                case StoreOperationTransactionScopeKind.Global:
                    return _globalConnection.GetTransactionScope(StoreTransactionScopeKind.ReadWrite);

                case StoreOperationTransactionScopeKind.LocalSource:
                    return _localConnectionSource.GetTransactionScope(StoreTransactionScopeKind.ReadWrite);

                default:
                    Debug.Assert(scopeKind == StoreOperationTransactionScopeKind.LocalTarget);
                    return _localConnectionTarget.GetTransactionScope(StoreTransactionScopeKind.ReadWrite);
            }
        }

        /// <summary>
        /// Performs the initial GSM operation prior to LSM operations.
        /// </summary>
        /// <returns>Pending operations on the target objects if any.</returns>
        private IStoreResults DoGlobalPreLocal()
        {
            IStoreResults result;

            _operationState = StoreOperationState.DoGlobalPreLocalBeginTransaction;

            using (IStoreTransactionScope ts = this.GetTransactionScope(StoreOperationTransactionScopeKind.Global))
            {
                _operationState = StoreOperationState.DoGlobalPreLocalExecute;

                result = this.DoGlobalPreLocalExecute(ts);

                if (result.Result == StoreResult.Success)
                {
                    ts.Success = true;
                    _operationState = StoreOperationState.DoGlobalPreLocalCommitTransaction;
                }
            }

            if (result.Result != StoreResult.Success && result.Result != StoreResult.ShardPendingOperation)
            {
                this.HandleDoGlobalPreLocalExecuteError(result);
            }

            return result;
        }

        /// <summary>
        /// Performs the LSM operation on the source shard.
        /// </summary>
        private void DoLocalSource()
        {
            IStoreResults result;

            _operationState = StoreOperationState.DoLocalSourceBeginTransaction;

            using (IStoreTransactionScope ts = this.GetTransactionScope(StoreOperationTransactionScopeKind.LocalSource))
            {
                _operationState = StoreOperationState.DoLocalSourceExecute;

                result = this.DoLocalSourceExecute(ts);

                if (result.Result == StoreResult.Success)
                {
                    ts.Success = true;
                    _operationState = StoreOperationState.DoLocalSourceCommitTransaction;
                }
            }

            if (result.Result != StoreResult.Success)
            {
                this.HandleDoLocalSourceExecuteError(result);
            }
        }

        /// <summary>
        /// Performs the LSM operation on the target shard.
        /// </summary>
        private void DoLocalTarget()
        {
            if (_localConnectionTarget != null)
            {
                IStoreResults result;

                _operationState = StoreOperationState.DoLocalTargetBeginTransaction;

                using (IStoreTransactionScope ts = this.GetTransactionScope(StoreOperationTransactionScopeKind.LocalTarget))
                {
                    _operationState = StoreOperationState.DoLocalTargetExecute;

                    result = this.DoLocalTargetExecute(ts);

                    if (result.Result == StoreResult.Success)
                    {
                        ts.Success = true;
                        _operationState = StoreOperationState.DoLocalTargetCommitTransaction;
                    }
                }

                if (result.Result != StoreResult.Success)
                {
                    this.HandleDoLocalTargetExecuteError(result);
                }
            }
        }

        /// <summary>
        /// Performs the final GSM operation after the LSM operations.
        /// </summary>
        /// <returns>Results of the GSM operation.</returns>
        private IStoreResults DoGlobalPostLocal()
        {
            IStoreResults result;

            _operationState = StoreOperationState.DoGlobalPostLocalBeginTransaction;

            using (IStoreTransactionScope ts = this.GetTransactionScope(StoreOperationTransactionScopeKind.Global))
            {
                _operationState = StoreOperationState.DoGlobalPostLocalExecute;

                result = this.DoGlobalPostLocalExecute(ts);

                if (result.Result == StoreResult.Success)
                {
                    ts.Success = true;
                    _operationState = StoreOperationState.DoGlobalPostLocalCommitTransaction;
                }
            }

            if (result.Result != StoreResult.Success)
            {
                this.HandleDoGlobalPostLocalExecuteError(result);
            }
            else
            {
                this.DoGlobalPostLocalUpdateCache(result);
            }

            return result;
        }

        /// <summary>
        /// Perform undo of GSM operations before LSM operations. Basically checks if the operation
        /// to be undone is still present in the log.
        /// </summary>
        /// <returns>
        /// <c>true</c> if further undo operations are necessary, <c>false</c> otherwise.
        /// </returns>
        private bool UndoGlobalPreLocal()
        {
            IStoreResults result;

            _operationState = StoreOperationState.UndoGlobalPreLocalBeginTransaction;

            using (IStoreTransactionScope ts = this.GetTransactionScope(StoreOperationTransactionScopeKind.Global))
            {
                _operationState = StoreOperationState.UndoGlobalPreLocalExecute;

                result = this.UndoGlobalPreLocalExecute(ts);

                if (result.Result == StoreResult.Success)
                {
                    ts.Success = true;

                    if (result.StoreOperations.Any())
                    {
                        this.OriginalShardVersionRemoves = result.StoreOperations.Single().OriginalShardVersionRemoves;
                        this.OriginalShardVersionAdds = result.StoreOperations.Single().OriginalShardVersionAdds;
                        _operationState = StoreOperationState.UndoGlobalPreLocalCommitTransaction;
                    }
                }
            }

            if (result.Result != StoreResult.Success)
            {
                this.HandleUndoGlobalPreLocalExecuteError(result);
            }

            return result.StoreOperations.Any();
        }

        /// <summary>
        /// Performs the undo of LSM operation on the target shard.
        /// </summary>
        private void UndoLocalTarget()
        {
            if (_localConnectionTarget != null)
            {
                IStoreResults result;

                _operationState = StoreOperationState.UndoLocalTargetBeginTransaction;

                using (IStoreTransactionScope ts = this.GetTransactionScope(StoreOperationTransactionScopeKind.LocalTarget))
                {
                    _operationState = StoreOperationState.UndoLocalTargetExecute;

                    result = this.UndoLocalTargetExecute(ts);

                    if (result.Result == StoreResult.Success)
                    {
                        ts.Success = true;
                        _operationState = StoreOperationState.UndoLocalTargetCommitTransaction;
                    }
                }

                if (result.Result != StoreResult.Success)
                {
                    this.HandleUndoLocalTargetExecuteError(result);
                }
            }
        }

        /// <summary>
        /// Performs the undo of LSM operation on the source shard.
        /// </summary>
        private void UndoLocalSource()
        {
            IStoreResults result;

            _operationState = StoreOperationState.UndoLocalSourceBeginTransaction;

            using (IStoreTransactionScope ts = this.GetTransactionScope(StoreOperationTransactionScopeKind.LocalSource))
            {
                _operationState = StoreOperationState.UndoLocalSourceExecute;

                result = this.UndoLocalSourceExecute(ts);

                if (result.Result == StoreResult.Success)
                {
                    ts.Success = true;
                    _operationState = StoreOperationState.UndoLocalSourceCommitTransaction;
                }
            }

            if (result.Result != StoreResult.Success)
            {
                this.HandleUndoLocalSourceExecuteError(result);
            }
        }

        /// <summary>
        /// Performs the undo of GSM operation after LSM operations.
        /// </summary>
        private void UndoGlobalPostLocal()
        {
            IStoreResults result;

            _operationState = StoreOperationState.UndoGlobalPostLocalBeginTransaction;

            using (IStoreTransactionScope ts = this.GetTransactionScope(StoreOperationTransactionScopeKind.Global))
            {
                _operationState = StoreOperationState.UndoGlobalPostLocalExecute;

                result = this.UndoGlobalPostLocalExecute(ts);

                if (result.Result == StoreResult.Success)
                {
                    ts.Success = true;
                    _operationState = StoreOperationState.UndoGlobalPostLocalCommitTransaction;
                }
            }

            if (result.Result != StoreResult.Success)
            {
                this.HandleUndoGlobalPostLocalExecuteError(result);
            }
        }

        /// <summary>
        /// Terminates connections to target databases.
        /// </summary>
        private void TeardownConnections()
        {
            if (_localConnectionTarget != null)
            {
                _localConnectionTarget.CloseWithUnlock(this.Id);
            }

            if (_localConnectionSource != null)
            {
                _localConnectionSource.CloseWithUnlock(this.Id);
            }

            if (_globalConnection != null)
            {
                _globalConnection.CloseWithUnlock(this.Id);
            }
        }
    }
}
