// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Removes a shard from given shard map.
    /// </summary>
    internal class RemoveShardOperation : StoreOperation
    {
        /// <summary>
        /// Shard map for which to remove the shard.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Shard to remove.
        /// </summary>
        private IStoreShard _shard;

        /// <summary>
        /// Creates request to remove shard from given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which to remove shard.</param>
        /// <param name="shard">Shard to remove.</param>
        protected internal RemoveShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard) :
            this(
            shardMapManager,
            Guid.NewGuid(),
            StoreOperationState.UndoBegin,
            shardMap,
            shard)
        {
        }

        /// <summary>
        /// Creates request to remove shard from given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation id.</param>
        /// <param name="undoStartState">State from which Undo operation starts.</param>
        /// <param name="shardMap">Shard map for which to remove shard.</param>
        /// <param name="shard">Shard to remove.</param>
        internal RemoveShardOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            IStoreShardMap shardMap,
            IStoreShard shard) :
            base(
            shardMapManager,
            operationId,
            undoStartState,
            StoreOperationCode.RemoveShard,
            default(Guid),
            default(Guid))
        {
            _shardMap = shardMap;
            _shard = shard;
        }

        /// <summary>
        /// Requests the derived class to provide information regarding the connections
        /// needed for the operation.
        /// </summary>
        /// <returns>Information about shards involved in the operation.</returns>
        public override StoreConnectionInfo GetStoreConnectionInfo()
        {
            return new StoreConnectionInfo()
            {
                SourceLocation = this.UndoStartState <= StoreOperationState.UndoLocalSourceBeginTransaction ? _shard.Location : null
            };
        }

        /// <summary>
        /// Performs the initial GSM operation prior to LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        public override IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardsGlobalBegin,
                StoreOperationRequestBuilder.RemoveShardGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    _shardMap,
                    _shard));
        }

        /// <summary>
        /// Handles errors from the initial GSM operation prior to LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalPreLocalExecuteError(IStoreResults result)
        {
            if (result.Result == StoreResult.ShardMapDoesNotExist)
            {
                // Remove shard map from cache.
                this.Manager.Cache.DeleteShardMap(_shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.ShardDoesNotExist
            // StoreResult.ShardVersionMismatch
            // StoreResult.ShardHasMappings
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorGlobal(
                result,
                _shardMap,
                _shard,
                ShardManagementErrorCategory.ShardMap,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardsGlobalBegin);
        }

        /// <summary>
        /// Performs the LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            // Now actually add the shard entries.
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpRemoveShardLocal,
                StoreOperationRequestBuilder.RemoveShardLocal(this.Id, _shardMap, _shard));
        }

        /// <summary>
        /// Handles errors from the the LSM operation on the source shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoLocalSourceExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorLocal(
                result,
                _shardMap,
                _shard.Location,
                ShardManagementErrorCategory.ShardMap,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpRemoveShardLocal);
        }

        /// <summary>
        /// Performs the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd,
                StoreOperationRequestBuilder.RemoveShardGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    _shardMap,
                    _shard));
        }

        /// <summary>
        /// Handles errors from the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalPostLocalExecuteError(IStoreResults result)
        {
            if (result.Result == StoreResult.ShardMapDoesNotExist)
            {
                // Remove shard map from cache.
                this.Manager.Cache.DeleteShardMap(_shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorGlobal(
                result,
                _shardMap,
                _shard,
                ShardManagementErrorCategory.ShardMap,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd);
        }

        /// <summary>
        /// Performs the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
        {
            // Adds back the removed shard entries.
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpAddShardLocal,
                StoreOperationRequestBuilder.AddShardLocal(this.Id, true, _shardMap, _shard));
        }

        /// <summary>
        /// Handles errors from the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleUndoLocalSourceExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorLocal(
                result,
                _shardMap,
                _shard.Location,
                ShardManagementErrorCategory.ShardMap,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpAddShardLocal);
        }

        /// <summary>
        /// Performs the undo of GSM operation after LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        public override IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd,
                StoreOperationRequestBuilder.RemoveShardGlobal(
                    this.Id,
                    this.OperationCode,
                    true, // undo
                    _shardMap,
                    _shard));
        }

        /// <summary>
        /// Handles errors from the undo of GSM operation after LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
        {
            if (result.Result == StoreResult.ShardMapDoesNotExist)
            {
                // Remove shard map from cache.
                this.Manager.Cache.DeleteShardMap(_shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorGlobal(
                result,
                _shardMap,
                _shard,
                ShardManagementErrorCategory.ShardMap,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardsGlobalEnd);
        }

        /// <summary>
        /// Source location of error.
        /// </summary>
        protected override ShardLocation ErrorSourceLocation
        {
            get
            {
                return _shard.Location;
            }
        }

        /// <summary>
        /// Target location of error.
        /// </summary>
        protected override ShardLocation ErrorTargetLocation
        {
            get
            {
                return _shard.Location;
            }
        }

        /// <summary>
        /// Error category for error.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return ShardManagementErrorCategory.ShardMap;
            }
        }
    }
}
