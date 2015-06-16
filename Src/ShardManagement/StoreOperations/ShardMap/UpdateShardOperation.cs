using System;
using System.Data.SqlClient;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Updates a shard in given shard map.
    /// </summary>
    class UpdateShardOperation : StoreOperation
    {
        /// <summary>
        /// Shard map for which to update the shard.
        /// </summary>
        private IStoreShardMap shardMap;

        /// <summary>
        /// Shard to update.
        /// </summary>
        private IStoreShard shardOld;

        /// <summary>
        /// Updated shard.
        /// </summary>
        private IStoreShard shardNew;

        /// <summary>
        /// Creates request to update shard in given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which to remove shard.</param>
        /// <param name="shardOld">Shard to update.</param>
        /// <param name="shardNew">Updated shard.</param>
        protected internal UpdateShardOperation(
            ShardMapManager shardMapManager, 
            IStoreShardMap shardMap, 
            IStoreShard shardOld, 
            IStoreShard shardNew) :
            this(shardMapManager, Guid.NewGuid(), StoreOperationState.UndoBegin, shardMap, shardOld, shardNew)
        {
        }

        /// <summary>
        /// Creates request to update shard in given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation id.</param>
        /// <param name="undoStartState">State from which Undo operation starts.</param>
        /// <param name="shardMap">Shard map for which to remove shard.</param>
        /// <param name="shardOld">Shard to update.</param>
        /// <param name="shardNew">Updated shard.</param>
        internal UpdateShardOperation(
            ShardMapManager shardMapManager, 
            Guid operationId, 
            StoreOperationState undoStartState, 
            IStoreShardMap shardMap, 
            IStoreShard shardOld, 
            IStoreShard shardNew) :
            base(
            shardMapManager, 
            operationId, 
            undoStartState, 
            StoreOperationCode.UpdateShard,
            default(Guid),
            default(Guid))
        {
            this.shardMap = shardMap;
            this.shardOld = shardOld;
            this.shardNew = shardNew;
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
                SourceLocation = this.UndoStartState <= StoreOperationState.UndoLocalSourceBeginTransaction ? this.shardOld.Location : null
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
                StoreOperationRequestBuilder.UpdateShardGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    this.shardMap,
                    this.shardOld,
                    this.shardNew));
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
                this.Manager.Cache.DeleteShardMap(this.shardMap);
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
                this.shardMap,
                this.shardOld,
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
            // Now actually update the shard.
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpUpdateShardLocal,
                StoreOperationRequestBuilder.UpdateShardLocal(this.Id, this.shardMap, this.shardNew));
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
            // Stored procedure can also return StoreResult.ShardDoesNotExist, but only for AttachShard operations
            throw StoreOperationErrorHandler.OnShardMapErrorLocal(
                result,
                this.shardMap,
                this.shardOld.Location,
                ShardManagementErrorCategory.ShardMap,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpUpdateShardLocal);
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
                StoreOperationRequestBuilder.UpdateShardGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    this.shardMap,
                    this.shardOld,
                    this.shardNew));
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
                this.Manager.Cache.DeleteShardMap(this.shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorGlobal(
                result,
                this.shardMap,
                this.shardOld,
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
                StoreOperationRequestBuilder.SpUpdateShardLocal,
                StoreOperationRequestBuilder.UpdateShardLocal(this.Id, this.shardMap, this.shardOld));
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
            // Stored procedure can also return StoreResult.ShardDoesNotExist, but only for AttachShard operations
            throw StoreOperationErrorHandler.OnShardMapErrorLocal(
                result,
                this.shardMap,
                this.shardNew.Location,
                ShardManagementErrorCategory.ShardMap,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpUpdateShardLocal);
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
                StoreOperationRequestBuilder.UpdateShardGlobal(
                    this.Id,
                    this.OperationCode,
                    true, // undo
                    this.shardMap,
                    this.shardOld,
                    this.shardNew));
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
                this.Manager.Cache.DeleteShardMap(this.shardMap);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorGlobal(
                result,
                this.shardMap,
                this.shardOld,
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
                return this.shardOld.Location;
            }
        }

        /// <summary>
        /// Target location of error.
        /// </summary>
        protected override ShardLocation ErrorTargetLocation
        {
            get
            {
                return this.shardNew.Location;
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
