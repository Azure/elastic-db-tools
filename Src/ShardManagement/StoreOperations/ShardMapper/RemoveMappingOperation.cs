using System;
using System.Data.SqlClient;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Removes a mapping from given shard map.
    /// </summary>
    class RemoveMappingOperation : StoreOperation
    {
        /// <summary>
        /// Shard map from which to remove the mapping.
        /// </summary>
        private IStoreShardMap shardMap;

        /// <summary>
        /// Mapping to remove.
        /// </summary>
        private IStoreMapping mapping;

        /// <summary>
        /// Lock owner.
        /// </summary>
        private Guid lockOwnerId;

        /// <summary>
        /// Error category to use.
        /// </summary>
        private ShardManagementErrorCategory errorCategory;

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map from which to remove mapping.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <param name="lockOwnerId">Id of lock owner.</param>
        protected internal RemoveMappingOperation(
            ShardMapManager shardMapManager, 
            StoreOperationCode operationCode, 
            IStoreShardMap shardMap, 
            IStoreMapping mapping, 
            Guid lockOwnerId) :
            this(
            shardMapManager, 
            Guid.NewGuid(), 
            StoreOperationState.UndoBegin, 
            operationCode, 
            shardMap, 
            mapping, 
            lockOwnerId, 
            default(Guid))
        {
        }

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation id.</param>
        /// <param name="undoStartState">State from which Undo operation starts.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map from which to remove mapping.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <param name="lockOwnerId">Id of lock owner.</param>
        /// <param name="originalShardVersionRemoves">Original shard version.</param>
        internal RemoveMappingOperation(
            ShardMapManager shardMapManager, 
            Guid operationId, 
            StoreOperationState undoStartState, 
            StoreOperationCode operationCode, 
            IStoreShardMap shardMap, 
            IStoreMapping mapping, 
            Guid lockOwnerId, 
            Guid originalShardVersionRemoves) :
            base(
            shardMapManager, 
            operationId, 
            undoStartState, 
            operationCode,
            originalShardVersionRemoves,
            default(Guid))
        {
            this.shardMap = shardMap;
            this.mapping = mapping;
            this.lockOwnerId = lockOwnerId;
            this.errorCategory = operationCode == StoreOperationCode.RemoveRangeMapping ?
                ShardManagementErrorCategory.RangeShardMap :
                ShardManagementErrorCategory.ListShardMap;
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
                SourceLocation = this.UndoStartState <= StoreOperationState.UndoLocalSourceBeginTransaction ? this.mapping.StoreShard.Location : null
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
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin,
                StoreOperationRequestBuilder.RemoveShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    this.shardMap,
                    this.mapping,
                    this.lockOwnerId));
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

            if (result.Result == StoreResult.MappingDoesNotExist)
            {
                // Remove mapping from cache.
                this.Manager.Cache.DeleteMapping(this.mapping);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.ShardDoesNotExist
            // StoreShard.MappingDoesNotExist
            // StoreResult.MappingLockOwnerIdDoesNotMatch
            // StoreResult.MappingIsNotOffline
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                this.shardMap,
                this.mapping.StoreShard,
                this.errorCategory,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalBegin);
        }

        /// <summary>
        /// Performs the LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                StoreOperationRequestBuilder.RemoveShardMappingLocal(
                    this.Id,
                    false,
                    this.shardMap,
                    this.mapping));
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
            throw StoreOperationErrorHandler.OnShardMapperErrorLocal(
                result,
                this.mapping.StoreShard.Location,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
        }

        /// <summary>
        /// Performs the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd,
                StoreOperationRequestBuilder.RemoveShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    this.shardMap,
                    this.mapping,
                    this.lockOwnerId));
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
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                this.shardMap,
                this.mapping.StoreShard,
                this.errorCategory,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
        }

        /// <summary>
        /// Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalPostLocalUpdateCache(IStoreResults result)
        {
            // Remove from cache.
            this.Manager.Cache.DeleteMapping(this.mapping);
        }

        /// <summary>
        /// Performs the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
        {
            DefaultStoreMapping dsm = new DefaultStoreMapping(
                this.mapping.Id,
                this.shardMap.Id,
                new DefaultStoreShard(
                    this.mapping.StoreShard.Id,
                    this.OriginalShardVersionRemoves,
                    this.shardMap.Id,
                    this.mapping.StoreShard.Location,
                    this.mapping.StoreShard.Status),
                this.mapping.MinValue,
                this.mapping.MaxValue,
                this.mapping.Status,
                this.lockOwnerId);

            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                StoreOperationRequestBuilder.AddShardMappingLocal(
                    this.Id,
                    true,
                    this.shardMap,
                    dsm));
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
            throw StoreOperationErrorHandler.OnShardMapperErrorLocal(
                result,
                this.mapping.StoreShard.Location,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
        }

        /// <summary>
        /// Performs the undo of GSM operation after LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        public override IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd,
                StoreOperationRequestBuilder.RemoveShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    true, // undo
                    this.shardMap,
                    this.mapping,
                    this.lockOwnerId));
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
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                this.shardMap,
                this.mapping.StoreShard,
                this.errorCategory,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
        }

        /// <summary>
        /// Source location of error.
        /// </summary>
        protected override ShardLocation ErrorSourceLocation
        {
            get
            {
                return this.mapping.StoreShard.Location;
            }
        }

        /// <summary>
        /// Target location of error.
        /// </summary>
        protected override ShardLocation ErrorTargetLocation
        {
            get
            {
                return this.mapping.StoreShard.Location;
            }
        }

        /// <summary>
        /// Error category for error.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return this.errorCategory;
            }
        }
    }
}
