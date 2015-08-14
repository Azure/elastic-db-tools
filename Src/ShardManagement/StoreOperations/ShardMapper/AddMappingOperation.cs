// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Adds a mapping to given shard map.
    /// </summary>
    internal class AddMappingOperation : StoreOperation
    {
        /// <summary>
        /// Shard map for which to add the mapping.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Mapping to add.
        /// </summary>
        private IStoreMapping _mapping;

        /// <summary>
        /// Error category to use.
        /// </summary>
        private ShardManagementErrorCategory _errorCategory;

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to add mapping.</param>
        /// <param name="mapping">Mapping to add.</param>
        protected internal AddMappingOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping) :
            this(
            shardMapManager,
            Guid.NewGuid(),
            StoreOperationState.UndoBegin,
            operationCode,
            shardMap,
            mapping,
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
        /// <param name="shardMap">Shard map for which to add mapping.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <param name="originalShardVersionAdds">Original shard version.</param>
        internal AddMappingOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid originalShardVersionAdds) :
            base(
            shardMapManager,
            operationId,
            undoStartState,
            operationCode,
            default(Guid),
            originalShardVersionAdds)
        {
            _shardMap = shardMap;
            _mapping = mapping;
            _errorCategory = operationCode == StoreOperationCode.AddRangeMapping ?
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
                SourceLocation = this.UndoStartState <= StoreOperationState.UndoLocalSourceBeginTransaction ? _mapping.StoreShard.Location : null
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
                StoreOperationRequestBuilder.AddShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    _shardMap,
                    _mapping));
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
            // StoreResult.MappingPointAlreadyMapped
            // StoreResult.MappingRangeAlreadyMapped
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                _shardMap,
                _mapping.StoreShard,
                _errorCategory,
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
                StoreOperationRequestBuilder.AddShardMappingLocal(
                    this.Id,
                    false,
                    _shardMap,
                    _mapping));
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
                _mapping.StoreShard.Location,
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
                StoreOperationRequestBuilder.AddShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    _shardMap,
                    _mapping));
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
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                _shardMap,
                _mapping.StoreShard,
                _errorCategory,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsGlobalEnd);
        }

        /// <summary>
        /// Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalPostLocalUpdateCache(IStoreResults result)
        {
            // Add mapping to cache.
            this.Manager.Cache.AddOrUpdateMapping(_mapping, CacheStoreMappingUpdatePolicy.OverwriteExisting);
        }

        /// <summary>
        /// Performs the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
        {
            DefaultStoreMapping dsm = new DefaultStoreMapping(
                _mapping.Id,
                _shardMap.Id,
                new DefaultStoreShard(
                    _mapping.StoreShard.Id,
                    this.OriginalShardVersionAdds,
                    _shardMap.Id,
                    _mapping.StoreShard.Location,
                    _mapping.StoreShard.Status),
                _mapping.MinValue,
                _mapping.MaxValue,
                _mapping.Status,
                default(Guid));

            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                StoreOperationRequestBuilder.RemoveShardMappingLocal(
                    this.Id,
                    true,
                    _shardMap,
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
                _mapping.StoreShard.Location,
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
                StoreOperationRequestBuilder.AddShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    true, // undo
                    _shardMap,
                    _mapping));
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
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                _shardMap,
                _mapping.StoreShard,
                _errorCategory,
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
                return _mapping.StoreShard.Location;
            }
        }

        /// <summary>
        /// Target location of error.
        /// </summary>
        protected override ShardLocation ErrorTargetLocation
        {
            get
            {
                return _mapping.StoreShard.Location;
            }
        }

        /// <summary>
        /// Error category for error.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return _errorCategory;
            }
        }
    }
}
