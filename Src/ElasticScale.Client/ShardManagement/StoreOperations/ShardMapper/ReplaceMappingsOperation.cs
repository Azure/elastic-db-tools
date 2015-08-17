// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Replaces existing mappings with new mappings in given shard map.
    /// </summary>
    internal class ReplaceMappingsOperation : StoreOperation
    {
        /// <summary>
        /// Shard map for which to perform operation.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Original mappings.
        /// </summary>
        private Tuple<IStoreMapping, Guid>[] _mappingsSource;

        /// <summary>
        /// New mappings.
        /// </summary>
        private Tuple<IStoreMapping, Guid>[] _mappingsTarget;

        /// <summary>
        /// Creates request to replace mappings within shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to update mapping.</param>
        /// <param name="mappingsSource">Original mappings.</param>
        /// <param name="mappingsTarget">Target mappings mapping.</param>
        protected internal ReplaceMappingsOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            Tuple<IStoreMapping, Guid>[] mappingsSource,
            Tuple<IStoreMapping, Guid>[] mappingsTarget) :
            this(
            shardMapManager,
            Guid.NewGuid(),
            StoreOperationState.UndoBegin,
            operationCode,
            shardMap,
            mappingsSource,
            mappingsTarget,
            default(Guid))
        {
        }

        /// <summary>
        /// Creates request to replace mappings within shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation id.</param>
        /// <param name="undoStartState">State from which Undo operation starts.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to update mapping.</param>
        /// <param name="mappingsSource">Original mappings.</param>
        /// <param name="mappingsTarget">Target mappings mapping.</param>
        /// <param name="originalShardVersionAdds">Original shard version on source.</param>
        internal ReplaceMappingsOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            Tuple<IStoreMapping, Guid>[] mappingsSource,
            Tuple<IStoreMapping, Guid>[] mappingsTarget,
            Guid originalShardVersionAdds) :
            base(
            shardMapManager,
            operationId,
            undoStartState,
            operationCode,
            originalShardVersionAdds,
            originalShardVersionAdds)
        {
            _shardMap = shardMap;
            _mappingsSource = mappingsSource;
            _mappingsTarget = mappingsTarget;
        }

        /// <summary>
        /// Requests the derived class to provide information regarding the connections
        /// needed for the operation.
        /// </summary>
        /// <returns>Information about shards involved in the operation.</returns>
        public override StoreConnectionInfo GetStoreConnectionInfo()
        {
            Debug.Assert(_mappingsSource.Length > 0);
            return new StoreConnectionInfo()
            {
                SourceLocation = this.UndoStartState <= StoreOperationState.UndoLocalSourceBeginTransaction ?
                    _mappingsSource[0].Item1.StoreShard.Location :
                    null
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
                StoreOperationRequestBuilder.ReplaceShardMappingsGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    _shardMap,
                    _mappingsSource,
                    _mappingsTarget));
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

            if (result.Result == StoreResult.MappingDoesNotExist)
            {
                foreach (IStoreMapping mappingSource in _mappingsSource.Select(m => m.Item1))
                {
                    // Remove mapping from cache.
                    this.Manager.Cache.DeleteMapping(mappingSource);
                }
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.ShardDoesNotExist
            // StoreResult.MappingRangeAlreadyMapped
            // StoreResult.MappingDoesNotExist
            // StoreResult.MappingLockOwnerIdDoesNotMatch
            // StoreResult.MappingIsNotOffline
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                _shardMap,
                _mappingsSource[0].Item1.StoreShard,
                ShardManagementErrorCategory.RangeShardMap,
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
                StoreOperationRequestBuilder.ReplaceShardMappingsLocal(
                    this.Id,
                    false,
                    _shardMap,
                    _mappingsSource.Select(m => m.Item1).ToArray(),
                    _mappingsTarget.Select(m => m.Item1).ToArray()));
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
                _mappingsSource[0].Item1.StoreShard.Location,
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
                StoreOperationRequestBuilder.ReplaceShardMappingsGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    _shardMap,
                    _mappingsSource,
                    _mappingsTarget));
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
                _mappingsSource[0].Item1.StoreShard,
                ShardManagementErrorCategory.RangeShardMap,
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
            foreach (Tuple<IStoreMapping, Guid> ssm in _mappingsSource)
            {
                this.Manager.Cache.DeleteMapping(ssm.Item1);
            }

            // Add to cache.
            foreach (Tuple<IStoreMapping, Guid> ssm in _mappingsTarget)
            {
                this.Manager.Cache.AddOrUpdateMapping(ssm.Item1, CacheStoreMappingUpdatePolicy.OverwriteExisting);
            }
        }

        /// <summary>
        /// Performs the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
        {
            DefaultStoreShard dssOriginal = new DefaultStoreShard(
                _mappingsSource[0].Item1.StoreShard.Id,
                this.OriginalShardVersionAdds,
                _mappingsSource[0].Item1.ShardMapId,
                _mappingsSource[0].Item1.StoreShard.Location,
                _mappingsSource[0].Item1.StoreShard.Status);

            DefaultStoreMapping dsmSource = new DefaultStoreMapping(
                _mappingsSource[0].Item1.Id,
                _mappingsSource[0].Item1.ShardMapId,
                dssOriginal,
                _mappingsSource[0].Item1.MinValue,
                _mappingsSource[0].Item1.MaxValue,
                _mappingsSource[0].Item1.Status,
                _mappingsSource[0].Item2);

            DefaultStoreMapping dsmTarget = new DefaultStoreMapping(
                _mappingsTarget[0].Item1.Id,
                _mappingsTarget[0].Item1.ShardMapId,
                dssOriginal,
                _mappingsTarget[0].Item1.MinValue,
                _mappingsTarget[0].Item1.MaxValue,
                _mappingsTarget[0].Item1.Status,
                _mappingsTarget[0].Item2);

            IStoreMapping[] ms = new[] { dsmSource };
            IStoreMapping[] mt = new[] { dsmTarget };

            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                StoreOperationRequestBuilder.ReplaceShardMappingsLocal(
                    this.Id,
                    true,
                    _shardMap,
                    mt.Concat(_mappingsTarget.Skip(1).Select(m => m.Item1)).ToArray(),
                    ms.Concat(_mappingsSource.Skip(1).Select(m => m.Item1)).ToArray()));
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
                _mappingsSource[0].Item1.StoreShard.Location,
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
                StoreOperationRequestBuilder.ReplaceShardMappingsGlobal(
                    this.Id,
                    this.OperationCode,
                    true, // undo
                    _shardMap,
                    _mappingsSource,
                    _mappingsTarget));
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
                _mappingsSource[0].Item1.StoreShard,
                ShardManagementErrorCategory.RangeShardMap,
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
                return _mappingsSource[0].Item1.StoreShard.Location;
            }
        }

        /// <summary>
        /// Target location of error.
        /// </summary>
        protected override ShardLocation ErrorTargetLocation
        {
            get
            {
                return _mappingsSource[0].Item1.StoreShard.Location;
            }
        }

        /// <summary>
        /// Error category for error.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return ShardManagementErrorCategory.RangeShardMap;
            }
        }
    }
}
