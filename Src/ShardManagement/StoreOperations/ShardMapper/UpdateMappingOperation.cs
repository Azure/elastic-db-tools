// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Updates a mapping in given shard map.
    /// </summary>
    internal class UpdateMappingOperation : StoreOperation
    {
        /// <summary>
        /// Shard map for which to update the mapping.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Mapping to update.
        /// </summary>
        private IStoreMapping _mappingSource;

        /// <summary>
        /// Updated mapping.
        /// </summary>
        private IStoreMapping _mappingTarget;

        /// <summary>
        /// Lock owner.
        /// </summary>
        private Guid _lockOwnerId;

        /// <summary>
        /// Error category to use.
        /// </summary>
        private ShardManagementErrorCategory _errorCategory;

        /// <summary>
        /// Is this a shard location update operation.
        /// </summary>
        private bool _updateLocation;

        /// <summary>
        /// Is mapping being taken offline.
        /// </summary>
        private bool _fromOnlineToOffline;

        /// <summary>
        /// Pattern for kill commands.
        /// </summary>
        private string _patternForKill;

        /// <summary>
        /// Creates request to add shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationCode">Store operation code.</param>
        /// <param name="shardMap">Shard map for which to update mapping.</param>
        /// <param name="mappingSource">Mapping to update.</param>
        /// <param name="mappingTarget">Updated mapping.</param>
        /// <param name="patternForKill">Pattern for kill commands.</param>
        /// <param name="lockOwnerId">Id of lock owner.</param>
        protected internal UpdateMappingOperation(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget,
            string patternForKill,
            Guid lockOwnerId) :
            this(
            shardMapManager,
            Guid.NewGuid(),
            StoreOperationState.UndoBegin,
            operationCode,
            shardMap,
            mappingSource,
            mappingTarget,
            patternForKill,
            lockOwnerId,
            default(Guid),
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
        /// <param name="shardMap">Shard map for which to update mapping.</param>
        /// <param name="mappingSource">Mapping to update.</param>
        /// <param name="mappingTarget">Updated mapping.</param>
        /// <param name="patternForKill">Pattern for kill commands.</param>
        /// <param name="lockOwnerId">Id of lock owner.</param>
        /// <param name="originalShardVersionRemoves">Original shard version for removes.</param>
        /// <param name="originalShardVersionAdds">Original shard version for adds.</param>
        internal UpdateMappingOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget,
            string patternForKill,
            Guid lockOwnerId,
            Guid originalShardVersionRemoves,
            Guid originalShardVersionAdds) :
            base(
            shardMapManager,
            operationId,
            undoStartState,
            operationCode,
            originalShardVersionRemoves,
            originalShardVersionAdds)
        {
            _shardMap = shardMap;
            _mappingSource = mappingSource;
            _mappingTarget = mappingTarget;
            _lockOwnerId = lockOwnerId;

            _errorCategory = (operationCode == StoreOperationCode.UpdateRangeMapping ||
                                  operationCode == StoreOperationCode.UpdateRangeMappingWithOffline) ?
                ShardManagementErrorCategory.RangeShardMap :
                ShardManagementErrorCategory.ListShardMap;

            _updateLocation = _mappingSource.StoreShard.Id != _mappingTarget.StoreShard.Id;

            _fromOnlineToOffline = operationCode == StoreOperationCode.UpdatePointMappingWithOffline ||
                                       operationCode == StoreOperationCode.UpdateRangeMappingWithOffline;

            _patternForKill = patternForKill;
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
                SourceLocation = this.UndoStartState <= StoreOperationState.UndoLocalSourceBeginTransaction ? _mappingSource.StoreShard.Location : null,
                TargetLocation = (_updateLocation && this.UndoStartState <= StoreOperationState.UndoLocalTargetBeginTransaction) ? _mappingTarget.StoreShard.Location : null
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
                StoreOperationRequestBuilder.UpdateShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    _patternForKill,
                    _shardMap,
                    _mappingSource,
                    _mappingTarget,
                    _lockOwnerId));
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
                // Remove mapping from cache.
                this.Manager.Cache.DeleteMapping(_mappingSource);
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
                result.Result == StoreResult.MappingRangeAlreadyMapped ?
                    _mappingTarget.StoreShard :
                    _mappingSource.StoreShard,
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
            IStoreResults result;

            if (_updateLocation)
            {
                result = ts.ExecuteOperation(
                    StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                    StoreOperationRequestBuilder.RemoveShardMappingLocal(
                        this.Id,
                        false,
                        _shardMap,
                        _mappingSource));
            }
            else
            {
                result = ts.ExecuteOperation(
                    StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                    StoreOperationRequestBuilder.UpdateShardMappingLocal(
                        this.Id,
                        false,
                        _shardMap,
                        _mappingSource,
                        _mappingTarget));
            }

            // We need to treat the kill connection operation separately, the reason
            // being that we cannot perform kill operations within a transaction.
            if (result.Result == StoreResult.Success && _fromOnlineToOffline)
            {
                this.KillConnectionsOnSourceShard();
            }

            return result;
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
                _mappingSource.StoreShard.Location,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
        }

        /// <summary>
        /// Performs the LSM operation on the target shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            Debug.Assert(_updateLocation);
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                StoreOperationRequestBuilder.AddShardMappingLocal(
                    this.Id,
                    false,
                    _shardMap,
                    _mappingTarget));
        }

        /// <summary>
        /// Handles errors from the the LSM operation on the target shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoLocalTargetExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.UnableToKillSessions
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapperErrorLocal(
                result,
                _mappingTarget.StoreShard.Location,
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
                StoreOperationRequestBuilder.UpdateShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    false, // undo
                    _patternForKill,
                    _shardMap,
                    _mappingSource,
                    _mappingTarget,
                    _lockOwnerId));
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
                _mappingSource.StoreShard,
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
            // Remove from cache.
            this.Manager.Cache.DeleteMapping(_mappingSource);

            // Add to cache.
            this.Manager.Cache.AddOrUpdateMapping(_mappingTarget, CacheStoreMappingUpdatePolicy.OverwriteExisting);
        }

        /// <summary>
        /// Performs the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
        {
            DefaultStoreMapping dsmSource = new DefaultStoreMapping(
                _mappingSource.Id,
                _shardMap.Id,
                new DefaultStoreShard(
                    _mappingSource.StoreShard.Id,
                    this.OriginalShardVersionRemoves,
                    _shardMap.Id,
                    _mappingSource.StoreShard.Location,
                    _mappingSource.StoreShard.Status),
                _mappingSource.MinValue,
                _mappingSource.MaxValue,
                _mappingSource.Status,
                _lockOwnerId);

            if (_updateLocation)
            {
                return ts.ExecuteOperation(
                    StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                    StoreOperationRequestBuilder.AddShardMappingLocal(
                        this.Id,
                        true,
                        _shardMap,
                        dsmSource));
            }
            else
            {
                DefaultStoreMapping dsmTarget = new DefaultStoreMapping(
                    _mappingTarget.Id,
                    _shardMap.Id,
                    new DefaultStoreShard(
                        _mappingTarget.StoreShard.Id,
                        this.OriginalShardVersionRemoves,
                        _shardMap.Id,
                        _mappingTarget.StoreShard.Location,
                        _mappingTarget.StoreShard.Status),
                    _mappingTarget.MinValue,
                    _mappingTarget.MaxValue,
                    _mappingTarget.Status,
                    _lockOwnerId);

                return ts.ExecuteOperation(
                    StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                    StoreOperationRequestBuilder.UpdateShardMappingLocal(
                        this.Id,
                        true,
                        _shardMap,
                        dsmTarget,
                        dsmSource));
            }
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
                _mappingSource.StoreShard.Location,
                StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
        }

        /// <summary>
        /// Performs undo of LSM operation on the target shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        public override IStoreResults UndoLocalTargetExecute(IStoreTransactionScope ts)
        {
            DefaultStoreMapping dsmTarget = new DefaultStoreMapping(
                                    _mappingTarget.Id,
                                    _shardMap.Id,
                                    new DefaultStoreShard(
                                        _mappingTarget.StoreShard.Id,
                                        this.OriginalShardVersionAdds,
                                        _shardMap.Id,
                                        _mappingTarget.StoreShard.Location,
                                        _mappingTarget.StoreShard.Status),
                                    _mappingTarget.MinValue,
                                    _mappingTarget.MaxValue,
                                    _mappingTarget.Status,
                                    _lockOwnerId);

            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                StoreOperationRequestBuilder.RemoveShardMappingLocal(
                    this.Id,
                    true,
                    _shardMap,
                    dsmTarget));
        }

        /// <summary>
        /// Performs undo of LSM operation on the target shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleUndoLocalTargetExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapperErrorLocal(
                result,
                _mappingSource.StoreShard.Location,
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
                StoreOperationRequestBuilder.UpdateShardMappingGlobal(
                    this.Id,
                    this.OperationCode,
                    true, // undo
                    _patternForKill,
                    _shardMap,
                    _mappingSource,
                    _mappingTarget,
                    _lockOwnerId));
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
                _mappingSource.StoreShard,
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
                return _mappingSource.StoreShard.Location;
            }
        }

        /// <summary>
        /// Target location of error.
        /// </summary>
        protected override ShardLocation ErrorTargetLocation
        {
            get
            {
                return _mappingTarget.StoreShard.Location;
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

        /// <summary>
        /// Terminates connection on the source shard object.
        /// </summary>
        private void KillConnectionsOnSourceShard()
        {
            SqlUtils.WithSqlExceptionHandling(() =>
            {
                string sourceShardConnectionString = this.GetConnectionStringForShardLocation(_mappingSource.StoreShard.Location);

                IStoreResults result;

                using (IStoreConnection connectionForKill = this.Manager.StoreConnectionFactory.GetConnection(StoreConnectionKind.LocalSource, sourceShardConnectionString))
                {
                    connectionForKill.Open();

                    using (IStoreTransactionScope ts = connectionForKill.GetTransactionScope(StoreTransactionScopeKind.NonTransactional))
                    {
                        result = ts.ExecuteOperation(
                            StoreOperationRequestBuilder.SpKillSessionsForShardMappingLocal,
                            StoreOperationRequestBuilder.KillSessionsForShardMappingLocal(_patternForKill));
                    }
                }

                if (result.Result != StoreResult.Success)
                {
                    // Possible errors are:
                    // StoreResult.UnableToKillSessions
                    // StoreResult.StoreVersionMismatch
                    // StoreResult.MissingParametersForStoredProcedure
                    throw StoreOperationErrorHandler.OnShardMapErrorLocal(
                        result,
                        _shardMap,
                        _mappingSource.StoreShard.Location,
                        _errorCategory,
                        StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                        StoreOperationRequestBuilder.SpKillSessionsForShardMappingLocal);
                }
            });
        }
    }
}
