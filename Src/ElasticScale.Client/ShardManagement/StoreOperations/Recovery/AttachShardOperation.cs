// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Attaches the given shard map and shard information to the GSM database and update shard location in LSM.
    /// <remarks>UndoLocalSourceExecute is not implemented for this operation because UpdateShardLocal operation is needed
    /// irrespective of the success/error condition, it updates Shards table with correct location of the Shard.</remarks>
    /// </summary>
    internal class AttachShardOperation : StoreOperation
    {
        /// <summary>
        /// Shard map to attach the shard.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Shard to attach.
        /// </summary>
        private IStoreShard _shard;

        /// <summary>
        /// Creates request to attach shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which to attach shard.</param>
        /// <param name="shard">Shard to attach.</param>
        protected internal AttachShardOperation(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard) :
            this(shardMapManager, Guid.NewGuid(), StoreOperationState.UndoBegin, shardMap, shard)
        {
        }

        /// <summary>
        /// Creates request to attach shard to given shard map.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationId">Operation id.</param>
        /// <param name="undoStartState">State from which Undo operation starts.</param>
        /// <param name="shardMap">Shard map for which to attach shard.</param>
        /// <param name="shard">Shard to attach.</param>
        internal AttachShardOperation(
            ShardMapManager shardMapManager,
            Guid operationId,
            StoreOperationState undoStartState,
            IStoreShardMap shardMap,
            IStoreShard shard) :
            base(shardMapManager, operationId, undoStartState, StoreOperationCode.AttachShard, default(Guid), default(Guid))
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
                StoreOperationRequestBuilder.AddShardGlobal(
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
            // StoreResult.ShardExists
            // StoreResult.ShardLocationExists
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorGlobal(
                result,
                _shardMap,
                _shard,
                ShardManagementErrorCategory.Recovery,
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
            // There should already be some version of LSM at this location as RecoveryMAnager.AttachShard() first reads existing shard maps from this location.

            IStoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.CheckIfExistsLocalScript.Single());
            Debug.Assert(checkResult.StoreVersion != null);

            // Upgrade local shard map to latest version before attaching.
            ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.UpgradeLocalScript, GlobalConstants.LsmVersionClient, checkResult.StoreVersion.Version));

            // Now update the shards table in LSM.
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpUpdateShardLocal,
                StoreOperationRequestBuilder.UpdateShardLocal(this.Id, _shardMap, _shard));
        }

        /// <summary>
        /// Handles errors from the the LSM operation on the source shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoLocalSourceExecuteError(IStoreResults result)
        {
            // Possible errors from spUpdateShardLocal:
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            // StoreResult.ShardDoesNotExist
            switch (result.Result)
            {
                case StoreResult.StoreVersionMismatch:
                case StoreResult.MissingParametersForStoredProcedure:
                case StoreResult.ShardDoesNotExist:
                    throw StoreOperationErrorHandler.OnShardMapErrorLocal(
                        result,
                        _shardMap,
                        _shard.Location,
                        ShardManagementErrorCategory.ShardMap,
                        StoreOperationErrorHandler.OperationNameFromStoreOperationCode(this.OperationCode),
                        StoreOperationRequestBuilder.SpUpdateShardLocal);

                default:
                    throw new ShardManagementException(
                        ShardManagementErrorCategory.ShardMapManager,
                        ShardManagementErrorCode.StorageOperationFailure,
                        Errors._Store_SqlExceptionLocal,
                        OperationName
                        );
            }
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
                StoreOperationRequestBuilder.AddShardGlobal(
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
            // as part of local source execute step, we just update shard location to reflect correct
            // servername and database name, so there is no need to undo that action.
            return new SqlResults();
        }

        /// <summary>
        /// Handles errors from the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleUndoLocalSourceExecuteError(IStoreResults result)
        {
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
                StoreOperationRequestBuilder.AddShardGlobal(
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
        /// Error category for store exception.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return ShardManagementErrorCategory.Recovery;
            }
        }
    }
}
