// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Locks or unlocks given mappings GSM.
    /// </summary>
    internal class LockOrUnLockMappingsGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Shard map manager object.
        /// </summary>
        private ShardMapManager _shardMapManager;

        /// <summary>
        /// Shard map to add.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Mapping to lock or unlock.
        /// </summary>
        private IStoreMapping _mapping;

        /// <summary>
        /// Lock owner id.
        /// </summary>
        private Guid _lockOwnerId;

        /// <summary>
        /// Operation type.
        /// </summary>
        private LockOwnerIdOpType _lockOpType;

        /// <summary>
        /// Error category to use.
        /// </summary>
        private ShardManagementErrorCategory _errorCategory;

        /// <summary>
        /// Constructs request to lock or unlock given mappings in GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map to add.</param>
        /// <param name="mapping">Mapping to lock or unlock. Null means all mappings.</param>
        /// <param name="lockOwnerId">Lock owner.</param>
        /// <param name="lockOpType">Lock operation type.</param>
        /// <param name="errorCategory">Error category.</param>
        internal LockOrUnLockMappingsGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreMapping mapping, Guid lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _shardMapManager = shardMapManager;
            _shardMap = shardMap;
            _mapping = mapping;
            _lockOwnerId = lockOwnerId;
            _lockOpType = lockOpType;
            _errorCategory = errorCategory;

            Debug.Assert(mapping != null || (lockOpType == LockOwnerIdOpType.UnlockAllMappingsForId || lockOpType == LockOwnerIdOpType.UnlockAllMappings));
        }

        /// <summary>
        /// Whether this is a read-only operation.
        /// </summary>
        public override bool ReadOnly
        {
            get
            {
                return false;
            }
        }

        /// <summary>
        /// Execute the operation against GSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        public override IStoreResults DoGlobalExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpLockOrUnLockShardMappingsGlobal,
                StoreOperationRequestBuilder.LockOrUnLockShardMappingsGlobal(
                    _shardMap,
                    _mapping,
                    _lockOwnerId,
                    _lockOpType));
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            if (result.Result == StoreResult.ShardMapDoesNotExist)
            {
                // Remove shard map from cache.
                _shardMapManager.Cache.DeleteShardMap(_shardMap);
            }

            if (result.Result == StoreResult.MappingDoesNotExist)
            {
                Debug.Assert(_mapping != null);

                // Remove mapping from cache.
                _shardMapManager.Cache.DeleteMapping(_mapping);
            }

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.MappingDoesNotExist
            // StoreResult.MappingAlreadyLocked
            // StoreResult.MappingLockOwnerIdMismatch
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                _shardMap,
                _mapping == null ? null : _mapping.StoreShard,
                _errorCategory,
                this.OperationName,
                StoreOperationRequestBuilder.SpLockOrUnLockShardMappingsGlobal);
        }

        /// <summary>
        /// Error category for store exception.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return ShardManagementErrorCategory.ShardMap;
            }
        }

        /// <summary>
        /// Performs undo of the storage operation that is pending.
        /// </summary>
        /// <param name="logEntry">Log entry for the pending operation.</param>
        protected override void UndoPendingStoreOperations(IStoreLogEntry logEntry)
        {
            using (IStoreOperation op = _shardMapManager.StoreOperationFactory.FromLogEntry(_shardMapManager, logEntry))
            {
                op.Undo();
            }
        }
    }
}
