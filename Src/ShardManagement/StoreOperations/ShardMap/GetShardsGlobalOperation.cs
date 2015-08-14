// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Gets all shards from given shard map from GSM.
    /// </summary>
    internal class GetShardsGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Shard map manager object.
        /// </summary>
        private ShardMapManager _shardMapManager;

        /// <summary>
        /// Shard map for which shards are being requested.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Constructs request to get all shards for given shard map from GSM.
        /// </summary>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="shardMap">Shard map for which shards are being requested.</param>
        internal GetShardsGlobalOperation(
            string operationName,
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _shardMapManager = shardMapManager;
            _shardMap = shardMap;
        }

        /// <summary>
        /// Whether this is a read-only operation.
        /// </summary>
        public override bool ReadOnly
        {
            get
            {
                return true;
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
                StoreOperationRequestBuilder.SpGetAllShardsGlobal,
                StoreOperationRequestBuilder.GetAllShardsGlobal(_shardMap));
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

            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapErrorGlobal(
                result,
                _shardMap,
                null, // shard
                ShardManagementErrorCategory.ShardMap,
                this.OperationName,
                StoreOperationRequestBuilder.SpGetAllShardsGlobal);
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
    }
}
