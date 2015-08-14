// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Obtains all the shard maps and shards from an LSM.
    /// </summary>
    internal class GetMappingsByRangeLocalOperation : StoreOperationLocal
    {
        /// <summary>
        /// Local shard map.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Local shard.
        /// </summary>
        private IStoreShard _shard;

        /// <summary>
        /// Range to get mappings from.
        /// </summary>
        private ShardRange _range;

        /// <summary>
        /// Ignore ShardMapNotFound error.
        /// </summary>
        private bool _ignoreFailure;

        /// <summary>
        /// Constructs request for obtaining all the shard maps and shards from an LSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="shard">Local shard.</param>
        /// <param name="range">Optional range to get mappings from.</param>
        /// <param name="ignoreFailure">Ignore shard map not found error.</param>
        internal GetMappingsByRangeLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardRange range,
            bool ignoreFailure) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, location, operationName)
        {
            Debug.Assert(shard != null);

            _shardMap = shardMap;
            _shard = shard;
            _range = range;
            _ignoreFailure = ignoreFailure;
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
        /// Execute the operation against LSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        public override IStoreResults DoLocalExecute(IStoreTransactionScope ts)
        {
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpGetAllShardMappingsLocal,
                StoreOperationRequestBuilder.GetAllShardMappingsLocal(_shardMap, _shard, _range));
        }

        /// <summary>
        /// Handles errors from the LSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoLocalExecuteError(IStoreResults result)
        {
            if (!_ignoreFailure || result.Result != StoreResult.ShardMapDoesNotExist)
            {
                // Possible errors are:
                // StoreResult.ShardMapDoesNotExist
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnRecoveryErrorLocal(
                    result,
                    _shardMap,
                    this.Location,
                    ShardManagementErrorCategory.Recovery,
                    this.OperationName,
                    StoreOperationRequestBuilder.SpGetAllShardMappingsLocal);
            }
        }
    }
}
