// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Obtains all the mappings from the GSM based on given shard and range.
    /// </summary>
    internal class GetMappingsByRangeGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Shard map manager instance.
        /// </summary>
        private ShardMapManager _manager;

        /// <summary>
        /// Shard map for which mappings are requested.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Optional shard which has the mappings.
        /// </summary>
        private IStoreShard _shard;

        /// <summary>
        /// Optional range to get mappings for.
        /// </summary>
        private ShardRange _range;

        /// <summary>
        /// Error category to use.
        /// </summary>
        private ShardManagementErrorCategory _errorCategory;

        /// <summary>
        /// Whether to cache the results.
        /// </summary>
        private bool _cacheResults;

        /// <summary>
        /// Ignore ShardMapNotFound error.
        /// </summary>
        private bool _ignoreFailure;


        /// <summary>
        /// Constructs request for obtaining all the mappings from GSM based on given shard and mappings.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation being executed.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="shard">Local shard.</param>
        /// <param name="range">Optional range to get mappings from.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <param name="cacheResults">Whether to cache the results of the operation.</param>
        /// <param name="ignoreFailure">Ignore shard map not found error.</param>
        internal GetMappingsByRangeGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreShard shard, ShardRange range, ShardManagementErrorCategory errorCategory, bool cacheResults, bool ignoreFailure) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _manager = shardMapManager;
            _shardMap = shardMap;
            _shard = shard;
            _range = range;
            _errorCategory = errorCategory;
            _cacheResults = cacheResults;
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
        /// Execute the operation against GSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        public override IStoreResults DoGlobalExecute(IStoreTransactionScope ts)
        {
            // If no ranges are specified, blindly mark everything for deletion.
            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal,
                StoreOperationRequestBuilder.GetAllShardMappingsGlobal(_shardMap, _shard, _range));
        }

        /// <summary>
        /// Invalidates the cache on unsuccessful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalUpdateCachePre(IStoreResults result)
        {
            if (result.Result == StoreResult.ShardMapDoesNotExist)
            {
                // Remove shard map from cache.
                _manager.Cache.DeleteShardMap(_shardMap);
            }
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            // Recovery manager handles the ShardMapDoesNotExist error properly, so we don't interfere.
            if (!_ignoreFailure || result.Result != StoreResult.ShardMapDoesNotExist)
            {
                // Possible errors are:
                // StoreResult.ShardMapDoesNotExist
                // StoreResult.ShardDoesNotExist
                // StoreResult.ShardVersionMismatch
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                    result,
                    _shardMap,
                    _shard,
                    _errorCategory,
                    this.OperationName,
                    StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal);
            }
        }

        /// <summary>
        /// Refreshes the cache on successful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalUpdateCachePost(IStoreResults result)
        {
            if (result.Result == StoreResult.Success && _cacheResults)
            {
                foreach (IStoreMapping sm in result.StoreMappings)
                {
                    _manager.Cache.AddOrUpdateMapping(sm, CacheStoreMappingUpdatePolicy.OverwriteExisting);
                }
            }
        }

        /// <summary>
        /// Error category for store exception.
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
