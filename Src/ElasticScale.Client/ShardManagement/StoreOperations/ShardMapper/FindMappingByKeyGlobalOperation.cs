// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Obtains the mapping from the GSM based on given key.
    /// </summary>
    internal class FindMappingByKeyGlobalOperation : StoreOperationGlobal
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
        /// Key being searched.
        /// </summary>
        private ShardKey _key;

        /// <summary>
        /// Policy for cache update.
        /// </summary>
        private CacheStoreMappingUpdatePolicy _policy;

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
        /// Constructs request for obtaining mapping from GSM based on given key.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation being executed.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="key">Key for lookup operation.</param>
        /// <param name="policy">Policy for cache update.</param>
        /// <param name="errorCategory">Error category.</param>
        /// <param name="cacheResults">Whether to cache the results of the operation.</param>
        /// <param name="ignoreFailure">Ignore shard map not found error.</param>
        protected internal FindMappingByKeyGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            IStoreShardMap shardMap,
            ShardKey key,
            CacheStoreMappingUpdatePolicy policy,
            ShardManagementErrorCategory errorCategory,
            bool cacheResults,
            bool ignoreFailure) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _manager = shardMapManager;
            _shardMap = shardMap;
            _key = key;
            _policy = policy;
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
                StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal,
                StoreOperationRequestBuilder.FindShardMappingByKeyGlobal(_shardMap, _key));
        }

        /// <summary>
        /// Asynchronously execute the operation against GSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Task encapsulating results of the operation.
        /// </returns>
        public override Task<IStoreResults> DoGlobalExecuteAsync(IStoreTransactionScope ts)
        {
            // If no ranges are specified, blindly mark everything for deletion.
            return ts.ExecuteOperationAsync(
                StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal,
                StoreOperationRequestBuilder.FindShardMappingByKeyGlobal(_shardMap, _key));
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
            // MappingNotFound for key is supposed to be handled in the calling layers 
            // so that TryLookup vs Lookup have proper behavior.
            if (result.Result != StoreResult.MappingNotFoundForKey)
            {
                // Recovery manager handles the ShardMapDoesNotExist error properly, so we don't interfere.
                if (!_ignoreFailure || result.Result != StoreResult.ShardMapDoesNotExist)
                {
                    // Possible errors are:
                    // StoreResult.ShardMapDoesNotExist
                    // StoreResult.StoreVersionMismatch
                    // StoreResult.MissingParametersForStoredProcedure
                    throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                        result,
                        _shardMap,
                        null, // shard
                        _errorCategory,
                        this.OperationName,
                        StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal);
                }
            }
        }

        /// <summary>
        /// Refreshes the cache on successful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalUpdateCachePost(IStoreResults result)
        {
            Debug.Assert(
                result.Result == StoreResult.Success ||
                result.Result == StoreResult.MappingNotFoundForKey ||
                result.Result == StoreResult.ShardMapDoesNotExist);

            if (result.Result == StoreResult.Success && _cacheResults)
            {
                foreach (IStoreMapping sm in result.StoreMappings)
                {
                    _manager.Cache.AddOrUpdateMapping(sm, _policy);
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
