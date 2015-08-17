// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Obtains the mapping by Id from the GSM.
    /// </summary>
    internal class FindMappingByIdGlobalOperation : StoreOperationGlobal
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
        /// Mapping whose Id will be used.
        /// </summary>
        private IStoreMapping _mapping;

        /// <summary>
        /// Error category to use.
        /// </summary>
        private ShardManagementErrorCategory _errorCategory;

        /// <summary>
        /// Constructs request for obtaining mapping from GSM based on given key.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation being executed.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="mapping">Mapping whose Id will be used.</param>
        /// <param name="errorCategory">Error category.</param>
        internal FindMappingByIdGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreMapping mapping, ShardManagementErrorCategory errorCategory) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _manager = shardMapManager;
            _shardMap = shardMap;
            _mapping = mapping;
            _errorCategory = errorCategory;
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
                StoreOperationRequestBuilder.SpFindShardMappingByIdGlobal,
                StoreOperationRequestBuilder.FindShardMappingByIdGlobal(_shardMap, _mapping));
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

            if (result.Result == StoreResult.MappingDoesNotExist)
            {
                // Remove mapping from cache.
                _manager.Cache.DeleteMapping(_mapping);
            }
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.MappingDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                result,
                _shardMap,
                _mapping.StoreShard, // shard
                _errorCategory,
                this.OperationName,
                StoreOperationRequestBuilder.SpFindShardMappingByIdGlobal);
        }

        /// <summary>
        /// Refreshes the cache on successful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalUpdateCachePost(IStoreResults result)
        {
            Debug.Assert(result.Result == StoreResult.Success);
            foreach (IStoreMapping sm in result.StoreMappings)
            {
                _manager.Cache.AddOrUpdateMapping(sm, CacheStoreMappingUpdatePolicy.OverwriteExisting);
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
