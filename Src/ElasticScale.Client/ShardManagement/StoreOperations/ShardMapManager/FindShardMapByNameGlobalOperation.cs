﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Finds shard map with given name from GSM.
    /// </summary>
    internal class FindShardMapByNameGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Shard map manager object.
        /// </summary>
        private ShardMapManager _shardMapManager;

        /// <summary>
        /// Name of shard map being searched.
        /// </summary>
        private string _shardMapName;

        /// <summary>
        /// Constructs request to find shard map with given name from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMapName">Name of the shard map being searched.</param>
        internal FindShardMapByNameGlobalOperation(ShardMapManager shardMapManager, string operationName, string shardMapName) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _shardMapManager = shardMapManager;
            _shardMapName = shardMapName;
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
                StoreOperationRequestBuilder.SpFindShardMapByNameGlobal,
                StoreOperationRequestBuilder.FindShardMapByNameGlobal(_shardMapName));
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnShardMapManagerErrorGlobal(
                result,
                null,
                this.OperationName,
                StoreOperationRequestBuilder.SpFindShardMapByNameGlobal);
        }

        /// <summary>
        /// Refreshes the cache on successful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Result of the operation.</param>
        public override void DoGlobalUpdateCachePost(IStoreResults result)
        {
            Debug.Assert(result.Result == StoreResult.Success);

            // Add cache entry.
            foreach (IStoreShardMap ssm in result.StoreShardMaps)
            {
                _shardMapManager.Cache.AddOrUpdateShardMap(ssm);
            }
        }

        /// <summary>
        /// Error category for store exception.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return ShardManagementErrorCategory.ShardMapManager;
            }
        }
    }
}
