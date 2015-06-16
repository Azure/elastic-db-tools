using System.Data.SqlClient;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Removes given shard map from GSM.
    /// </summary>
    internal class RemoveShardMapGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Shard map manager object.
        /// </summary>
        private ShardMapManager shardMapManager;

        /// <summary>
        /// Shard map to remove.
        /// </summary>
        private IStoreShardMap shardMap;

        /// <summary>
        /// Constructs request to remove given shard map from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="shardMap">Shard map to remove.</param>
        internal RemoveShardMapGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            this.shardMapManager = shardMapManager;
            this.shardMap = shardMap;
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
                StoreOperationRequestBuilder.SpRemoveShardMapGlobal,
                StoreOperationRequestBuilder.RemoveShardMapGlobal(this.shardMap));
        }

        /// <summary>
        /// Invalidates the cache on unsuccessful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalUpdateCachePre(IStoreResults result)
        {
            if (result.Result == StoreResult.ShardMapDoesNotExist)
            {
                // Remove cache entry.
                this.shardMapManager.Cache.DeleteShardMap(this.shardMap);
            }
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            if (result.Result != StoreResult.ShardMapDoesNotExist)
            {
                // Possible errors are:
                // StoreResult.ShardMapHasShards
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnShardMapManagerErrorGlobal(
                    result,
                    this.shardMap,
                    this.OperationName,
                    StoreOperationRequestBuilder.SpRemoveShardMapGlobal);
            }
        }

        /// <summary>
        /// Refreshes the cache on successful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalUpdateCachePost(IStoreResults result)
        {
            Debug.Assert(result.Result == StoreResult.Success ||
                         result.Result == StoreResult.ShardMapDoesNotExist);

            if (result.Result == StoreResult.Success)
            {
                // Remove cache entry.
                this.shardMapManager.Cache.DeleteShardMap(this.shardMap);
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
