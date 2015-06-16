using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Detaches the given shard and corresponding mapping information to the GSM database.
    /// </summary>
    class DetachShardGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Location to be detached.
        /// </summary>
        private ShardLocation location;

        /// <summary>
        /// Shard map from which shard is being detached.
        /// </summary>
        private string shardMapName;

        /// <summary>
        /// Constructs request for Detaching the given shard and mapping information to the GSM database.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="location">Location to be detached.</param>
        /// <param name="shardMapName">Shard map from which shard is being detached.</param>
        internal DetachShardGlobalOperation(ShardMapManager shardMapManager, string operationName, ShardLocation location, string shardMapName) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            this.shardMapName = shardMapName;
            this.location = location;
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
                StoreOperationRequestBuilder.SpDetachShardGlobal,
                StoreOperationRequestBuilder.DetachShardGlobal(this.shardMapName, this.location));
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
            throw StoreOperationErrorHandler.OnRecoveryErrorGlobal(
                result,
                null,
                null,
                ShardManagementErrorCategory.Recovery,
                this.OperationName,
                StoreOperationRequestBuilder.SpDetachShardGlobal);
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
