using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Delete schema info from GSM.
    /// </summary>
    class RemoveShardingSchemaInfoGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Name of schema info to remove.
        /// </summary>
        private string schemaInfoName;

        /// <summary>
        /// Constructs a request to delete schema info from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfoName">Name of schema info to delete.</param>
        internal RemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, string schemaInfoName) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            this.schemaInfoName = schemaInfoName;
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
                StoreOperationRequestBuilder.SpRemoveShardingSchemaInfoGlobal,
                StoreOperationRequestBuilder.RemoveShardingSchemaInfoGlobal(this.schemaInfoName));
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            // Expected errors are:
            // StoreResult.SchemaInfoNameDoesNotExist:
            // StoreResult.MissingParametersForStoredProcedure:
            // StoreResult.StoreVersionMismatch:
            throw StoreOperationErrorHandler.OnShardSchemaInfoErrorGlobal(
                result,
                this.schemaInfoName,
                this.OperationName,
                StoreOperationRequestBuilder.SpRemoveShardingSchemaInfoGlobal);
        }

        /// <summary>
        /// Error category for store exception.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return ShardManagementErrorCategory.SchemaInfoCollection;
            }
        }
    }
}