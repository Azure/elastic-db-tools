using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Add schema info to GSM.
    /// </summary>
    class AddShardingSchemaInfoGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Schema info to add.
        /// </summary>
        private IStoreSchemaInfo schemaInfo;

        /// <summary>
        /// Constructs a request to add schema info to GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfo">Schema info to add.</param>
        internal AddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreSchemaInfo schemaInfo) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            this.schemaInfo = schemaInfo;
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
                StoreOperationRequestBuilder.SpAddShardingSchemaInfoGlobal,
                StoreOperationRequestBuilder.AddShardingSchemaInfoGlobal(this.schemaInfo));
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            // Expected errors are:
            // StoreResult.SchemaInfoNameConflict:
            // StoreResult.MissingParametersForStoredProcedure:
            // StoreResult.StoreVersionMismatch:
            throw StoreOperationErrorHandler.OnShardSchemaInfoErrorGlobal(
                result,
                this.schemaInfo.Name,
                this.OperationName,
                StoreOperationRequestBuilder.SpAddShardingSchemaInfoGlobal);
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