// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Add schema info to GSM.
    /// </summary>
    internal class AddShardingSchemaInfoGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Schema info to add.
        /// </summary>
        private IStoreSchemaInfo _schemaInfo;

        /// <summary>
        /// Constructs a request to add schema info to GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfo">Schema info to add.</param>
        internal AddShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreSchemaInfo schemaInfo) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _schemaInfo = schemaInfo;
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
                StoreOperationRequestBuilder.AddShardingSchemaInfoGlobal(_schemaInfo));
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
                _schemaInfo.Name,
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