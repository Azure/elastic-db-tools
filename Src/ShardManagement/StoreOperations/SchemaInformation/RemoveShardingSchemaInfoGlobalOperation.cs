// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Delete schema info from GSM.
    /// </summary>
    internal class RemoveShardingSchemaInfoGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Name of schema info to remove.
        /// </summary>
        private string _schemaInfoName;

        /// <summary>
        /// Constructs a request to delete schema info from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfoName">Name of schema info to delete.</param>
        internal RemoveShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, string schemaInfoName) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _schemaInfoName = schemaInfoName;
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
                StoreOperationRequestBuilder.RemoveShardingSchemaInfoGlobal(_schemaInfoName));
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
                _schemaInfoName,
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