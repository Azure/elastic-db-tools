// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Finds schema info for given name in GSM.
    /// </summary>
    internal class FindShardingSchemaInfoGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Shard map name for given schema info.
        /// </summary>
        private string _schemaInfoName;

        /// <summary>
        /// Constructs a request to find schema info in GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="schemaInfoName">Name of schema info to search.</param>
        internal FindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, string schemaInfoName) :
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
                StoreOperationRequestBuilder.SpFindShardingSchemaInfoByNameGlobal,
                StoreOperationRequestBuilder.FindShardingSchemaInfoGlobal(_schemaInfoName));
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            // SchemaInfoNameDoesNotExist is handled by the callers i.e. Get vs TryGet.
            if (result.Result != StoreResult.SchemaInfoNameDoesNotExist)
            {
                // Expected errors are:
                // StoreResult.MissingParametersForStoredProcedure:
                // StoreResult.StoreVersionMismatch:
                throw StoreOperationErrorHandler.OnShardSchemaInfoErrorGlobal(
                    result,
                    _schemaInfoName,
                    this.OperationName,
                    StoreOperationRequestBuilder.SpFindShardingSchemaInfoByNameGlobal);
            }
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