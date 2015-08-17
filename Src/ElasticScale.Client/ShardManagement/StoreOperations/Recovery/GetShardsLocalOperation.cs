// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Obtains all the shard maps and shards from an LSM.
    /// </summary>
    internal class GetShardsLocalOperation : StoreOperationLocal
    {
        /// <summary>
        /// Constructs request for obtaining all the shard maps and shards from an LSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        /// <param name="operationName">Operatio name.</param>
        internal GetShardsLocalOperation(ShardMapManager shardMapManager, ShardLocation location, string operationName) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, location, operationName)
        {
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
        /// Execute the operation against LSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        public override IStoreResults DoLocalExecute(IStoreTransactionScope ts)
        {
            IStoreResults result = ts.ExecuteCommandSingle(SqlUtils.CheckIfExistsLocalScript.Single());

            if (result.StoreVersion == null)
            {
                // Shard not deployed, which is an error condition.
                throw new ShardManagementException(
                    ShardManagementErrorCategory.Recovery,
                    ShardManagementErrorCode.ShardNotValid,
                    Errors._Recovery_ShardNotValid,
                    this.Location,
                    this.OperationName);
            }

            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpGetAllShardsLocal,
                StoreOperationRequestBuilder.GetAllShardsLocal());
        }

        /// <summary>
        /// Handles errors from the LSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoLocalExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnRecoveryErrorLocal(
                result,
                null,
                this.Location,
                ShardManagementErrorCategory.Recovery,
                this.OperationName,
                StoreOperationRequestBuilder.SpGetAllShardsLocal);
        }
    }
}
