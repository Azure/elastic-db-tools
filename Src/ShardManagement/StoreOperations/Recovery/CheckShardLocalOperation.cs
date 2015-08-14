// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Obtains all the shard maps and shards from an LSM.
    /// </summary>
    internal class CheckShardLocalOperation : StoreOperationLocal
    {
        /// <summary>
        /// Constructs request for obtaining all the shard maps and shards from an LSM.
        /// </summary>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        internal CheckShardLocalOperation(
            string operationName,
            ShardMapManager shardMapManager,
            ShardLocation location) :
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

            Debug.Assert(result.Result == StoreResult.Success);

            return result;
        }

        /// <summary>
        /// Handles errors from the LSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoLocalExecuteError(IStoreResults result)
        {
            Debug.Fail("Not expecting call because failure handled in the DoLocalExecute method.");
        }
    }
}
