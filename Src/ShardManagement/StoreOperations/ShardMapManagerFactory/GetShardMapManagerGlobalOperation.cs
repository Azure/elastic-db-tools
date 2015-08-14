// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Obtains the shard map manager object if the GSM has the SMM objects in it.
    /// </summary>
    internal class GetShardMapManagerGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Whether to throw exception on failure.
        /// </summary>
        private bool _throwOnFailure;

        /// <summary>
        /// Constructs request for obtaining shard map manager object if the GSM has the SMM objects in it.
        /// </summary>
        /// <param name="credentials">Credentials for connection.</param>
        /// <param name="retryPolicy">Retry policy.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="throwOnFailure">Whether to throw exception on failure or return error code.</param>
        internal GetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials credentials, TransientFaultHandling.RetryPolicy retryPolicy, string operationName, bool throwOnFailure) :
            base(credentials, retryPolicy, operationName)
        {
            _throwOnFailure = throwOnFailure;
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
            IStoreResults result = ts.ExecuteCommandSingle(SqlUtils.CheckIfExistsGlobalScript.Single());

            SqlResults returnedResult = new SqlResults();

            // If we did not find some store deployed.
            if (result.StoreVersion == null)
            {
                returnedResult.Result = StoreResult.Failure;
            }
            else
            {
                // DEVNOTE(wbasheer): We need to have a way of erroring out if versions do not match.
                // we can potentially call upgrade here to get to latest version. Should this be exposed as a new parameter ?
                returnedResult.Result = StoreResult.Success;
            }

            return returnedResult;
        }


        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            if (_throwOnFailure)
            {
                throw new ShardManagementException(
                    ShardManagementErrorCategory.ShardMapManagerFactory,
                    ShardManagementErrorCode.ShardMapManagerStoreDoesNotExist,
                    Errors._Store_ShardMapManager_DoesNotExistGlobal);
            }
        }

        /// <summary>
        /// Error category for store exception.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return ShardManagementErrorCategory.ShardMapManagerFactory;
            }
        }
    }
}
