// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Linq;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Deploys the SMM storage objects to the target GSM database.
    /// </summary>
    internal class CreateShardMapManagerGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Creation mode.
        /// </summary>
        private ShardMapManagerCreateMode _createMode;

        /// <summary>
        /// Target version of GSM to deploy, this will be used mainly for upgrade testing purpose.
        /// </summary>
        private Version _targetVersion;

        /// <summary>
        /// Constructs request for deploying SMM storage objects to target GSM database.
        /// </summary>
        /// <param name="credentials">Credentials for connection.</param>
        /// <param name="retryPolicy">Retry policy.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="createMode">Creation mode.</param>
        /// <param name="targetVersion">target version of store to deploy</param>
        internal CreateShardMapManagerGlobalOperation(
            SqlShardMapManagerCredentials credentials,
            TransientFaultHandling.RetryPolicy retryPolicy,
            string operationName,
            ShardMapManagerCreateMode createMode,
            Version targetVersion) :
            base(credentials, retryPolicy, operationName)
        {
            _createMode = createMode;
            _targetVersion = targetVersion;
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
            TraceHelper.Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                this.OperationName,
                "Started creating Global Shard Map structures.");

            Stopwatch stopwatch = Stopwatch.StartNew();

            IStoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.CheckIfExistsGlobalScript.Single());

            // If we did find some store deployed.
            if (checkResult.StoreVersion != null)
            {
                // DEVNOTE(wbasheer): We need to have a way of erroring out if versions do not match.
                if (_createMode == ShardMapManagerCreateMode.KeepExisting)
                {
                    throw new ShardManagementException(
                        ShardManagementErrorCategory.ShardMapManagerFactory,
                        ShardManagementErrorCode.ShardMapManagerStoreAlreadyExists,
                        Errors._Store_ShardMapManager_AlreadyExistsGlobal);
                }

                TraceHelper.Tracer.TraceVerbose(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    this.OperationName,
                    "Dropping existing Global Shard Map structures.");

                ts.ExecuteCommandBatch(SqlUtils.DropGlobalScript);
            }

            // Deploy initial version and run upgrade script to bring it to the specified version.
            ts.ExecuteCommandBatch(SqlUtils.CreateGlobalScript);

            ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.UpgradeGlobalScript, _targetVersion));

            stopwatch.Stop();

            TraceHelper.Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                this.OperationName,
                "Finished creating Global Shard Map structures. Duration: {0}",
                stopwatch.Elapsed);

            return new SqlResults();
        }


        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            Debug.Fail("Always expect Success or Exception from DoGlobalExecute.");
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
