// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Linq;
using System.Data.SqlClient;
using System.Diagnostics;
using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Upgrade store hosting GSM.
    /// </summary>
    internal class UpgradeStoreGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Target version of GSM to deploy, this will be used mainly for upgrade testing purpose.
        /// </summary>
        private Version _targetVersion;

        /// <summary>
        /// Constructs request to upgrade store hosting GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="targetVersion">Target version to upgrade.</param>
        internal UpgradeStoreGlobalOperation(
            ShardMapManager shardMapManager,
            string operationName,
            Version targetVersion) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
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
                "Started upgrading Global Shard Map structures.");

            IStoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.CheckIfExistsGlobalScript.Single());

            Debug.Assert(checkResult.StoreVersion != null, "GSM store structures not found.");

            if (checkResult.StoreVersion.Version < _targetVersion)
            {
                Stopwatch stopwatch = Stopwatch.StartNew();

                ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.UpgradeGlobalScript, _targetVersion, checkResult.StoreVersion.Version));

                // read GSM version after upgrade.
                checkResult = ts.ExecuteCommandSingle(SqlUtils.CheckIfExistsGlobalScript.Single());

                // DEVNOTE(apurvs): verify (checkResult.StoreVersion == GlobalConstants.GsmVersionClient) and throw on failure.

                stopwatch.Stop();

                TraceHelper.Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    this.OperationName,
                    "Finished upgrading Global Shard Map. Duration: {0}",
                    stopwatch.Elapsed);
            }
            else
            {
                TraceHelper.Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    this.OperationName,
                    "Global Shard Map is at a version {0} equal to or higher than Client library version {1}, skipping upgrade.",
                    (checkResult.StoreVersion == null) ? "" : checkResult.StoreVersion.Version.ToString(),
                    GlobalConstants.GsmVersionClient);
            }

            return checkResult;
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
                return ShardManagementErrorCategory.ShardMapManager;
            }
        }
    }
}
