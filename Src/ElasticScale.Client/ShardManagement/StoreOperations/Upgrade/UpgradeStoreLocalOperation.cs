// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Upgrade store structures at specified location.
    /// </summary>
    internal class UpgradeStoreLocalOperation : StoreOperationLocal
    {
        /// <summary>
        /// Target version of LSM to deploy, this will be used mainly for upgrade testing purpose.
        /// </summary>
        private Version _targetVersion;

        /// <summary>
        /// Constructs request to upgrade store hosting LSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="location">Store location to upgrade.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        /// <param name="targetVersion">Target version to upgrade.</param>
        internal UpgradeStoreLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            Version targetVersion) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, location, operationName)
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
        /// Execute the operation against LSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        public override IStoreResults DoLocalExecute(IStoreTransactionScope ts)
        {
            TraceHelper.Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                this.OperationName,
                "Started upgrading Local Shard Map structures at location {0}", base.Location);

            Stopwatch stopwatch = Stopwatch.StartNew();

            IStoreResults checkResult = ts.ExecuteCommandSingle(SqlUtils.CheckIfExistsLocalScript.Single());
            if (checkResult.StoreVersion == null)
            {
                // DEVNOTE(apurvs): do we want to throw here if LSM is not already deployed?
                // deploy initial version of LSM, if not found.
                ts.ExecuteCommandBatch(SqlUtils.CreateLocalScript);
            }

            if (checkResult.StoreVersion == null || checkResult.StoreVersion.Version < _targetVersion)
            {
                if (checkResult.StoreVersion == null)
                    ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.UpgradeLocalScript, _targetVersion));
                else
                    ts.ExecuteCommandBatch(SqlUtils.FilterUpgradeCommands(SqlUtils.UpgradeLocalScript, _targetVersion, checkResult.StoreVersion.Version));

                // Read LSM version again after upgrade.
                checkResult = ts.ExecuteCommandSingle(SqlUtils.CheckIfExistsLocalScript.Single());

                stopwatch.Stop();

                TraceHelper.Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    this.OperationName,
                    "Finished upgrading store at location {0}. Duration: {1}",
                    base.Location,
                    stopwatch.Elapsed);
            }
            else
            {
                TraceHelper.Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    this.OperationName,
                    "Local Shard Map at location {0} has version {1} equal to or higher than Client library version {2}, skipping upgrade.",
                    base.Location,
                    checkResult.StoreVersion,
                    GlobalConstants.GsmVersionClient);
            }

            return checkResult;
        }

        public override void HandleDoLocalExecuteError(IStoreResults result)
        {
            throw new ShardManagementException(
                ShardManagementErrorCategory.ShardMapManager,
                ShardManagementErrorCode.StorageOperationFailure,
                Errors._Store_SqlExceptionLocal,
                OperationName
                );
        }
    }
}
