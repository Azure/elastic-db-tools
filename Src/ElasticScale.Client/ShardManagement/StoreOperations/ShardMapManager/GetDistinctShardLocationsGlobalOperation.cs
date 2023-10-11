// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.StoreOperations.ShardMapManager;

/// <summary>
/// Gets all distinct shard locations from GSM.
/// </summary>
internal class GetDistinctShardLocationsGlobalOperation : StoreOperationGlobal
{
    /// <summary>
    /// Constructs request to get distinct shard locations from GSM.
    /// </summary>
    /// <param name="shardMapManager">Shard map manager object.</param>
    /// <param name="operationName">Operation name, useful for diagnostics.</param>
    internal GetDistinctShardLocationsGlobalOperation(ShardMapManager shardMapManager, string operationName) :
        base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
    {
    }

    /// <summary>
    /// Whether this is a read-only operation.
    /// </summary>
    public override bool ReadOnly => true;

    /// <summary>
    /// Execute the operation against GSM in the current transaction scope.
    /// </summary>
    /// <param name="ts">Transaction scope.</param>
    /// <returns>
    /// Results of the operation.
    /// </returns>
    public override IStoreResults DoGlobalExecute(IStoreTransactionScope ts) => ts.ExecuteOperation(
            StoreOperationRequestBuilder.SpGetAllDistinctShardLocationsGlobal,
            StoreOperationRequestBuilder.GetAllDistinctShardLocationsGlobal());

    /// <summary>
    /// Handles errors from the GSM operation after the LSM operations.
    /// </summary>
    /// <param name="result">Operation result.</param>
    public override void HandleDoGlobalExecuteError(IStoreResults result) =>
        // Possible errors are:
        // StoreResult.StoreVersionMismatch
        // StoreResult.MissingParametersForStoredProcedure
        throw StoreOperationErrorHandler.OnShardMapManagerErrorGlobal(
            result,
            null,
            OperationName,
            StoreOperationRequestBuilder.SpGetAllDistinctShardLocationsGlobal);
    public override void HandleDoGlobalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public override void HandleDoGlobalExecuteError(IStoreResults result) => throw new System.NotImplementedException();

    /// <summary>
    /// Error category for store exception.
    /// </summary>
    protected override ShardManagementErrorCategory ErrorCategory => ShardManagementErrorCategory.ShardMapManager;
}
