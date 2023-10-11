// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.StoreOperations.SchemaInformation;

/// <summary>
/// Finds schema info for given name in GSM.
/// </summary>
internal class FindShardingSchemaInfoGlobalOperation : StoreOperationGlobal
{
    /// <summary>
    /// Shard map name for given schema info.
    /// </summary>
    private readonly string _schemaInfoName;

    /// <summary>
    /// Constructs a request to find schema info in GSM.
    /// </summary>
    /// <param name="shardMapManager">Shard map manager object.</param>
    /// <param name="operationName">Operation name, useful for diagnostics.</param>
    /// <param name="schemaInfoName">Name of schema info to search.</param>
    internal FindShardingSchemaInfoGlobalOperation(ShardMapManager shardMapManager, string operationName, string schemaInfoName) :
        base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName) => _schemaInfoName = schemaInfoName;

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
            StoreOperationRequestBuilder.SpFindShardingSchemaInfoByNameGlobal,
            StoreOperationRequestBuilder.FindShardingSchemaInfoGlobal(_schemaInfoName));

    /// <summary>
    /// Handles errors from the GSM operation after the LSM operations.
    /// </summary>
    /// <param name="result">Operation result.</param>
    public override void HandleDoGlobalExecuteError(IStoreResults result)
    {
        // SchemaInfoNameDoesNotExist is handled by the callers i.e. Get vs TryGet.
        if (result.Result != StoreResult.SchemaInfoNameDoesNotExist)
            // Expected errors are:
            // StoreResult.MissingParametersForStoredProcedure:
            // StoreResult.StoreVersionMismatch:
            throw StoreOperationErrorHandler.OnShardSchemaInfoErrorGlobal(
                result,
                _schemaInfoName,
                OperationName,
                StoreOperationRequestBuilder.SpFindShardingSchemaInfoByNameGlobal);
    }

    public override void HandleDoGlobalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public override void HandleDoGlobalExecuteError(IStoreResults result) => throw new System.NotImplementedException();

    /// <summary>
    /// Error category for store exception.
    /// </summary>
    protected override ShardManagementErrorCategory ErrorCategory => ShardManagementErrorCategory.SchemaInfoCollection;
}