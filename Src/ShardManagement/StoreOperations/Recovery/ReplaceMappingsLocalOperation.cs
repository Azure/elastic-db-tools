// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Replaces the LSM mappings for given shard map with the input mappings.
    /// </summary>
    internal class ReplaceMappingsLocalOperation : StoreOperationLocal
    {
        /// <summary>
        /// Local shard map.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Local shard.
        /// </summary>
        private IStoreShard _shard;

        /// <summary>
        /// List of ranges to be removed.
        /// </summary>
        private IEnumerable<ShardRange> _rangesToRemove;

        /// <summary>
        /// List of mappings to add.
        /// </summary>
        private IEnumerable<IStoreMapping> _mappingsToAdd;

        /// <summary>
        /// Constructs request for replacing the LSM mappings for given shard map with the input mappings.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="location">Location of the LSM.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMap">Local shard map.</param>
        /// <param name="shard">Local shard.</param>
        /// <param name="rangesToRemove">Optional list of ranges to minimize amount of deletions.</param>
        /// <param name="mappingsToAdd">List of mappings to add.</param>
        internal ReplaceMappingsLocalOperation(
            ShardMapManager shardMapManager,
            ShardLocation location,
            string operationName,
            IStoreShardMap shardMap,
            IStoreShard shard,
            IEnumerable<ShardRange> rangesToRemove,
            IEnumerable<IStoreMapping> mappingsToAdd) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, location, operationName)
        {
            _shardMap = shardMap;
            _shard = shard;
            _rangesToRemove = rangesToRemove;
            _mappingsToAdd = mappingsToAdd;
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
            IEnumerable<IStoreMapping> mappingsToRemove = this.GetMappingsToPurge(ts);

            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal,
                StoreOperationRequestBuilder.ReplaceShardMappingsLocal(
                    Guid.NewGuid(), // Create a new Guid so that this operation forces over-writes.
                    false,
                    _shardMap,
                    mappingsToRemove.ToArray(),
                    _mappingsToAdd.ToArray()));
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
                _shardMap,
                this.Location,
                ShardManagementErrorCategory.Recovery,
                this.OperationName,
                StoreOperationRequestBuilder.SpBulkOperationShardMappingsLocal);
        }

        /// <summary>
        /// Finds all mappings to be purged based on the given input ranges.
        /// </summary>
        /// <param name="ts">LSM transaction scope.</param>
        /// <returns>Mappings which are to be removed.</returns>
        private IEnumerable<IStoreMapping> GetMappingsToPurge(IStoreTransactionScope ts)
        {
            IEnumerable<IStoreMapping> lsmMappings = null;

            IStoreResults result;

            if (_rangesToRemove == null)
            {
                // If no ranges are specified, get all the mappings for the shard.
                result = ts.ExecuteOperation(
                    StoreOperationRequestBuilder.SpGetAllShardMappingsLocal,
                    StoreOperationRequestBuilder.GetAllShardMappingsLocal(_shardMap, _shard, null));

                if (result.Result != StoreResult.Success)
                {
                    // Possible errors are:
                    // StoreResult.ShardMapDoesNotExist
                    // StoreResult.StoreVersionMismatch
                    // StoreResult.MissingParametersForStoredProcedure
                    throw StoreOperationErrorHandler.OnRecoveryErrorLocal(
                        result,
                        _shardMap,
                        this.Location,
                        ShardManagementErrorCategory.Recovery,
                        this.OperationName,
                        StoreOperationRequestBuilder.SpGetAllShardMappingsLocal);
                }

                lsmMappings = result.StoreMappings;
            }
            else
            {
                // If any ranges are specified, only delete intersected ranges.
                IDictionary<ShardRange, IStoreMapping> mappingsToPurge = new Dictionary<ShardRange, IStoreMapping>();

                foreach (ShardRange range in _rangesToRemove)
                {
                    switch (_shardMap.MapType)
                    {
                        case ShardMapType.Range:
                            result = ts.ExecuteOperation(
                                StoreOperationRequestBuilder.SpGetAllShardMappingsLocal,
                                StoreOperationRequestBuilder.GetAllShardMappingsLocal(
                                    _shardMap,
                                    _shard,
                                    range));
                            break;

                        default:
                            Debug.Assert(_shardMap.MapType == ShardMapType.List);
                            result = ts.ExecuteOperation(
                                StoreOperationRequestBuilder.SpFindShardMappingByKeyLocal,
                                StoreOperationRequestBuilder.FindShardMappingByKeyLocal(
                                    _shardMap,
                                    ShardKey.FromRawValue(_shardMap.KeyType, range.Low.RawValue)));
                            break;
                    }

                    if (result.Result != StoreResult.Success)
                    {
                        if (result.Result != StoreResult.MappingNotFoundForKey)
                        {
                            // Possible errors are:
                            // StoreResult.ShardMapDoesNotExist
                            // StoreResult.StoreVersionMismatch
                            // StoreResult.MissingParametersForStoredProcedure
                            throw StoreOperationErrorHandler.OnRecoveryErrorLocal(
                                result,
                                _shardMap,
                                this.Location,
                                ShardManagementErrorCategory.Recovery,
                                this.OperationName,
                                _shardMap.MapType == ShardMapType.Range ?
                                StoreOperationRequestBuilder.SpGetAllShardMappingsLocal :
                                StoreOperationRequestBuilder.SpFindShardMappingByKeyLocal);
                        }
                        else
                        {
                            // No intersections being found is fine. Skip to the next mapping.
                            Debug.Assert(_shardMap.MapType == ShardMapType.List);
                        }
                    }
                    else
                    {
                        foreach (IStoreMapping mapping in result.StoreMappings)
                        {
                            ShardRange intersectedRange = new ShardRange(
                                ShardKey.FromRawValue(_shardMap.KeyType, mapping.MinValue),
                                ShardKey.FromRawValue(_shardMap.KeyType, mapping.MaxValue));

                            mappingsToPurge[intersectedRange] = mapping;
                        }
                    }
                }

                lsmMappings = mappingsToPurge.Values;
            }

            return lsmMappings;
        }
    }
}
