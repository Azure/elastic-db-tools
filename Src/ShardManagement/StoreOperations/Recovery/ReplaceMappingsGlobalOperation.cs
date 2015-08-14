// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Replaces the GSM mappings for given shard map with the input mappings.
    /// </summary>
    internal class ReplaceMappingsGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Global shard map.
        /// </summary>
        private IStoreShardMap _shardMap;

        /// <summary>
        /// Global shard.
        /// </summary>
        private IStoreShard _shard;

        /// <summary>
        /// List of mappings to remove.
        /// </summary>
        private IEnumerable<IStoreMapping> _mappingsToRemove;

        /// <summary>
        /// List of mappings to add.
        /// </summary>
        private IEnumerable<IStoreMapping> _mappingsToAdd;

        /// <summary>
        /// Constructs request for replacing the GSM mappings for given shard map with the input mappings.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager.</param>
        /// <param name="operationName">Operation name.</param>
        /// <param name="shardMap">GSM Shard map.</param>
        /// <param name="shard">GSM Shard.</param>
        /// <param name="mappingsToRemove">Optional list of mappings to remove.</param>
        /// <param name="mappingsToAdd">List of mappings to add.</param>
        internal ReplaceMappingsGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, IStoreShard shard, IEnumerable<IStoreMapping> mappingsToRemove, IEnumerable<IStoreMapping> mappingsToAdd) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _shardMap = shardMap;
            _shard = shard;
            _mappingsToRemove = mappingsToRemove;
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
        /// Execute the operation against GSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        public override IStoreResults DoGlobalExecute(IStoreTransactionScope ts)
        {
            IEnumerable<IStoreMapping> mappingsToReplace = this.GetMappingsToPurge(ts);

            return ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpReplaceShardMappingsGlobal,
                StoreOperationRequestBuilder.ReplaceShardMappingsGlobalWithoutLogging(
                    _shardMap,
                    mappingsToReplace.ToArray(),
                    _mappingsToAdd.ToArray()));
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            // Possible errors are:
            // StoreResult.ShardMapDoesNotExist
            // StoreResult.StoreVersionMismatch
            // StoreResult.MissingParametersForStoredProcedure
            throw StoreOperationErrorHandler.OnRecoveryErrorGlobal(
                result,
                _shardMap,
                _shard,
                ShardManagementErrorCategory.Recovery,
                this.OperationName,
                StoreOperationRequestBuilder.SpReplaceShardMappingsGlobal);
        }

        /// <summary>
        /// Error category for store exception.
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                return ShardManagementErrorCategory.Recovery;
            }
        }

        /// <summary>
        /// Finds all mappings to be purged based on the given input ranges.
        /// </summary>
        /// <param name="ts">GSM transaction scope.</param>
        /// <returns>Mappings which are to be removed.</returns>
        private IEnumerable<IStoreMapping> GetMappingsToPurge(IStoreTransactionScope ts)
        {
            // Find all the mappings in GSM belonging to the shard
            IStoreResults gsmMappingsByShard = ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal,
                StoreOperationRequestBuilder.GetAllShardMappingsGlobal(_shardMap, _shard, null));

            if (gsmMappingsByShard.Result != StoreResult.Success)
            {
                // Possible errors are:
                // StoreResult.ShardMapDoesNotExist
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnRecoveryErrorGlobal(
                    gsmMappingsByShard,
                    _shardMap,
                    _shard,
                    ShardManagementErrorCategory.Recovery,
                    this.OperationName,
                    StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal);
            }

            IDictionary<ShardRange, IStoreMapping> intersectingMappings = new Dictionary<ShardRange, IStoreMapping>();

            foreach (IStoreMapping gsmMappingByShard in gsmMappingsByShard.StoreMappings)
            {
                ShardKey min = ShardKey.FromRawValue(
                    _shardMap.KeyType,
                    gsmMappingByShard.MinValue);

                ShardKey max = null;

                switch (_shardMap.MapType)
                {
                    case ShardMapType.Range:
                        max = ShardKey.FromRawValue(
                            _shardMap.KeyType,
                            gsmMappingByShard.MaxValue);
                        break;
                    default:
                        Debug.Assert(_shardMap.MapType == ShardMapType.List);
                        max = ShardKey.FromRawValue(
                            _shardMap.KeyType,
                            gsmMappingByShard.MinValue).GetNextKey();
                        break;
                }

                intersectingMappings.Add(new ShardRange(min, max), gsmMappingByShard);
            }

            // We need to discover, also, the range of intersecting mappings, so we can transitively detect
            // inconsistencies with other shards.
            foreach (IStoreMapping lsmMapping in _mappingsToRemove)
            {
                ShardKey min = ShardKey.FromRawValue(_shardMap.KeyType, lsmMapping.MinValue);

                IStoreResults gsmMappingsByRange;

                switch (_shardMap.MapType)
                {
                    case ShardMapType.Range:
                        gsmMappingsByRange = ts.ExecuteOperation(
                            StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal,
                            StoreOperationRequestBuilder.GetAllShardMappingsGlobal(
                                _shardMap,
                                null,
                                new ShardRange(
                                min,
                                ShardKey.FromRawValue(_shardMap.KeyType, lsmMapping.MaxValue))));
                        break;

                    default:
                        Debug.Assert(_shardMap.MapType == ShardMapType.List);
                        gsmMappingsByRange = ts.ExecuteOperation(
                            StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal,
                            StoreOperationRequestBuilder.FindShardMappingByKeyGlobal(
                                _shardMap,
                                min));
                        break;
                }

                if (gsmMappingsByRange.Result != StoreResult.Success)
                {
                    if (gsmMappingsByRange.Result != StoreResult.MappingNotFoundForKey)
                    {
                        // Possible errors are:
                        // StoreResult.ShardMapDoesNotExist
                        // StoreResult.StoreVersionMismatch
                        // StoreResult.MissingParametersForStoredProcedure
                        throw StoreOperationErrorHandler.OnRecoveryErrorGlobal(
                            gsmMappingsByRange,
                            _shardMap,
                            _shard,
                            ShardManagementErrorCategory.Recovery,
                            this.OperationName,
                            _shardMap.MapType == ShardMapType.Range ?
                            StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal :
                            StoreOperationRequestBuilder.SpFindShardMappingByKeyGlobal);
                    }
                    else
                    {
                        // No intersections being found is fine. Skip to the next mapping.
                        Debug.Assert(_shardMap.MapType == ShardMapType.List);
                    }
                }
                else
                {
                    foreach (IStoreMapping gsmMappingByRange in gsmMappingsByRange.StoreMappings)
                    {
                        ShardKey minGlobal = ShardKey.FromRawValue(_shardMap.KeyType, gsmMappingByRange.MinValue);
                        ShardKey maxGlobal = null;

                        switch (_shardMap.MapType)
                        {
                            case ShardMapType.Range:
                                maxGlobal = ShardKey.FromRawValue(_shardMap.KeyType, gsmMappingByRange.MaxValue);
                                break;
                            default:
                                Debug.Assert(_shardMap.MapType == ShardMapType.List);
                                maxGlobal = ShardKey.FromRawValue(_shardMap.KeyType, gsmMappingByRange.MinValue).GetNextKey();
                                break;
                        }

                        intersectingMappings[new ShardRange(minGlobal, maxGlobal)] = gsmMappingByRange;
                    }
                }
            }

            return intersectingMappings.Values;
        }
    }
}
