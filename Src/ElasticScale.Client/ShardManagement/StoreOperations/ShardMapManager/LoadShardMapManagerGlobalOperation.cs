// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Gets all shard maps from GSM.
    /// </summary>
    internal class LoadShardMapManagerGlobalOperation : StoreOperationGlobal
    {
        /// <summary>
        /// Shard map manager object.
        /// </summary>
        private ShardMapManager _shardMapManager;

        private List<LoadResult> _loadResults;

        private IStoreShardMap _ssmCurrent;

        /// <summary>
        /// Constructs request to get all shard maps from GSM.
        /// </summary>
        /// <param name="shardMapManager">Shard map manager object.</param>
        /// <param name="operationName">Operation name, useful for diagnostics.</param>
        internal LoadShardMapManagerGlobalOperation(ShardMapManager shardMapManager, string operationName) :
            base(shardMapManager.Credentials, shardMapManager.RetryPolicy, operationName)
        {
            _shardMapManager = shardMapManager;
            _loadResults = new List<LoadResult>();
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
            _loadResults.Clear();

            IStoreResults result = ts.ExecuteOperation(
                StoreOperationRequestBuilder.SpGetAllShardMapsGlobal,
                StoreOperationRequestBuilder.GetAllShardMapsGlobal());

            if (result.Result == StoreResult.Success)
            {
                foreach (IStoreShardMap ssm in result.StoreShardMaps)
                {
                    _ssmCurrent = ssm;

                    result = ts.ExecuteOperation(
                        StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal,
                        StoreOperationRequestBuilder.GetAllShardMappingsGlobal(ssm, null, null));

                    if (result.Result == StoreResult.Success)
                    {
                        _loadResults.Add(
                            new LoadResult
                            {
                                ShardMap = ssm,
                                Mappings = result.StoreMappings
                            });
                    }
                    else
                    if (result.Result != StoreResult.ShardMapDoesNotExist)
                    {
                        // Ignore some possible failures for Load operation and skip failed
                        // shard map caching operations.
                        break;
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            if (_ssmCurrent == null)
            {
                // Possible errors are:
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnShardMapManagerErrorGlobal(
                    result,
                    null,
                    this.OperationName,
                    StoreOperationRequestBuilder.SpGetAllShardMapsGlobal);
            }
            else
            {
                if (result.Result != StoreResult.ShardMapDoesNotExist)
                {
                    // Possible errors are:
                    // StoreResult.StoreVersionMismatch
                    // StoreResult.MissingParametersForStoredProcedure
                    throw StoreOperationErrorHandler.OnShardMapperErrorGlobal(
                        result,
                        _ssmCurrent,
                        null, // shard
                        ShardManagementErrorCategory.ShardMapManager,
                        this.OperationName,
                        StoreOperationRequestBuilder.SpGetAllShardMappingsGlobal);
                }
            }
        }

        /// <summary>
        /// Refreshes the cache on successful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        public override void DoGlobalUpdateCachePost(IStoreResults result)
        {
            Debug.Assert(result.Result == StoreResult.Success ||
                         result.Result == StoreResult.ShardMapDoesNotExist);

            // Add shard maps and mappings to cache.
            foreach (LoadResult loadResult in _loadResults)
            {
                _shardMapManager.Cache.AddOrUpdateShardMap(loadResult.ShardMap);

                foreach (IStoreMapping sm in loadResult.Mappings)
                {
                    _shardMapManager.Cache.AddOrUpdateMapping(sm, CacheStoreMappingUpdatePolicy.OverwriteExisting);
                }
            }
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

        /// <summary>
        /// Result of load operation.
        /// </summary>
        private class LoadResult
        {
            /// <summary>
            /// Shard map from the store.
            /// </summary>
            internal IStoreShardMap ShardMap
            {
                get;
                set;
            }

            /// <summary>
            /// Mappings corresponding to the shard map.
            /// </summary>
            internal IEnumerable<IStoreMapping> Mappings
            {
                get;
                set;
            }
        }
    }
}
