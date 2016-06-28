// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    internal static class ValidationUtils
    {
        /// <summary>
        /// The Tracer
        /// </summary>
        private static ILogger Tracer
        {
            get
            {
                return TraceHelper.Tracer;
            }
        }

        /// <summary>
        /// Performs validation that the local representation is as up-to-date 
        /// as the representation on the backing data store.
        /// </summary>
        /// <param name="conn">Connection used for validation.</param>
        /// <param name="manager">ShardMapManager reference.</param>
        /// <param name="shardMap">Shard map for the mapping.</param>
        /// <param name="storeMapping">Mapping to validate.</param>
        internal static void ValidateMapping(
            SqlConnection conn,
            ShardMapManager manager,
            IStoreShardMap shardMap,
            IStoreMapping storeMapping)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            SqlResults lsmResult = new SqlResults();

            XElement xeValidate = StoreOperationRequestBuilder.ValidateShardMappingLocal(shardMap.Id, storeMapping.Id);

            using (SqlCommand cmd = conn.CreateCommand())
            using (XmlReader input = xeValidate.CreateReader())
            {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardMappingLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(
                    cmd,
                    "@input",
                    SqlDbType.Xml,
                    ParameterDirection.Input,
                    0,
                    new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(
                    cmd,
                    "@result",
                    SqlDbType.Int,
                    ParameterDirection.Output,
                    0,
                    0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    lsmResult.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.Result = (StoreResult)resultParam.Value;
            }

            stopwatch.Stop();

            Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.Shard,
                "ValidateMapping",
                "Complete; Shard: {0}; Connection: {1}; Result: {2}; Duration: {3}",
                storeMapping.StoreShard.Location,
                conn.ConnectionString,
                lsmResult.Result,
                stopwatch.Elapsed);

            if (lsmResult.Result != StoreResult.Success)
            {
                if (lsmResult.Result == StoreResult.ShardMapDoesNotExist)
                {
                    manager.Cache.DeleteShardMap(shardMap);
                }
                else
                if (lsmResult.Result == StoreResult.MappingDoesNotExist)
                {
                    // Only evict from cache is mapping is no longer present,
                    // for Offline mappings, we don't even retry, so same request
                    // will continue to go to the LSM.
                    manager.Cache.DeleteMapping(storeMapping);
                }

                // Possible errors are:
                // StoreResult.ShardMapDoesNotExist
                // StoreResult.MappingDoesNotExist
                // StoreResult.MappingIsOffline
                // StoreResult.ShardVersionMismatch
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnValidationErrorLocal(
                    lsmResult,
                    shardMap,
                    storeMapping.StoreShard.Location,
                    "ValidateMapping",
                    StoreOperationRequestBuilder.SpValidateShardLocal);
            }

            Debug.Assert(lsmResult.Result == StoreResult.Success);
        }

        /// <summary>
        /// Asynchronously performs validation that the local representation is as up-to-date 
        /// as the representation on the backing data store.
        /// </summary>
        /// <param name="conn">Connection used for validation.</param>
        /// <param name="manager">ShardMapManager reference.</param>
        /// <param name="shardMap">Shard map for the mapping.</param>
        /// <param name="storeMapping">Mapping to validate.</param>
        /// <returns>A task to await validation completion</returns>
        internal static async Task ValidateMappingAsync(
            SqlConnection conn,
            ShardMapManager manager,
            IStoreShardMap shardMap,
            IStoreMapping storeMapping)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            SqlResults lsmResult = new SqlResults();

            XElement xeValidate = StoreOperationRequestBuilder.ValidateShardMappingLocal(shardMap.Id, storeMapping.Id);

            using (SqlCommand cmd = conn.CreateCommand())
            using (XmlReader input = xeValidate.CreateReader())
            {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardMappingLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(
                    cmd,
                    "@input",
                    SqlDbType.Xml,
                    ParameterDirection.Input,
                    0,
                    new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(
                    cmd,
                    "@result",
                    SqlDbType.Int,
                    ParameterDirection.Output,
                    0,
                    0);

                using (SqlDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    await lsmResult.FetchAsync(reader).ConfigureAwait(false);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.Result = (StoreResult)resultParam.Value;
            }

            stopwatch.Stop();

            Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.Shard,
                "ValidateMappingAsync",
                "Complete; Shard: {0}; Connection: {1}; Result: {2}; Duration: {3}",
                storeMapping.StoreShard.Location,
                conn.ConnectionString,
                lsmResult.Result,
                stopwatch.Elapsed);

            if (lsmResult.Result != StoreResult.Success)
            {
                if (lsmResult.Result == StoreResult.ShardMapDoesNotExist)
                {
                    manager.Cache.DeleteShardMap(shardMap);
                }
                else if (lsmResult.Result == StoreResult.MappingDoesNotExist ||
                        lsmResult.Result == StoreResult.MappingIsOffline)
                {
                    manager.Cache.DeleteMapping(storeMapping);
                }

                // Possible errors are:
                // StoreResult.ShardMapDoesNotExist
                // StoreResult.MappingDoesNotExist
                // StoreResult.MappingIsOffline
                // StoreResult.ShardVersionMismatch
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnValidationErrorLocal(
                    lsmResult,
                    shardMap,
                    storeMapping.StoreShard.Location,
                    "ValidateMappingAsync",
                    StoreOperationRequestBuilder.SpValidateShardLocal);
            }

            Debug.Assert(lsmResult.Result == StoreResult.Success);
        }

        /// <summary>
        /// Performs validation that the local representation is as 
        /// up-to-date as the representation on the backing data store.
        /// </summary>
        /// <param name="conn">Connection used for validation.</param>
        /// <param name="manager">ShardMapManager reference.</param>
        /// <param name="shardMap">Shard map for the shard.</param>
        /// <param name="shard">Shard to validate.</param>
        internal static void ValidateShard(
            SqlConnection conn,
            ShardMapManager manager,
            IStoreShardMap shardMap,
            IStoreShard shard
            )
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            SqlResults lsmResult = new SqlResults();

            XElement xeValidate = StoreOperationRequestBuilder.ValidateShardLocal(
                shardMap.Id,
                shard.Id,
                shard.Version);

            using (SqlCommand cmd = conn.CreateCommand())
            using (XmlReader input = xeValidate.CreateReader())
            {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(
                    cmd,
                    "@input",
                    SqlDbType.Xml,
                    ParameterDirection.Input,
                    0,
                    new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(
                    cmd,
                    "@result",
                    SqlDbType.Int,
                    ParameterDirection.Output,
                    0,
                    0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    lsmResult.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.Result = (StoreResult)resultParam.Value;
            }

            stopwatch.Stop();

            Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.Shard,
                "ValidateShard",
                "Complete; Shard: {0}; Connection: {1}; Result: {2}; Duration: {3}",
                shard.Location,
                conn.ConnectionString,
                lsmResult.Result,
                stopwatch.Elapsed);

            if (lsmResult.Result != StoreResult.Success)
            {
                if (lsmResult.Result == StoreResult.ShardMapDoesNotExist)
                {
                    manager.Cache.DeleteShardMap(shardMap);
                }

                // Possible errors are:
                // StoreResult.ShardMapDoesNotExist
                // StoreResult.ShardDoesNotExist
                // StoreResult.ShardVersionMismatch
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnValidationErrorLocal(
                    lsmResult,
                    shardMap,
                    shard.Location,
                    "ValidateShard",
                    StoreOperationRequestBuilder.SpValidateShardLocal);
            }
        }

        /// <summary>
        /// Asynchronously performs validation that the local representation is as 
        /// up-to-date as the representation on the backing data store.
        /// </summary>
        /// <param name="conn">Connection used for validation.</param>
        /// <param name="manager">ShardMapManager reference.</param>
        /// <param name="shardMap">Shard map for the shard.</param>
        /// <param name="shard">Shard to validate.</param>
        /// <returns>A task to await validation completion</returns>
        internal static async Task ValidateShardAsync(
            SqlConnection conn,
            ShardMapManager manager,
            IStoreShardMap shardMap,
            IStoreShard shard
            )
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            SqlResults lsmResult = new SqlResults();

            XElement xeValidate = StoreOperationRequestBuilder.ValidateShardLocal(shardMap.Id, shard.Id, shard.Version);

            using (SqlCommand cmd = conn.CreateCommand())
            using (XmlReader input = xeValidate.CreateReader())
            {
                cmd.CommandText = StoreOperationRequestBuilder.SpValidateShardLocal;
                cmd.CommandType = CommandType.StoredProcedure;

                SqlUtils.AddCommandParameter(
                    cmd,
                    "@input",
                    SqlDbType.Xml,
                    ParameterDirection.Input,
                    0,
                    new SqlXml(input));

                SqlParameter resultParam = SqlUtils.AddCommandParameter(
                    cmd,
                    "@result",
                    SqlDbType.Int,
                    ParameterDirection.Output,
                    0,
                    0);

                using (SqlDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    await lsmResult.FetchAsync(reader).ConfigureAwait(false);
                }

                // Output parameter will be used to specify the outcome.
                lsmResult.Result = (StoreResult)resultParam.Value;
            }

            stopwatch.Stop();

            Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.Shard,
                "ValidateShardAsync",
                "Complete; Shard: {0}; Connection: {1}; Result: {2}; Duration: {3}",
                shard.Location,
                conn.ConnectionString,
                lsmResult.Result,
                stopwatch.Elapsed);

            if (lsmResult.Result != StoreResult.Success)
            {
                if (lsmResult.Result == StoreResult.ShardMapDoesNotExist)
                {
                    manager.Cache.DeleteShardMap(shardMap);
                }

                // Possible errors are:
                // StoreResult.ShardMapDoesNotExist
                // StoreResult.ShardDoesNotExist
                // StoreResult.ShardVersionMismatch
                // StoreResult.StoreVersionMismatch
                // StoreResult.MissingParametersForStoredProcedure
                throw StoreOperationErrorHandler.OnValidationErrorLocal(
                    lsmResult,
                    shardMap,
                    shard.Location,
                    "ValidateShardAsync",
                    StoreOperationRequestBuilder.SpValidateShardLocal);
            }
        }
    }
}
