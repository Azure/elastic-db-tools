// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Linq;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Constructs requests for store operations.
    /// </summary>
    internal static class StoreOperationRequestBuilder
    {
        /// <summary>
        /// Step kind for for Bulk Operations.
        /// </summary>
        private enum StoreOperationStepKind
        {
            /// <summary>
            /// Remove operation.
            /// </summary>
            Remove = 1,

            /// <summary>
            /// Update operation.
            /// </summary>
            Update = 2,

            /// <summary>
            /// Add operation.
            /// </summary>
            Add = 3
        }


        /// <summary>
        /// Element representing GSM version.
        /// </summary>
        private static readonly XElement s_gsmVersion = new XElement("GsmVersion",
            new XElement("MajorVersion", GlobalConstants.GsmVersionClient.Major),
            new XElement("MinorVersion", GlobalConstants.GsmVersionClient.Minor));

        /// <summary>
        /// Element representing LSM version.
        /// </summary>
        private static readonly XElement s_lsmVersion = new XElement("LsmVersion",
            new XElement("MajorVersion", GlobalConstants.LsmVersionClient.Major),
            new XElement("MinorVersion", GlobalConstants.LsmVersionClient.Minor));

        #region GSM Stored Procedures

        /// <summary>
        /// FindAndUpdateOperationLogEntryByIdGlobal stored procedure.
        /// </summary>
        internal const string SpFindAndUpdateOperationLogEntryByIdGlobal = @"__ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal";

        /// <summary>
        /// GetAllShardMapsGlobal stored procedure.
        /// </summary>
        internal const string SpGetAllShardMapsGlobal = @"__ShardManagement.spGetAllShardMapsGlobal";

        /// <summary>
        /// FindShardMapByNameGlobal stored procedure.
        /// </summary>
        internal const string SpFindShardMapByNameGlobal = @"__ShardManagement.spFindShardMapByNameGlobal";

        /// <summary>
        /// GetAllDistinctShardLocationsGlobal stored procedure.
        /// </summary>
        internal const string SpGetAllDistinctShardLocationsGlobal = @"__ShardManagement.spGetAllDistinctShardLocationsGlobal";

        /// <summary>
        /// AddShardMapGlobal stored procedure.
        /// </summary>
        internal const string SpAddShardMapGlobal = @"__ShardManagement.spAddShardMapGlobal";

        /// <summary>
        /// RemoveShardMapGlobal stored procedure.
        /// </summary>
        internal const string SpRemoveShardMapGlobal = @"__ShardManagement.spRemoveShardMapGlobal";

        /// <summary>
        /// GetAllShardsGlobal stored procedure.
        /// </summary>
        internal const string SpGetAllShardsGlobal = @"__ShardManagement.spGetAllShardsGlobal";

        /// <summary>
        /// FindShardByLocationGlobal stored procedure.
        /// </summary>
        internal const string SpFindShardByLocationGlobal = @"__ShardManagement.spFindShardByLocationGlobal";

        /// <summary>
        /// BulkOperationShardsGlobalBegin stored procedure.
        /// </summary>
        internal const string SpBulkOperationShardsGlobalBegin = @"__ShardManagement.spBulkOperationShardsGlobalBegin";

        /// <summary>
        /// BulkOperationShardsGlobalEnd stored procedure.
        /// </summary>
        internal const string SpBulkOperationShardsGlobalEnd = @"__ShardManagement.spBulkOperationShardsGlobalEnd";

        /// <summary>
        /// GetAllShardMappingsGlobal stored procedure.
        /// </summary>
        internal const string SpGetAllShardMappingsGlobal = @"__ShardManagement.spGetAllShardMappingsGlobal";

        /// <summary>
        /// FindShardMappingByKeyGlobal stored procedure.
        /// </summary>
        internal const string SpFindShardMappingByKeyGlobal = @"__ShardManagement.spFindShardMappingByKeyGlobal";

        /// <summary>
        /// FindShardMappingByIdGlobal stored procedure.
        /// </summary>
        internal const string SpFindShardMappingByIdGlobal = @"__ShardManagement.spFindShardMappingByIdGlobal";

        /// <summary>
        /// BulkShardMappingOperationsGlobalBegin stored procedure.
        /// </summary>
        internal const string SpBulkOperationShardMappingsGlobalBegin = @"__ShardManagement.spBulkOperationShardMappingsGlobalBegin";

        /// <summary>
        /// BulkShardMappingOperationsGlobalEnd stored procedure.
        /// </summary>
        internal const string SpBulkOperationShardMappingsGlobalEnd = @"__ShardManagement.spBulkOperationShardMappingsGlobalEnd";

        /// <summary>
        /// LockOrUnLockShardMappingsGlobal stored procedure.
        /// </summary>
        internal const string SpLockOrUnLockShardMappingsGlobal = @"__ShardManagement.spLockOrUnlockShardMappingsGlobal";

        /// <summary>
        /// GetAllShardingSchemaInfosGlobal stored procedure.
        /// </summary>
        internal const string SpGetAllShardingSchemaInfosGlobal = @"__ShardManagement.spGetAllShardingSchemaInfosGlobal";

        /// <summary>
        /// FindShardingSchemaInfoByNameGlobal stored procedure.
        /// </summary>
        internal const string SpFindShardingSchemaInfoByNameGlobal = @"__ShardManagement.spFindShardingSchemaInfoByNameGlobal";

        /// <summary>
        /// AddShardingSchemaInfoGlobal stored procedure.
        /// </summary>
        internal const string SpAddShardingSchemaInfoGlobal = @"__ShardManagement.spAddShardingSchemaInfoGlobal";

        /// <summary>
        /// RemoveShardingSchemaInfoGlobal stored procedure.
        /// </summary>
        internal const string SpRemoveShardingSchemaInfoGlobal = @"__ShardManagement.spRemoveShardingSchemaInfoGlobal";

        /// <summary>
        /// UpdateShardingSchemaInfoGlobal stored procedure.
        /// </summary>
        internal const string SpUpdateShardingSchemaInfoGlobal = @"__ShardManagement.spUpdateShardingSchemaInfoGlobal";

        /// <summary>
        /// AttachShardGlobal stored procedure.
        /// </summary>
        internal const string SpAttachShardGlobal = @"__ShardManagement.spAttachShardGlobal";

        /// <summary>
        /// DetachShardGlobal stored procedure.
        /// </summary>
        internal const string SpDetachShardGlobal = @"__ShardManagement.spDetachShardGlobal";

        /// <summary>
        /// ReplaceShardMappingsGlobal stored procedure.
        /// </summary>
        internal const string SpReplaceShardMappingsGlobal = @"__ShardManagement.spReplaceShardMappingsGlobal";

        #endregion GSM Stored Procedures

        #region LSM Stored Procedures

        /// <summary>
        /// GetAllShardsLocal stored procedure.
        /// </summary>
        internal const string SpGetAllShardsLocal = @"__ShardManagement.spGetAllShardsLocal";

        /// <summary>
        /// ValidateShardLocal stored procedure.
        /// </summary>
        internal const string SpValidateShardLocal = @"__ShardManagement.spValidateShardLocal";

        /// <summary>
        /// AddShardLocal stored procedure.
        /// </summary>
        internal const string SpAddShardLocal = @"__ShardManagement.spAddShardLocal";

        /// <summary>
        /// RemoveShardLocal stored procedure.
        /// </summary>
        internal const string SpRemoveShardLocal = @"__ShardManagement.spRemoveShardLocal";

        /// <summary>
        /// UpdateShardLocal stored procedure.
        /// </summary>
        internal const string SpUpdateShardLocal = @"__ShardManagement.spUpdateShardLocal";

        /// <summary>
        /// GetAllShardMappingsLocal stored procedure.
        /// </summary>
        internal const string SpGetAllShardMappingsLocal = @"__ShardManagement.spGetAllShardMappingsLocal";

        /// <summary>
        /// FindShardMappingByKeyLocal stored procedure.
        /// </summary>
        internal const string SpFindShardMappingByKeyLocal = @"__ShardManagement.spFindShardMappingByKeyLocal";

        /// <summary>
        /// ValidateShardMappingLocal stored procedure.
        /// </summary>
        internal const string SpValidateShardMappingLocal = @"__ShardManagement.spValidateShardMappingLocal";

        /// <summary>
        /// BulkOperationShardMappingsLocal stored procedure.
        /// </summary>
        internal const string SpBulkOperationShardMappingsLocal = @"__ShardManagement.spBulkOperationShardMappingsLocal";

        /// <summary>
        /// KillSessionsForShardMappingLocal stored procedure.
        /// </summary>
        internal const string SpKillSessionsForShardMappingLocal = @"__ShardManagement.spKillSessionsForShardMappingLocal";

        #endregion LSM Stored Procedures

        /// <summary>
        /// Find operation log entry by Id from GSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="undoStartState">Minimum start from which to start undo operation.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement FindAndUpdateOperationLogEntryByIdGlobal(Guid operationId, StoreOperationState undoStartState)
        {
            return new XElement(
                @"FindAndUpdateOperationLogEntryByIdGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.UndoStartState(undoStartState));
        }

        /// <summary>
        /// Request to get all shard maps from GSM.
        /// </summary>
        /// <returns>Xml formatted request.</returns>
        internal static XElement GetAllShardMapsGlobal()
        {
            return new XElement(
                @"GetAllShardMapsGlobal",
                StoreOperationRequestBuilder.s_gsmVersion);
        }

        /// <summary>
        /// Request to get shard map with the given name from GSM.
        /// </summary>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement FindShardMapByNameGlobal(string shardMapName)
        {
            return new XElement(
                @"FindShardMapByNameGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                new XElement("ShardMap",
                    new XElement("Name", shardMapName)));
        }

        /// <summary>
        /// Request to get all distinct shard locations from GSM.
        /// </summary>
        /// <returns>Xml formatted request.</returns>
        internal static XElement GetAllDistinctShardLocationsGlobal()
        {
            return new XElement(
                @"GetAllDistinctShardLocationsGlobal",
                StoreOperationRequestBuilder.s_gsmVersion);
        }

        /// <summary>
        /// Request to add shard map to GSM.
        /// </summary>
        /// <param name="shardMap">Shard map to add.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement AddShardMapGlobal(IStoreShardMap shardMap)
        {
            return new XElement(
                @"AddShardMapGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap));
        }

        /// <summary>
        /// Request to remove shard map from GSM.
        /// </summary>
        /// <param name="shardMap">Shard map to remove.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement RemoveShardMapGlobal(IStoreShardMap shardMap)
        {
            return new XElement(
                @"RemoveShardMapGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap));
        }

        /// <summary>
        /// Request to get all shards for a shard map from GSM.
        /// </summary>
        /// <param name="shardMap">Shard map for which to get all shards.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement GetAllShardsGlobal(IStoreShardMap shardMap)
        {
            return new XElement(
                @"GetAllShardsGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap));
        }

        /// <summary>
        /// Request to get shard with specified location for a shard map from GSM.
        /// </summary>
        /// <param name="shardMap">Shard map for which to get shard.</param>
        /// <param name="location">Location for which to find shard.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement FindShardByLocationGlobal(
            IStoreShardMap shardMap,
            ShardLocation location)
        {
            return new XElement(
                @"FindShardByLocationGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteShardLocation("Location", location));
        }

        /// <summary>
        /// Request to add shard to given shard map in GSM.
        /// </summary>
        /// <param name="operationId">Operation Id</param>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="undo">Whether this is an undo request.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="shard">Shard to add.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement AddShardGlobal(
            Guid operationId,
            StoreOperationCode operationCode,
            bool undo,
            IStoreShardMap shardMap,
            IStoreShard shard)
        {
            return new XElement(
                "BulkOperationShardsGlobal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.OperationCode(operationCode),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(1),
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                new XElement("Steps",
                    new XElement("Step",
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add),
                        new XAttribute("Id", 1),
                        StoreObjectFormatterXml.WriteIStoreShard("Shard", shard))));
        }

        /// <summary>
        /// Request to remove shard from given shard map in GSM.
        /// </summary>
        /// <param name="operationId">Operation Id</param>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="undo">Whether this is an undo request.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="shard">Shard to remove.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement RemoveShardGlobal(
            Guid operationId,
            StoreOperationCode operationCode,
            bool undo,
            IStoreShardMap shardMap,
            IStoreShard shard)
        {
            return new XElement(
                "BulkOperationShardsGlobal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.OperationCode(operationCode),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(1),
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                new XElement("Steps",
                    new XElement("Step",
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove),
                        new XAttribute("Id", 1),
                        StoreObjectFormatterXml.WriteIStoreShard("Shard", shard))),
                new XElement("AddSteps"));
        }

        /// <summary>
        /// Request to update shard in given shard map in GSM.
        /// </summary>
        /// <param name="operationId">Operation Id</param>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="undo">Whether this is an undo request.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="shardOld">Shard to update.</param>
        /// <param name="shardNew">Updated shard.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement UpdateShardGlobal(
            Guid operationId,
            StoreOperationCode operationCode,
            bool undo,
            IStoreShardMap shardMap,
            IStoreShard shardOld,
            IStoreShard shardNew)
        {
            return new XElement(
                "BulkOperationShardsGlobal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.OperationCode(operationCode),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(1),
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                new XElement("Steps",
                    new XElement("Step",
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Update),
                        new XAttribute("Id", 1),
                        StoreObjectFormatterXml.WriteIStoreShard("Shard", shardOld),
                        new XElement("Update",
                            StoreObjectFormatterXml.WriteIStoreShard("Shard", shardNew)))));
        }

        /// <summary>
        /// Request to get all shard mappings from GSM for a particular shard map
        /// and optional shard and range.
        /// </summary>
        /// <param name="shardMap">Shard map whose mappings are being requested.</param>
        /// <param name="shard">Optional shard for which mappings are being requested.</param>
        /// <param name="range">Optional range for which mappings are being requested.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement GetAllShardMappingsGlobal(
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardRange range)
        {
            return new XElement(
                @"GetAllShardMappingsGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", shard),
                StoreObjectFormatterXml.WriteShardRange(range));
        }

        /// <summary>
        /// Request to get mapping from GSM for a particular key belonging to a shard map.
        /// </summary>
        /// <param name="shardMap">Shard map whose mappings are being requested.</param>
        /// <param name="key">Key being searched.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement FindShardMappingByKeyGlobal(
            IStoreShardMap shardMap,
            ShardKey key)
        {
            return new XElement(
                @"FindShardMappingByKeyGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteShardKey(key));
        }

        /// <summary>
        /// Request to get mapping from GSM for a particular mapping Id.
        /// </summary>
        /// <param name="shardMap">Shard map whose mappings are being requested.</param>
        /// <param name="mapping">Mapping to look up.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement FindShardMappingByIdGlobal(
            IStoreShardMap shardMap,
            IStoreMapping mapping)
        {
            return new XElement(
                @"FindShardMappingByIdGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping));
        }

        /// <summary>
        /// Request to add shard to given shard map in GSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="undo">Whether this is an undo request.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement AddShardMappingGlobal(
            Guid operationId,
            StoreOperationCode operationCode,
            bool undo,
            IStoreShardMap shardMap,
            IStoreMapping mapping)
        {
            return new XElement(
                "BulkOperationShardMappingsGlobal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.OperationCode(operationCode),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(1),
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                new XElement("Removes",
                    StoreObjectFormatterXml.WriteIStoreShard("Shard", mapping.StoreShard)),
                new XElement("Adds",
                    StoreObjectFormatterXml.WriteIStoreShard("Shard", mapping.StoreShard)),
                new XElement("Steps",
                    new XElement("Step",
                        new XAttribute("Id", 1),
                        StoreOperationRequestBuilder.Validate(true),
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add),
                        StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping))));
        }

        /// <summary>
        /// Request to remove shard from given shard map in GSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="undo">Whether this is an undo request.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mapping">Mapping to remove.</param>
        /// <param name="lockOwnerId">Lock owner.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement RemoveShardMappingGlobal(
            Guid operationId,
            StoreOperationCode operationCode,
            bool undo,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockOwnerId)
        {
            return new XElement(
                "BulkOperationShardMappingsGlobal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.OperationCode(operationCode),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(1),
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                new XElement("Removes",
                    StoreObjectFormatterXml.WriteIStoreShard("Shard", mapping.StoreShard)),
                new XElement("Adds",
                    StoreObjectFormatterXml.WriteIStoreShard("Shard", mapping.StoreShard)),
                new XElement("Steps",
                    new XElement("Step",
                        new XAttribute("Id", 1),
                        StoreOperationRequestBuilder.Validate(false),
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove),
                        StoreObjectFormatterXml.WriteLock(lockOwnerId),
                        StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping))));
        }

        /// <summary>
        /// Request to update mapping in given shard map in GSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="undo">Whether this is an undo request.</param>
        /// <param name="patternForKill">Pattern to use for kill connection.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mappingSource">Shard to update.</param>
        /// <param name="mappingTarget">Updated shard.</param>
        /// <param name="lockOwnerId">Lock owner.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement UpdateShardMappingGlobal(
            Guid operationId,
            StoreOperationCode operationCode,
            bool undo,
            string patternForKill,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget,
            Guid lockOwnerId)
        {
            return new XElement(
                "BulkOperationShardMappingsGlobal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.OperationCode(operationCode),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(1),
                StoreOperationRequestBuilder.s_gsmVersion,
                new XElement("PatternForKill", patternForKill),
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                new XElement("Removes",
                    StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingSource.StoreShard)),
                new XElement("Adds",
                    StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingTarget.StoreShard)),
                new XElement("Steps",
                    new XElement("Step",
                        new XAttribute("Id", 1),
                        StoreOperationRequestBuilder.Validate(false),
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Update),
                        StoreObjectFormatterXml.WriteLock(lockOwnerId),
                        StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mappingSource),
                        new XElement("Update",
                            StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mappingTarget)))));
        }

        /// <summary>
        /// Request to replace mappings in given shard map in GSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="operationCode">Operation code.</param>
        /// <param name="undo">Whether this is an undo request.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mappingsSource">Original mappings.</param>
        /// <param name="mappingsTarget">New mappings.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement ReplaceShardMappingsGlobal(
            Guid operationId,
            StoreOperationCode operationCode,
            bool undo,
            IStoreShardMap shardMap,
            Tuple<IStoreMapping, Guid>[] mappingsSource,
            Tuple<IStoreMapping, Guid>[] mappingsTarget)
        {
            Debug.Assert(mappingsSource.Length > 0);
            Debug.Assert(mappingsTarget.Length > 0);

            return new XElement(
                "BulkOperationShardMappingsGlobal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.OperationCode(operationCode),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(mappingsSource.Length + mappingsTarget.Length),
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                new XElement("Removes",
                    StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsSource[0].Item1.StoreShard)),
                new XElement("Adds",
                    StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsTarget[0].Item1.StoreShard)),
                new XElement("Steps",
                    mappingsSource.Select((mapping, i) =>
                        new XElement("Step",
                            StoreOperationRequestBuilder.Validate(false),
                            StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove),
                            new XAttribute("Id", i + 1),
                            StoreObjectFormatterXml.WriteLock(mapping.Item2),
                            StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping.Item1))),
                    mappingsTarget.Select((mapping, i) =>
                        new XElement("Step",
                            StoreOperationRequestBuilder.Validate(false),
                            StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add),
                            new XAttribute("Id", mappingsSource.Length + i + 1),
                            StoreObjectFormatterXml.WriteLock(mapping.Item2),
                            StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping.Item1)))));
        }

        /// <summary>
        /// Request to lock or unlock mappings in GSM.
        /// </summary>
        /// <param name="shardMap">Shard map whose mappings are being requested.</param>
        /// <param name="mapping">Mapping being locked or unlocked.</param>
        /// <param name="lockId">Lock Id.</param>
        /// <param name="lockOpType">Lock operation code.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement LockOrUnLockShardMappingsGlobal(
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockId,
            LockOwnerIdOpType lockOpType)
        {
            Debug.Assert(mapping != null || (lockOpType == LockOwnerIdOpType.UnlockAllMappingsForId || lockOpType == LockOwnerIdOpType.UnlockAllMappings));
            return new XElement(
                @"LockOrUnlockShardMappingsGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                mapping == null ? null : StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping),
                new XElement("Lock",
                    new XElement("Id", lockId),
                    new XElement("Operation", (int)lockOpType)));
        }

        /// <summary>
        /// Request to get all schema info objects from GSM.
        /// </summary>
        /// <returns>Xml formatted request.</returns>
        internal static XElement GetAllShardingSchemaInfosGlobal()
        {
            return new XElement(
                @"GetAllShardingSchemaInfosGlobal",
                StoreOperationRequestBuilder.s_gsmVersion);
        }

        /// <summary>
        /// Request to find schema info in GSM.
        /// </summary>
        /// <param name="name">Schema info name to find.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement FindShardingSchemaInfoGlobal(string name)
        {
            return new XElement(
                @"FindShardingSchemaInfoGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                new XElement("SchemaInfo",
                    new XElement("Name", name)));
        }

        /// <summary>
        /// Request to add schema info to GSM.
        /// </summary>
        /// <param name="schemaInfo">Schema info object to add</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement AddShardingSchemaInfoGlobal(IStoreSchemaInfo schemaInfo)
        {
            return new XElement(
                @"AddShardingSchemaInfoGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreSchemaInfo("SchemaInfo", schemaInfo));
        }

        /// <summary>
        /// Request to delete schema info object from GSM.
        /// </summary>
        /// <param name="name">Name of schema info to delete.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement RemoveShardingSchemaInfoGlobal(string name)
        {
            return new XElement(
                @"RemoveShardingSchemaInfoGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                new XElement("SchemaInfo",
                    new XElement("Name", name)));
        }

        /// <summary>
        /// Request to update schema info to GSM.
        /// </summary>
        /// <param name="schemaInfo">Schema info object to update</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement UpdateShardingSchemaInfoGlobal(IStoreSchemaInfo schemaInfo)
        {
            return new XElement(
                @"UpdateShardingSchemaInfoGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreSchemaInfo("SchemaInfo", schemaInfo));
        }

        /// <summary>
        /// Request to attach shard to GSM.
        /// </summary>
        /// <param name="shardMap">Shard map to attach.</param>
        /// <param name="shard">Shard to attach.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement AttachShardGlobal(IStoreShardMap shardMap, IStoreShard shard)
        {
            return new XElement(
                @"AttachShardGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", shard));
        }

        /// <summary>
        /// Request to detach shard to GSM.
        /// </summary>
        /// <param name="shardMapName">Optional shard map name to detach.</param>
        /// <param name="location">Location to detach.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement DetachShardGlobal(string shardMapName, ShardLocation location)
        {
            return new XElement(
                @"DetachShardGlobal",
                StoreOperationRequestBuilder.s_gsmVersion,
                shardMapName == null ?
                    new XElement("ShardMap", new XAttribute("Null", 1)) :
                    new XElement("ShardMap", new XAttribute("Null", 0), new XElement("Name", shardMapName)),
                StoreObjectFormatterXml.WriteShardLocation("Location", location));
        }

        /// <summary>
        /// Request to replace mappings in given shard map in GSM without logging.
        /// </summary>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mappingsSource">Original mappings.</param>
        /// <param name="mappingsTarget">New mappings.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement ReplaceShardMappingsGlobalWithoutLogging(
            IStoreShardMap shardMap,
            IStoreMapping[] mappingsSource,
            IStoreMapping[] mappingsTarget)
        {
            Debug.Assert(mappingsSource.Length + mappingsTarget.Length > 0, "Expecting at least one mapping for ReplaceMappingsGlobalWithoutLogging.");

            return new XElement(
                "ReplaceShardMappingsGlobal",
                StoreOperationRequestBuilder.RemoveStepsCount(mappingsSource.Length),
                StoreOperationRequestBuilder.AddStepsCount(mappingsTarget.Length),
                StoreOperationRequestBuilder.s_gsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                mappingsSource.Length > 0 ?
                    new XElement("RemoveSteps",
                        StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsSource[0].StoreShard),
                        mappingsSource.Select((mapping, i) =>
                            new XElement("Step",
                                new XAttribute("Id", i + 1),
                                StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping)))) :
                    new XElement("RemoveSteps"),
                mappingsTarget.Length > 0 ?
                    new XElement("AddSteps",
                        StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsTarget[0].StoreShard),
                        mappingsTarget.Select((mapping, i) =>
                            new XElement("Step",
                                new XAttribute("Id", i + 1),
                                StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping)))) :
                    new XElement("AddSteps"));
        }

        /// <summary>
        /// Request to get all shards and shard maps from LSM.
        /// </summary>
        /// <returns>Xml formatted request.</returns>
        internal static XElement GetAllShardsLocal()
        {
            return new XElement(
                @"GetAllShardsLocal",
                StoreOperationRequestBuilder.s_lsmVersion);
        }

        /// <summary>
        /// Validation request for shard for LSM.
        /// </summary>
        /// <param name="shardMapId">Shard map Id.</param>
        /// <param name="shardId">Shard Id.</param>
        /// <param name="shardVersion">Shard version.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement ValidateShardLocal(
            Guid shardMapId,
            Guid shardId,
            Guid shardVersion)
        {
            return new XElement(
                "ValidateShardLocal",
                StoreOperationRequestBuilder.s_lsmVersion,
                new XElement("ShardMapId", shardMapId),
                new XElement("ShardId", shardId),
                new XElement("ShardVersion", shardVersion));
        }

        /// <summary>
        /// Request to add shard to given shard map in LSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="shardMap">Shard map to add shard to.</param>
        /// <param name="shard">Shard to add.</param>
        /// <param name="undo">Whether this is undo request.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement AddShardLocal(
            Guid operationId,
            bool undo,
            IStoreShardMap shardMap,
            IStoreShard shard)
        {
            return new XElement(
                "AddShardLocal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", shard));
        }

        /// <summary>
        /// Request to remove shard from given shard map in LSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="shardMap">Shard map to remove shard from.</param>
        /// <param name="shard">Shard to remove.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement RemoveShardLocal(
            Guid operationId,
            IStoreShardMap shardMap,
            IStoreShard shard)
        {
            return new XElement(
                "RemoveShardLocal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", shard));
        }

        /// <summary>
        /// Request to update shard in given shard map in LSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="shardMap">Shard map to remove shard from.</param>
        /// <param name="shard">Shard to update.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement UpdateShardLocal(
            Guid operationId,
            IStoreShardMap shardMap,
            IStoreShard shard)
        {
            return new XElement(
                "UpdateShardLocal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", shard));
        }

        /// <summary>
        /// Request to get all shard mappings from LSM for a particular shard map
        /// and optional shard and range.
        /// </summary>
        /// <param name="shardMap">Shard map whose mappings are being requested.</param>
        /// <param name="shard">Optional shard for which mappings are being requested.</param>
        /// <param name="range">Optional range for which mappings are being requested.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement GetAllShardMappingsLocal(
            IStoreShardMap shardMap,
            IStoreShard shard,
            ShardRange range)
        {
            return new XElement(
                @"GetAllShardMappingsLocal",
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", shard),
                StoreObjectFormatterXml.WriteShardRange(range));
        }

        /// <summary>
        /// Request to get mapping from LSM for a particular key belonging to a shard map.
        /// </summary>
        /// <param name="shardMap">Shard map whose mappings are being requested.</param>
        /// <param name="key">Key being searched.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement FindShardMappingByKeyLocal(
            IStoreShardMap shardMap,
            ShardKey key)
        {
            return new XElement(
                @"FindShardMappingByKeyLocal",
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteShardKey(key));
        }

        /// <summary>
        /// Validation request for shard mapping for LSM.
        /// </summary>
        /// <param name="shardMapId">Shard map Id.</param>
        /// <param name="mappingId">Shard mapping Id.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement ValidateShardMappingLocal(
            Guid shardMapId,
            Guid mappingId)
        {
            return new XElement(
                "ValidateShardMappingLocal",
                    StoreOperationRequestBuilder.s_lsmVersion,
                    new XElement("ShardMapId", shardMapId),
                    new XElement("MappingId", mappingId));
        }


        /// <summary>
        /// Request to add mapping to given shard map in LSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mapping">Mapping to add.</param>
        /// <param name="undo">Whether this is undo request.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement AddShardMappingLocal(
            Guid operationId,
            bool undo,
            IStoreShardMap shardMap,
            IStoreMapping mapping)
        {
            return new XElement(
                "BulkOperationShardMappingsLocal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(1),
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", mapping.StoreShard),
                new XElement("Steps",
                    new XElement("Step",
                        new XAttribute("Id", 1),
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add),
                        StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping))));
        }

        /// <summary>
        /// Request to remove mapping from given shard map in LSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mapping">Mapping to remove.</param>
        /// <param name="undo">Whether this is undo operation.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement RemoveShardMappingLocal(
            Guid operationId,
            bool undo,
            IStoreShardMap shardMap,
            IStoreMapping mapping)
        {
            return new XElement(
                "BulkOperationShardMappingsLocal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(1),
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", mapping.StoreShard),
                new XElement("Steps",
                    new XElement("Step",
                        new XAttribute("Id", 1),
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove),
                        StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mapping))));
        }

        /// <summary>
        /// Request to update mapping in given shard map in LSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mappingSource">Mapping to update.</param>
        /// <param name="mappingTarget">Updated mapping.</param>
        /// <param name="undo">Whether this is undo request.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement UpdateShardMappingLocal(
            Guid operationId,
            bool undo,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget)
        {
            return new XElement(
                "BulkOperationShardMappingsLocal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(2),
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingTarget.StoreShard),
                new XElement("Steps",
                    new XElement("Step",
                        new XAttribute("Id", 1),
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove),
                        StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mappingSource)),
                    new XElement("Step",
                        new XAttribute("Id", 2),
                        StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add),
                        StoreObjectFormatterXml.WriteIStoreMapping("Mapping", mappingTarget))));
        }

        /// <summary>
        /// Request to replace mapping in given shard map in LSM.
        /// </summary>
        /// <param name="operationId">Operation Id.</param>
        /// <param name="shardMap">Shard map for which operation is being requested.</param>
        /// <param name="mappingsSource">Mappings to remove.</param>
        /// <param name="mappingsTarget">Mappings to add.</param>
        /// <param name="undo">Whether this is undo request.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement ReplaceShardMappingsLocal(
            Guid operationId,
            bool undo,
            IStoreShardMap shardMap,
            IStoreMapping[] mappingsSource,
            IStoreMapping[] mappingsTarget)
        {
            Debug.Assert(mappingsSource.Length + mappingsTarget.Length > 0, "Expecting at least one mapping for ReplaceMappingsLocal.");
            return new XElement(
                "BulkOperationShardMappingsLocal",
                StoreOperationRequestBuilder.OperationId(operationId),
                StoreOperationRequestBuilder.Undo(undo),
                StoreOperationRequestBuilder.StepsCount(mappingsSource.Length + mappingsTarget.Length),
                StoreOperationRequestBuilder.s_lsmVersion,
                StoreObjectFormatterXml.WriteIStoreShardMap("ShardMap", shardMap),
                StoreObjectFormatterXml.WriteIStoreShard("Shard", mappingsTarget.Length > 0 ? mappingsTarget[0].StoreShard : mappingsSource[0].StoreShard),
                new XElement("Steps",
                    mappingsSource.Select((m, i) =>
                        new XElement("Step",
                            new XAttribute("Id", i + 1),
                            StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Remove),
                            StoreObjectFormatterXml.WriteIStoreMapping("Mapping", m))),
                    mappingsTarget.Select((m, i) =>
                        new XElement("Step",
                            new XAttribute("Id", mappingsSource.Length + i + 1),
                            StoreOperationRequestBuilder.StepKind(StoreOperationStepKind.Add),
                            StoreObjectFormatterXml.WriteIStoreMapping("Mapping", m)))));
        }

        /// <summary>
        /// Request to kill sessions with given application name pattern in LSM.
        /// </summary>
        /// <param name="pattern">Pattern for application name.</param>
        /// <returns>Xml formatted request.</returns>
        internal static XElement KillSessionsForShardMappingLocal(string pattern)
        {
            return new XElement(
                "KillSessionsForShardMappingLocal",
                StoreOperationRequestBuilder.s_lsmVersion,
                new XElement("Pattern", pattern));
        }

        /// <summary>
        /// Adds OperationId attribute.
        /// </summary>
        /// <param name="operationId">Id of operation.</param>
        /// <returns>XAttribute for the operationId.</returns>
        private static XAttribute OperationId(Guid operationId)
        {
            return new XAttribute("OperationId", operationId);
        }

        /// <summary>
        /// Adds UndoStartState attribute.
        /// </summary>
        /// <param name="undoStartState">Number of remove steps.</param>
        /// <returns>XAttribute for the removeStepsCount.</returns>
        private static XAttribute UndoStartState(StoreOperationState undoStartState)
        {
            return new XAttribute("UndoStartState", (int)undoStartState);
        }

        /// <summary>
        /// Adds OperationCode attribute.
        /// </summary>
        /// <param name="operationCode">Code of operation.</param>
        /// <returns>XAttribute for the operationCode.</returns>
        private static XAttribute OperationCode(StoreOperationCode operationCode)
        {
            return new XAttribute("OperationCode", (int)operationCode);
        }

        /// <summary>
        /// Adds StepsCount attribute.
        /// </summary>
        /// <param name="stepsCount">Number of steps.</param>
        /// <returns>XAttribute for the StepsCount.</returns>
        private static XAttribute StepsCount(int stepsCount)
        {
            return new XAttribute("StepsCount", stepsCount);
        }

        /// <summary>
        /// Adds StepKind attribute.
        /// </summary>
        /// <param name="kind">Type of step.</param>
        /// <returns>XAttribute for the StepKind.</returns>
        private static XAttribute StepKind(StoreOperationStepKind kind)
        {
            return new XAttribute("Kind", (int)kind);
        }

        /// <summary>
        /// Adds RemoveStepsCount attribute.
        /// </summary>
        /// <param name="removeStepsCount">Number of remove steps.</param>
        /// <returns>XAttribute for the removeStepsCount.</returns>
        private static XAttribute RemoveStepsCount(int removeStepsCount)
        {
            return new XAttribute("RemoveStepsCount", removeStepsCount);
        }

        /// <summary>
        /// Adds AddStepsCount attribute.
        /// </summary>
        /// <param name="addStepsCount">Number of add steps.</param>
        /// <returns>XAttribute for the addStepsCount.</returns>
        private static XAttribute AddStepsCount(int addStepsCount)
        {
            return new XAttribute("AddStepsCount", addStepsCount);
        }

        /// <summary>
        /// Adds Undo attribute.
        /// </summary>
        /// <param name="undo">Undo request.</param>
        /// <returns>XAttribute for the undo.</returns>
        private static XAttribute Undo(bool undo)
        {
            return new XAttribute("Undo", undo ? 1 : 0);
        }

        /// <summary>
        /// Adds Validate attribute.
        /// </summary>
        /// <param name="validate">Validate request.</param>
        /// <returns>XAttribute for the validation.</returns>
        private static XAttribute Validate(bool validate)
        {
            return new XAttribute("Validate", validate ? 1 : 0);
        }
    }
}
