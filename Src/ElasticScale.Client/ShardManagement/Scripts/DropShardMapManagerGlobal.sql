-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Stored Procedures
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- Recovery
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spReplaceShardMappingsGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spReplaceShardMappingsGlobal
end
go

if object_id(N'__ShardManagement.spDetachShardGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spDetachShardGlobal
end
go

if object_id(N'__ShardManagement.spAttachShardGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spAttachShardGlobal
end
go

---------------------------------------------------------------------------------------------------
-- Sharding Schema Infos
---------------------------------------------------------------------------------------------------
if (object_id('__ShardManagement.spUpdateShardingSchemaInfoGlobal', N'P') is not null)
begin
	drop procedure __ShardManagement.spUpdateShardingSchemaInfoGlobal
end
go

if (object_id('__ShardManagement.spRemoveShardingSchemaInfoGlobal', N'P') is not null)
begin
	drop procedure __ShardManagement.spRemoveShardingSchemaInfoGlobal
end
go

if (object_id('__ShardManagement.spAddShardingSchemaInfoGlobal', N'P') is not null)
begin
	drop procedure __ShardManagement.spAddShardingSchemaInfoGlobal
end
go

if (object_id('__ShardManagement.spFindShardingSchemaInfoByNameGlobal', N'P') is not null)
begin
	drop procedure __ShardManagement.spFindShardingSchemaInfoByNameGlobal
end
go

if (object_id('__ShardManagement.spGetAllShardingSchemaInfosGlobal', N'P') is not null)
begin
	drop procedure __ShardManagement.spGetAllShardingSchemaInfosGlobal
end
go

---------------------------------------------------------------------------------------------------
-- Shard Mappings
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spLockOrUnlockShardMappingsGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spLockOrUnlockShardMappingsGlobal
end
go

if object_id(N'__ShardManagement.spBulkOperationShardMappingsGlobalEnd', N'P') is not null
begin
    drop procedure __ShardManagement.spBulkOperationShardMappingsGlobalEnd
end
go

if object_id(N'__ShardManagement.spBulkOperationShardMappingsGlobalBegin', N'P') is not null
begin
    drop procedure __ShardManagement.spBulkOperationShardMappingsGlobalBegin
end
go

if object_id(N'__ShardManagement.spFindShardMappingByIdGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spFindShardMappingByIdGlobal
end
go

if object_id(N'__ShardManagement.spFindShardMappingByKeyGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spFindShardMappingByKeyGlobal
end
go

if object_id(N'__ShardManagement.spGetAllShardMappingsGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spGetAllShardMappingsGlobal
end
go

---------------------------------------------------------------------------------------------------
-- Shards
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spBulkOperationShardsGlobalEnd', N'P') is not null
begin
    drop procedure __ShardManagement.spBulkOperationShardsGlobalEnd
end
go

if object_id(N'__ShardManagement.spBulkOperationShardsGlobalBegin', N'P') is not null
begin
    drop procedure __ShardManagement.spBulkOperationShardsGlobalBegin
end
go

if object_id(N'__ShardManagement.spFindShardByLocationGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spFindShardByLocationGlobal
end
go

if object_id(N'__ShardManagement.spGetAllShardsGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spGetAllShardsGlobal
end
go

---------------------------------------------------------------------------------------------------
-- Shard Maps
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spRemoveShardMapGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spRemoveShardMapGlobal
end
go

if object_id(N'__ShardManagement.spAddShardMapGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spAddShardMapGlobal
end
go

if object_id(N'__ShardManagement.spGetAllDistinctShardLocationsGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spGetAllDistinctShardLocationsGlobal
end
go

if object_id(N'__ShardManagement.spFindShardMapByNameGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spFindShardMapByNameGlobal
end
go

if object_id(N'__ShardManagement.spGetAllShardMapsGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spGetAllShardMapsGlobal
end
go

---------------------------------------------------------------------------------------------------
-- Operations
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal', N'P') is not null
begin
    drop procedure __ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal
end
go

---------------------------------------------------------------------------------------------------
-- Helper SPs and Functions
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spGetOperationLogEntryGlobalHelper', N'P') is not null
begin
    drop procedure __ShardManagement.spGetOperationLogEntryGlobalHelper
end
go

if object_id(N'__ShardManagement.spGetStoreVersionGlobalHelper', N'P') is not null
begin
    drop procedure __ShardManagement.spGetStoreVersionGlobalHelper
end
go

if object_id(N'__ShardManagement.fnGetStoreVersionGlobal', N'FN') is not null
begin
    drop function __ShardManagement.fnGetStoreVersionGlobal
end
go

if object_id(N'__ShardManagement.fnGetStoreVersionMajorGlobal', N'FN') is not null
begin
    drop function __ShardManagement.fnGetStoreVersionMajorGlobal
end
go

---------------------------------------------------------------------------------------------------
-- Constraints
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.fkShardMappingsGlobal_ShardId', N'F') is not null
begin
	alter table __ShardManagement.ShardMappingsGlobal
		drop constraint fkShardMappingsGlobal_ShardId
end
go

if object_id(N'__ShardManagement.fkShardMappingsGlobal_ShardMapId', N'F') is not null
begin
	alter table __ShardManagement.ShardMappingsGlobal
		drop constraint fkShardMappingsGlobal_ShardMapId
end
go

if object_id(N'__ShardManagement.fkShardsGlobal_ShardMapId ', N'F') is not null
begin
	alter table __ShardManagement.ShardsGlobal
		drop constraint fkShardsGlobal_ShardMapId
end
go

if object_id(N'__ShardManagement.ucShardMappingsGlobal_MappingId', N'UQ') is not null
begin
	alter table __ShardManagement.ShardMappingsGlobal 
		drop constraint ucShardMappingsGlobal_MappingId
end
go

if object_id(N'__ShardManagement.ucShardsGlobal_Location', N'UQ') is not null
begin
	alter table __ShardManagement.ShardsGlobal
		drop constraint ucShardsGlobal_Location
end
go

if object_id(N'__ShardManagement.ucShardMapsGlobal_Name', N'UQ') is not null
begin
	alter table __ShardManagement.ShardMapsGlobal 
		drop constraint ucShardMapsGlobal_Name
end
go

---------------------------------------------------------------------------------------------------
-- Tables
---------------------------------------------------------------------------------------------------
if (object_id('__ShardManagement.ShardedDatabaseSchemaInfosGlobal', N'U') is not null)
begin
	drop table __ShardManagement.ShardedDatabaseSchemaInfosGlobal
end
go

if object_id(N'__ShardManagement.OperationsLogGlobal', N'U') is not null
begin
    drop table __ShardManagement.OperationsLogGlobal
end
go

if object_id(N'__ShardManagement.ShardMappingsGlobal', N'U') is not null
begin
    drop table __ShardManagement.ShardMappingsGlobal
end
go

if object_id(N'__ShardManagement.ShardsGlobal', N'U') is not null
begin
    drop table __ShardManagement.ShardsGlobal
end
go

if object_id(N'__ShardManagement.ShardMapsGlobal', N'U') is not null
begin
    drop table __ShardManagement.ShardMapsGlobal
end
go

if object_id(N'__ShardManagement.ShardMapManagerGlobal', N'U') is not null
begin
    drop table __ShardManagement.ShardMapManagerGlobal
end
go

---------------------------------------------------------------------------------------------------
-- Schema
---------------------------------------------------------------------------------------------------
if schema_id('__ShardManagement') is not null
begin
	drop schema __ShardManagement
end
go
