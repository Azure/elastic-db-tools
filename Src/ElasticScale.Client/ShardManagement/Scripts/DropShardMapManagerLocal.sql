-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Stored Procedures
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- Shard Mappings
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spKillSessionsForShardMappingLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spKillSessionsForShardMappingLocal
end
go

if object_id(N'__ShardManagement.spBulkOperationShardMappingsLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spBulkOperationShardMappingsLocal
end
go

if object_id(N'__ShardManagement.spValidateShardMappingLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spValidateShardMappingLocal
end
go

if object_id(N'__ShardManagement.spFindShardMappingByKeyLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spFindShardMappingByKeyLocal
end
go

if object_id(N'__ShardManagement.spGetAllShardMappingsLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spGetAllShardMappingsLocal
end
go

---------------------------------------------------------------------------------------------------
-- Shards
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spUpdateShardLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spUpdateShardLocal
end
go

if object_id(N'__ShardManagement.spRemoveShardLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spRemoveShardLocal
end
go

if object_id(N'__ShardManagement.spAddShardLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spAddShardLocal
end
go

if object_id(N'__ShardManagement.spValidateShardLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spValidateShardLocal
end
go

if object_id(N'__ShardManagement.spGetAllShardsLocal', N'P') is not null
begin
    drop procedure __ShardManagement.spGetAllShardsLocal
end
go

---------------------------------------------------------------------------------------------------
-- Helper SPs and Functions
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.spGetStoreVersionLocalHelper', N'P') is not null
begin
    drop procedure __ShardManagement.spGetStoreVersionLocalHelper
end
go

if object_id(N'__ShardManagement.fnGetStoreVersionLocal', N'FN') is not null
begin
    drop function __ShardManagement.fnGetStoreVersionLocal
end
go

if object_id(N'__ShardManagement.fnGetStoreVersionMajorLocal', N'FN') is not null
begin
    drop function __ShardManagement.fnGetStoreVersionMajorLocal
end
go

---------------------------------------------------------------------------------------------------
-- Tables
---------------------------------------------------------------------------------------------------
if object_id(N'__ShardManagement.fkShardMappingsLocal_ShardId', N'F') is not null
begin
	alter table __ShardManagement.ShardMappingsLocal
		drop constraint fkShardMappingsLocal_ShardId
end
go

if object_id(N'__ShardManagement.fkShardMappingsLocal_ShardMapId', N'F') is not null
begin
	alter table __ShardManagement.ShardMappingsLocal
		drop constraint fkShardMappingsLocal_ShardMapId
end
go

if object_id(N'__ShardManagement.fkShardsLocal_ShardMapId ', N'F') is not null
begin
	alter table __ShardManagement.ShardsLocal
		drop constraint fkShardsLocal_ShardMapId
end
go

if object_id(N'__ShardManagement.ucShardMappingsLocal_ShardMapId_MinValue', N'UQ') is not null
begin
	alter table __ShardManagement.ShardMappingsLocal 
		drop constraint ucShardMappingsLocal_ShardMapId_MinValue
end
go

if object_id(N'__ShardManagement.ucShardsLocal_ShardMapId_Location', N'UQ') is not null
begin
	alter table __ShardManagement.ShardsLocal
		drop constraint ucShardsLocal_ShardMapId_Location
end
go

-- DEVNOTE(wbasheer): Introduce this once we allow overwrite existing semantics on CreateShard
--if object_id(N'__ShardManagement.ucShardMapsLocal_Name', N'UQ') is not null
--begin
--	alter table __ShardManagement.ShardsLocal
--		drop constraint ucShardMapsLocal_Name
--end
--go

if object_id(N'__ShardManagement.ShardMappingsLocal', N'U') is not null
begin
    drop table __ShardManagement.ShardMappingsLocal
end
go

if object_id(N'__ShardManagement.ShardsLocal', N'U') is not null
begin
    drop table __ShardManagement.ShardsLocal
end
go

if object_id(N'__ShardManagement.ShardMapsLocal', N'U') is not null
begin
    drop table __ShardManagement.ShardMapsLocal
end
go

if object_id(N'__ShardManagement.ShardMapManagerLocal', N'U') is not null
begin
    drop table __ShardManagement.ShardMapManagerLocal
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
