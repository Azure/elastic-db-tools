-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Schema
---------------------------------------------------------------------------------------------------
if schema_id('__ShardManagement') is null
begin
	exec sp_executesql N'create schema __ShardManagement'
end
go

---------------------------------------------------------------------------------------------------
-- Tables
---------------------------------------------------------------------------------------------------
create table __ShardManagement.ShardMapManagerLocal(
StoreVersion int not null
)
go

create table __ShardManagement.ShardMapsLocal(
ShardMapId uniqueidentifier not null, 
Name nvarchar(50) collate SQL_Latin1_General_CP1_CI_AS not null,
MapType int not null,
KeyType int not null,
LastOperationId uniqueidentifier default('00000000-0000-0000-0000-000000000000') not null
)
go

create table __ShardManagement.ShardsLocal(
ShardId uniqueidentifier not null,
Version uniqueidentifier not null,
ShardMapId uniqueidentifier not null, 
Protocol int not null,
ServerName nvarchar(128) collate SQL_Latin1_General_CP1_CI_AS not null, 
Port int not null,
DatabaseName nvarchar(128) collate SQL_Latin1_General_CP1_CI_AS not null, 
Status int not null, -- user defined
LastOperationId uniqueidentifier default('00000000-0000-0000-0000-000000000000') not null
)
go

create table __ShardManagement.ShardMappingsLocal(
MappingId uniqueidentifier not null,
ShardId uniqueidentifier not null,
ShardMapId uniqueidentifier not null,
MinValue varbinary(128) not null, 
MaxValue varbinary(128), -- nulls are allowed since +ve infinity is represented by null
Status int not null, -- 0 online, 1 offline
LockOwnerId uniqueidentifier default('00000000-0000-0000-0000-000000000000') not null,
LastOperationId uniqueidentifier default('00000000-0000-0000-0000-000000000000') not null
)
go

---------------------------------------------------------------------------------------------------
-- Constraints
---------------------------------------------------------------------------------------------------
alter table __ShardManagement.ShardMapManagerLocal 
	add constraint pkShardMapManagerLocal_StoreVersion primary key (StoreVersion)
go

alter table __ShardManagement.ShardMapsLocal 
	add constraint pkShardMapsLocal_ShardMapId primary key (ShardMapId)
go

alter table __ShardManagement.ShardsLocal
	add constraint pkShardsLocal_ShardId primary key (ShardId)
go

alter table __ShardManagement.ShardMappingsLocal
	add constraint pkShardMappingsLocal_MappingId primary key (MappingId)
go

-- DEVNOTE(wbasheer): Introduce this once we allow overwrite existing semantics on CreateShard
--alter table __ShardManagement.ShardMapsLocal
--	add constraint ucShardMapsLocal_Name unique (Name)
--go

alter table __ShardManagement.ShardsLocal
	add constraint ucShardsLocal_ShardMapId_Location unique (ShardMapId, Protocol, ServerName, DatabaseName, Port)
go

alter table __ShardManagement.ShardMappingsLocal
	add constraint ucShardMappingsLocal_ShardMapId_MinValue unique (ShardMapId, MinValue)
go

alter table __ShardManagement.ShardsLocal
	add constraint fkShardsLocal_ShardMapId foreign key (ShardMapId) references __ShardManagement.ShardMapsLocal(ShardMapId)
go

alter table __ShardManagement.ShardMappingsLocal
	add constraint fkShardMappingsLocal_ShardMapId foreign key (ShardMapId) references __ShardManagement.ShardMapsLocal(ShardMapId)
go

alter table __ShardManagement.ShardMappingsLocal
	add constraint fkShardMappingsLocal_ShardId foreign key (ShardId) references __ShardManagement.ShardsLocal(ShardId)
go

---------------------------------------------------------------------------------------------------
-- Data
---------------------------------------------------------------------------------------------------
insert into 
	__ShardManagement.ShardMapManagerLocal (StoreVersion)
values 
	(1)
go

---------------------------------------------------------------------------------------------------
-- Result Codes: keep these in sync with enum StoreResult in IStoreResults.cs
---------------------------------------------------------------------------------------------------
-- 001 => Success.

-- 050 => Missing parameters for stored procedure.
-- 051 => Store Version mismatch.
-- 052 => There is a pending operation on shard.
-- 053 => Unexpected store error.

-- 101 => A shard map with the given Name already exists.
-- 102 => The shard map does not exist.
-- 103 => The shard map cannot be deleted since there are shards associated with it.

-- 201 => The shard already exists.
-- 202 => The shard does not exist.
-- 203 => The shard already has some mappings associated with it.
-- 204 => The shard Version does not match with the Version specified.
-- 205 => The shard map already has the same location associated with another shard.

-- 301 => The shard mapping does not exist.
-- 302 => Range specified is already mapped by another shard mapping.
-- 303 => Point specified is already mapped by another shard mapping.
-- 304 => Shard mapping could not be found for key.
-- 305 => Unable to kill sessions corresponding to shard mapping.
-- 306 => Shard mapping is not offline.
-- 307 => Lock owner Id of shard mapping does not match.
-- 308 => Shard mapping is already locked.
-- 309 => Mapping is offline.

-- 401 => Schema Info Name Does Not Exist.
-- 402 => Schema Info Name Conflict.

---------------------------------------------------------------------------------------------------
-- Rowset Codes
---------------------------------------------------------------------------------------------------
-- 1 => ShardMap
-- 2 => Shard
-- 3 => ShardMapping
-- 4 => ShardLocation
-- 5 => StoreVersion
-- 6 => Operation
-- 7 => SchemaInfo

---------------------------------------------------------------------------------------------------
-- Shard Map Kind
---------------------------------------------------------------------------------------------------
-- 0 => Default
-- 1 => List
-- 2 => Range

---------------------------------------------------------------------------------------------------
-- Shard Mapping Status
---------------------------------------------------------------------------------------------------
-- 0 => Offline
-- 1 => Online

---------------------------------------------------------------------------------------------------
-- Operation Kind
---------------------------------------------------------------------------------------------------
-- NULL => None
-- 1 => Add/Update
-- 2 => Remove

---------------------------------------------------------------------------------------------------
-- Bulk Operation Step Types
---------------------------------------------------------------------------------------------------
-- 1 => Remove
-- 2 => Update
-- 3 => Add

---------------------------------------------------------------------------------------------------
-- Helper SPs and Functions
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- __ShardManagement.fnGetStoreVersionLocal
---------------------------------------------------------------------------------------------------
create function __ShardManagement.fnGetStoreVersionLocal()
returns int
as
begin
	return (select StoreVersion from __ShardManagement.ShardMapManagerLocal)
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetStoreVersionLocalHelper
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetStoreVersionLocalHelper
as
begin
	select
		5, StoreVersion
	from 
		__ShardManagement.ShardMapManagerLocal
end
go

---------------------------------------------------------------------------------------------------
-- Stored Procedures
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- Shards
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardsLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetAllShardsLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int

	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int')
	from 
		@input.nodes('/GetAllShardsLocal') as t(x)

	if (@lsmVersionClient is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient > __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	-- shard maps
	select 
		1, ShardMapId, Name, MapType, KeyType
	from 
		__ShardManagement.ShardMapsLocal

	-- shards
	select 
		2, ShardId, Version, ShardMapId, Protocol, ServerName, Port, DatabaseName, Status 
	from
		__ShardManagement.ShardsLocal

	goto Success_Exit;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Success_Exit:
	set @result = 1
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spValidateShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spValidateShardLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int,
			@shardMapId uniqueidentifier,
			@shardId uniqueidentifier,
			@shardVersion uniqueidentifier
	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMapId)[1]', 'uniqueidentifier'),
		@shardId = x.value('(ShardId)[1]', 'uniqueidentifier'),
		@shardVersion = x.value('(ShardVersion)[1]', 'uniqueidentifier')
	from 
		@input.nodes('/ValidateShardLocal') as t(x)

	if (@lsmVersionClient is null or @shardMapId is null or @shardId is null or @shardVersion is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient > __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	-- find shard map
	declare @currentShardMapId uniqueidentifier
	
	select 
		@currentShardMapId = ShardMapId 
	from 
		__ShardManagement.ShardMapsLocal
	where 
		ShardMapId = @shardMapId

	if (@currentShardMapId is null)
		goto Error_ShardMapNotFound;

	declare @currentShardVersion uniqueidentifier

	select 
		@currentShardVersion = Version 
	from 
		__ShardManagement.ShardsLocal
	where 
		ShardMapId = @shardMapId and ShardId = @shardId

	if (@currentShardVersion is null)
		goto Error_ShardDoesNotExist;

	if (@currentShardVersion <> @shardVersion)
		goto Error_ShardVersionMismatch;

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_ShardDoesNotExist:
	set @result = 202
	goto Exit_Procedure;

Error_ShardVersionMismatch:
	set @result = 204
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spAddShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spAddShardLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int,
			@operationId uniqueidentifier,
			@shardMapId uniqueidentifier,
			@shardId uniqueidentifier,
			@name nvarchar(50),
			@sm_kind int,
			@sm_keykind int,
			@shardVersion uniqueidentifier,
			@protocol int,
			@serverName nvarchar(128),
			@port int,
			@databaseName nvarchar(128),
			@shardStatus  int

	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@name = x.value('(ShardMap/Name)[1]', 'nvarchar(50)'),
		@sm_kind = x.value('(ShardMap/Kind)[1]', 'int'),
		@sm_keykind = x.value('(ShardMap/KeyKind)[1]', 'int'),
		@shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
		@shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier'),
		@protocol = x.value('(Shard/Location/Protocol)[1]', 'int'),
		@serverName = x.value('(Shard/Location/ServerName)[1]', 'nvarchar(128)'),
		@port = x.value('(Shard/Location/Port)[1]', 'int'),
		@databaseName = x.value('(Shard/Location/DatabaseName)[1]', 'nvarchar(128)'),
		@shardStatus = x.value('(Shard/Status)[1]', 'int')
	from 
		@input.nodes('/AddShardLocal') as t(x)

	if (@lsmVersionClient is null or @shardMapId is null or @operationId is null or @name is null or @sm_kind is null or @sm_keykind is null or 
		@shardId is null or @shardVersion is null or @protocol is null or @serverName is null or 
		@port is null or @databaseName is null or @shardStatus is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient <> __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	-- check for reentrancy
	if exists (
		select 
			ShardMapId
		from
			__ShardManagement.ShardMapsLocal
		where
			ShardMapId = @shardMapId and LastOperationId = @operationId)
		goto Success_Exit;

	-- add shard map row
	insert into 
		__ShardManagement.ShardMapsLocal 
		(ShardMapId, Name, MapType, KeyType, LastOperationId)
	values 
		(@shardMapId, @name, @sm_kind, @sm_keykind, @operationId) 

	-- add shard row
	insert into 
		__ShardManagement.ShardsLocal(
		ShardId, 
		Version, 
		ShardMapId, 
		Protocol, 
		ServerName, 
		Port, 
		DatabaseName, 
		Status,
		LastOperationId)
	values (
		@shardId, 
		@shardVersion, 
		@shardMapId,
		@protocol, 
		@serverName, 
		@port, 
		@databaseName, 
		@shardStatus,
		@operationId) 

	goto Success_Exit;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Success_Exit:
	set @result = 1
	goto Exit_Procedure;
	
Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spRemoveShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spRemoveShardLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int,
			@operationId uniqueidentifier,
			@shardMapId uniqueidentifier,
			@shardId uniqueidentifier

	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier')
	from 
		@input.nodes('/RemoveShardLocal') as t(x)

	if (@lsmVersionClient is null or @operationId is null or @shardMapId is null or @shardId is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient <> __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	-- remove shard row
	delete from
		__ShardManagement.ShardsLocal 
	where
		ShardMapId = @shardMapId and ShardId = @shardId

	-- remove shard map row
	delete from
		__ShardManagement.ShardMapsLocal 
	where
		ShardMapId = @shardMapId

	set @result = 1
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spUpdateShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spUpdateShardLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int,
			@operationId uniqueidentifier,
			@shardMapId uniqueidentifier,
			@shardId uniqueidentifier,
			@shardVersion uniqueidentifier,
			@shardStatus int

	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
		@shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier'),
		@shardStatus = x.value('(Shard/Status)[1]', 'int')
	from 
		@input.nodes('/UpdateShardLocal') as t(x)

	if (@lsmVersionClient is null or @operationId is null or @shardMapId is null or @shardId is null or @shardVersion is null or @shardStatus is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient <> __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	update 
		__ShardManagement.ShardsLocal
	set
		Version = @shardVersion,
		Status = @shardStatus,
		LastOperationId = @operationId
	where
		ShardMapId = @shardMapId and ShardId = @shardId

	set @result = 1
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- Shard Mappings
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardMappingsLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetAllShardMappingsLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int
	declare @shardMapId uniqueidentifier
	declare @shardId uniqueidentifier
	declare @minValue varbinary(128)
	declare @maxValue varbinary(128)

	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
		@minValue = convert(varbinary(128), x.value('(Range[@Null="0"]/MinValue)[1]', 'varchar(258)'), 1),
		@maxValue = convert(varbinary(128), x.value('(Range[@Null="0"]/MaxValue[@Null="0"])[1]', 'varchar(258)'), 1)
	from 
		@input.nodes('/GetAllShardMappingsLocal') as t(x)

	if (@lsmVersionClient is null or @shardMapId is null or @shardId is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient > __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	declare @mapType int

	select 
		@mapType = MapType
	from
		__ShardManagement.ShardMapsLocal
	where
		ShardMapId = @shardMapId

	if (@mapType is null)
		goto Error_ShardMapNotFound;

	declare @minValueCalculated varbinary(128) = 0x,
			@maxValueCalculated varbinary(128) = null

	-- check if range is supplied and update accordingly.
	if (@minValue is not null)
		set @minValueCalculated = @minValue

	if (@maxValue is not null)
		set @maxValueCalculated = @maxValue

	if (@mapType = 1)
	begin	
		select 
			3, m.MappingId, m.ShardMapId, m.MinValue, m.MaxValue, m.Status, m.LockOwnerId,  -- fields for SqlMapping
			s.ShardId, s.Version, s.ShardMapId, s.Protocol, s.ServerName, s.Port, s.DatabaseName, s.Status -- fields for SqlShard, ShardMapId is repeated here
		from 
			__ShardManagement.ShardMappingsLocal m 
			join 
			__ShardManagement.ShardsLocal s 
			on 
				m.ShardId = s.ShardId
		where
			m.ShardMapId = @shardMapId and 
			m.ShardId = @shardId and 
			MinValue >= @minValueCalculated and 
			((@maxValueCalculated is null) or (MinValue < @maxValueCalculated))
		order by 
			m.MinValue
	end
	else
	begin
		select 
			3, m.MappingId, m.ShardMapId, m.MinValue, m.MaxValue, m.Status, m.LockOwnerId,  -- fields for SqlMapping
			s.ShardId, s.Version, s.ShardMapId, s.Protocol, s.ServerName, s.Port, s.DatabaseName, s.Status -- fields for SqlShard, ShardMapId is repeated here
		from 
			__ShardManagement.ShardMappingsLocal m 
			join 
			__ShardManagement.ShardsLocal s 
			on 
				m.ShardId = s.ShardId
		where
			m.ShardMapId = @shardMapId and 
			m.ShardId = @shardId and 
			((MaxValue is null) or (MaxValue > @minValueCalculated)) and 
			((@maxValueCalculated is null) or (MinValue < @maxValueCalculated))
		order by 
			m.MinValue
	end

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardMappingByKeyLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spFindShardMappingByKeyLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int,
			@shardMapId uniqueidentifier,
			@keyValue varbinary(128)

	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@keyValue = convert(varbinary(128), x.value('(Key/Value)[1]', 'varchar(258)'), 1)
	from 
		@input.nodes('/FindShardMappingByKeyLocal') as t(x)
	
	if (@lsmVersionClient is null or @shardMapId is null or @keyValue is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient > __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	declare @mapType int

	select 
		@mapType = MapType
	from
		__ShardManagement.ShardMapsLocal
	where
		ShardMapId = @shardMapId

	if (@mapType is null)
		goto Error_ShardMapNotFound;

	if (@mapType = 1)
	begin	
		select
			3, m.MappingId, m.ShardMapId, m.MinValue, m.MaxValue, m.Status, m.LockOwnerId,  -- fields for SqlMapping
			s.ShardId, s.Version, s.ShardMapId, s.Protocol, s.ServerName, s.Port, s.DatabaseName, s.Status -- fields for SqlShard, ShardMapId is repeated here
		from
			__ShardManagement.ShardMappingsLocal m
		join 
			__ShardManagement.ShardsLocal s
		on 
			m.ShardId = s.ShardId
		where
			m.ShardMapId = @shardMapId and 
			m.MinValue = @keyValue
	end
	else
	begin
		select 
			3, m.MappingId, m.ShardMapId, m.MinValue, m.MaxValue, m.Status, m.LockOwnerId,  -- fields for SqlMapping
			s.ShardId, s.Version, s.ShardMapId, s.Protocol, s.ServerName, s.Port, s.DatabaseName, s.Status -- fields for SqlShard, ShardMapId is repeated here
		from 
			__ShardManagement.ShardMappingsLocal m 
		join 
			__ShardManagement.ShardsLocal s 
		on 
			m.ShardId = s.ShardId
		where
			m.ShardMapId = @shardMapId and 
			m.MinValue <= @keyValue and (m.MaxValue is null or m.MaxValue > @keyValue)
	end

	if (@@rowcount = 0)
		goto Error_KeyNotFound;

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_KeyNotFound:
	set @result = 304
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spValidateShardMappingLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spValidateShardMappingLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int,
			@shardMapId uniqueidentifier,
			@mappingId uniqueidentifier

	select
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMapId)[1]', 'uniqueidentifier'),
		@mappingId = x.value('(MappingId)[1]', 'uniqueidentifier')
	from
		@input.nodes('/ValidateShardMappingLocal') as t(x)

	if (@lsmVersionClient is null or @shardMapId is null or @mappingId is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient <> __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	-- find shard map
	declare @currentShardMapId uniqueidentifier
	
	select 
		@currentShardMapId = ShardMapId 
	from 
		__ShardManagement.ShardMapsLocal
	where 
		ShardMapId = @shardMapId

	if (@currentShardMapId is null)
		goto Error_ShardMapNotFound;

	declare @m_status_current int

	select 
		@m_status_current = Status
	from
		__ShardManagement.ShardMappingsLocal
	where
		ShardMapId = @shardMapId and MappingId = @mappingId
			
	if (@m_status_current is null)
		goto Error_MappingDoesNotExist;

	if (@m_status_current <> 1)
		goto Error_MappingIsOffline;

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MappingDoesNotExist:
	set @result = 301
	goto Exit_Procedure;

Error_MappingIsOffline:
	set @result = 309
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spBulkOperationShardMappingsLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spBulkOperationShardMappingsLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int,
			@operationId uniqueidentifier,
			@operationCode int,
			@stepsCount int,
			@shardMapId uniqueidentifier,
			@sm_kind int,
			@shardId uniqueidentifier,
			@shardVersion uniqueidentifier

	-- get operation information as well as number of steps information
	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@stepsCount = x.value('(@StepsCount)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
		@shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier')
	from 
		@input.nodes('/BulkOperationShardMappingsLocal') as t(x)

	if (@lsmVersionClient is null or @operationId is null or @stepsCount is null or @shardMapId is null or @shardId is null or @shardVersion is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient <> __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	-- check for reentrancy
	if exists (
		select 
			ShardId
		from
			__ShardManagement.ShardsLocal
		where
			ShardMapId = @shardMapId and ShardId = @shardId and Version = @shardVersion and LastOperationId = @operationId)
		goto Success_Exit;

	-- update the shard entry
	update __ShardManagement.ShardsLocal
	set
		Version = @shardVersion,
		LastOperationId = @operationId
	where
		ShardMapId = @shardMapId and ShardId = @shardId

	declare	@currentStep xml,
			@stepIndex int = 1,
			@stepType int,
			@stepMappingId uniqueidentifier

	while (@stepIndex <= @stepsCount)
	begin
		select 
			@currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]') 
		from 
			@input.nodes('/BulkOperationShardMappingsLocal/Steps') as t(x)

		-- Identify the step type.
		select
			@stepType = x.value('(@Kind)[1]', 'int'),
			@stepMappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier')
		from
			@currentStep.nodes('./Step') as t(x)
	
		if (@stepType is null or @stepMappingId is null)
			goto Error_MissingParameters;

		if (@stepType = 1)
		begin
			-- Remove Mapping
			delete
				__ShardManagement.ShardMappingsLocal
			where
				ShardMapId = @shardMapId and MappingId = @stepMappingId
		end
		else
		if (@stepType = 3)
		begin
			declare @stepMinValue varbinary(128),
					@stepMaxValue varbinary(128),
					@stepMappingStatus int

			-- AddMapping
			select 
				@stepMinValue = convert(varbinary(128), x.value('(Mapping/MinValue)[1]', 'varchar(258)'), 1),
				@stepMaxValue = convert(varbinary(128), x.value('(Mapping/MaxValue[@Null="0"])[1]', 'varchar(258)'), 1),
				@stepMappingStatus = x.value('(Mapping/Status)[1]', 'int')
			from
				@currentStep.nodes('./Step') as t(x)

			if (@stepMinValue is null or @stepMappingStatus is null)
				goto Error_MissingParameters;

			-- add mapping
			insert into
				__ShardManagement.ShardMappingsLocal
				(MappingId, 
				 ShardId, 
				 ShardMapId, 
				 MinValue, 
				 MaxValue, 
				 Status,
				 LastOperationId)
			values
				(@stepMappingId, 
				 @shardId, 
				 @shardMapId, 
				 @stepMinValue, 
				 @stepMaxValue, 
				 @stepMappingStatus,
				 @operationId)

			set @stepMinValue = null
			set @stepMaxValue = null
			set @stepMappingStatus = null
		end

		-- reset state for next iteration
		set @stepType = null
		set @stepMappingId = null

		set @stepIndex = @stepIndex + 1
	end

	goto Success_Exit;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Success_Exit:
	set @result = 1
	goto Exit_Procedure;
	
Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spKillSessionsForShardMappingLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spKillSessionsForShardMappingLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionClient int,
			@patternForKill nvarchar(128)

	-- get operation information as well as number of steps information
	select 
		@lsmVersionClient = x.value('(LsmVersion)[1]', 'int'),
		@patternForKill = x.value('(Pattern)[1]', 'nvarchar(128)')
	from 
		@input.nodes('/KillSessionsForShardMappingLocal') as t(x)

	if (@lsmVersionClient is null or @patternForKill is null)
		goto Error_MissingParameters;

	if (@lsmVersionClient <> __ShardManagement.fnGetStoreVersionLocal())
		goto Error_LSMVersionMismatch;

	declare @tvKillCommands table (spid smallint primary key, commandForKill nvarchar(10))

	-- insert empty row
	insert into 
		@tvKillCommands (spid, commandForKill) 
	values 
		(0, N'')

	insert into 
		@tvKillCommands(spid, commandForKill) 
		select 
			session_id, 'kill ' + convert(nvarchar(10), session_id)
		from 
			sys.dm_exec_sessions 
		where 
			session_id > 50 and program_name like '%' + @patternForKill + '%'

	declare @currentSpid int, 
			@currentCommandForKill nvarchar(10)

	declare @current_error int

	select top 1 
		@currentSpid = spid, 
		@currentCommandForKill = commandForKill 
	from 
		@tvKillCommands 
	order by 
		spid desc

	while (@currentSpid > 0)
	begin
		begin try
			-- kill the current spid
			exec (@currentCommandForKill)

			-- remove the current row
			delete 
				@tvKillCommands 
			where 
				spid = @currentSpid

			-- get next row
			select top 1 
				@currentSpid = spid, 
				@currentCommandForKill = commandForKill 
			from 
				@tvKillCommands 
			order by 
				spid desc
		end try
		begin catch
			-- if the process is no longer valid, assume that it is gone
			if (error_number() <> 6106)
				goto Error_UnableToKillSessions;
		end catch
	end

	set @result = 1
	goto Exit_Procedure;
	
Error_UnableToKillSessions:
	set @result = 305
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go
