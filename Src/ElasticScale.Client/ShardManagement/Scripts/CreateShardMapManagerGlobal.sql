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
create table __ShardManagement.ShardMapManagerGlobal(
StoreVersion int not null
)
go

create table __ShardManagement.ShardMapsGlobal(
ShardMapId uniqueidentifier not null, 
Name nvarchar(50) collate SQL_Latin1_General_CP1_CI_AS not null, 
ShardMapType int not null,
KeyType int not null
)
go

create table __ShardManagement.ShardsGlobal(
ShardId uniqueidentifier not null, 
Readable bit not null,
Version uniqueidentifier not null, 
ShardMapId uniqueidentifier not null, 
OperationId uniqueidentifier,
Protocol int not null,
ServerName nvarchar(128) collate SQL_Latin1_General_CP1_CI_AS not null, 
Port int not null,
DatabaseName nvarchar(128) collate SQL_Latin1_General_CP1_CI_AS not null, 
Status int not null -- user defined
)
go

create table __ShardManagement.ShardMappingsGlobal(
MappingId uniqueidentifier not null,
Readable bit not null,
ShardId uniqueidentifier not null,
ShardMapId uniqueidentifier not null,
OperationId uniqueidentifier,
MinValue varbinary(128) not null, 
MaxValue varbinary(128), -- null = +infinity for range shard map
Status int not null, 
LockOwnerId uniqueidentifier default('00000000-0000-0000-0000-000000000000') not null
)
go

create table __ShardManagement.OperationsLogGlobal(
OperationId uniqueidentifier not null,
OperationCode int not null, 
Data xml not null,
UndoStartState int default (100) not null,
ShardVersionRemoves uniqueidentifier,
ShardVersionAdds uniqueidentifier
)
go

create table __ShardManagement.ShardedDatabaseSchemaInfosGlobal(
Name nvarchar(128) not null,
SchemaInfo xml not null
)
go

---------------------------------------------------------------------------------------------------
-- Constraints
---------------------------------------------------------------------------------------------------
alter table __ShardManagement.ShardMapManagerGlobal 
	add constraint pkShardMapManagerGlobal_StoreVersion primary key (StoreVersion)
go

alter table __ShardManagement.ShardMapsGlobal 
	add constraint pkShardMapsGlobal_ShardMapId primary key (ShardMapId)
go

alter table __ShardManagement.ShardsGlobal
	add constraint pkShardsGlobal_ShardId primary key (ShardId)
go

alter table __ShardManagement.ShardMappingsGlobal
	add constraint pkShardMappingsGlobal_ShardMapId_MinValue_Readable primary key (ShardMapId, MinValue, Readable)
go

alter table __ShardManagement.OperationsLogGlobal
	add constraint pkOperationsLogGlobal_OperationId primary key (OperationId)
go

alter table __ShardManagement.ShardedDatabaseSchemaInfosGlobal
	add constraint pkShardedDatabaseSchemaInfosGlobal_Name primary key (Name)
go

alter table __ShardManagement.ShardMapsGlobal 
	add constraint ucShardMapsGlobal_Name unique (Name)
go

alter table __ShardManagement.ShardsGlobal
	add constraint ucShardsGlobal_Location unique (ShardMapId, Protocol, ServerName, DatabaseName, Port)
go

alter table __ShardManagement.ShardMappingsGlobal
	add constraint ucShardMappingsGlobal_MappingId unique (MappingId)
go

alter table __ShardManagement.ShardsGlobal
	add constraint fkShardsGlobal_ShardMapId foreign key (ShardMapId) references __ShardManagement.ShardMapsGlobal(ShardMapId)
go

alter table __ShardManagement.ShardMappingsGlobal
	add constraint fkShardMappingsGlobal_ShardMapId foreign key (ShardMapId) references __ShardManagement.ShardMapsGlobal(ShardMapId)
go

alter table __ShardManagement.ShardMappingsGlobal
	add constraint fkShardMappingsGlobal_ShardId foreign key (ShardId) references __ShardManagement.ShardsGlobal(ShardId)
go

---------------------------------------------------------------------------------------------------
-- Data
---------------------------------------------------------------------------------------------------
insert into 
	__ShardManagement.ShardMapManagerGlobal (StoreVersion)
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
-- __ShardManagement.fnGetStoreVersionGlobal
---------------------------------------------------------------------------------------------------
create function __ShardManagement.fnGetStoreVersionGlobal()
returns int
as
begin
	return (select StoreVersion from __ShardManagement.ShardMapManagerGlobal)
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetStoreVersionGlobalHelper
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetStoreVersionGlobalHelper
as
begin
	select
		5, StoreVersion
	from 
		__ShardManagement.ShardMapManagerGlobal
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetOperationLogEntryGlobalHelper
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetOperationLogEntryGlobalHelper
@operationId uniqueidentifier
as
begin
	select
		6, OperationId, OperationCode, Data, UndoStartState, ShardVersionRemoves, ShardVersionAdds
	from
		__ShardManagement.OperationsLogGlobal
	where
		OperationId = @operationId
end
go

---------------------------------------------------------------------------------------------------
-- Stored Procedures
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- Operations
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spFindAndUpdateOperationLogEntryByIdGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@operationId uniqueidentifier,
			@undoStartState int

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@undoStartState = x.value('(@UndoStartState)[1]', 'int')
	from
		@input.nodes('/FindAndUpdateOperationLogEntryByIdGlobal') as t(x)

	if (@gsmVersionClient is null or @operationId is null or @undoStartState is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	update 
		__ShardManagement.OperationsLogGlobal
	set
		UndoStartState = @undoStartState
	where
		OperationId = @operationId

	set @result = 1
	exec __ShardManagement.spGetOperationLogEntryGlobalHelper @operationId
	goto Exit_Procedure;
	
Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- Shard Maps
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardMapsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetAllShardMapsGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int

	select 
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int')
	from 
		@input.nodes('/GetAllShardMapsGlobal') as t(x)

	if (@gsmVersionClient is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	select 
		1, ShardMapId, Name, ShardMapType, KeyType 
	from 
		__ShardManagement.ShardMapsGlobal

	set @result = 1
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardMapByNameGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spFindShardMapByNameGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@name  nvarchar(50)

	select 
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@name = x.value('(ShardMap/Name)[1]', ' nvarchar(50)')
	from 
		@input.nodes('/FindShardMapByNameGlobal') as t(x)

	if (@gsmVersionClient is null or @name is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	select
		1, ShardMapId, Name, ShardMapType, KeyType
	from
		__ShardManagement.ShardMapsGlobal
	where 
		Name = @name

	set @result = 1
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllDistinctShardLocationsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetAllDistinctShardLocationsGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int

	select 
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int')
	from 
		@input.nodes('/GetAllDistinctShardLocationsGlobal') as t(x)

	if (@gsmVersionClient is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	select distinct 
		4, Protocol, ServerName, Port, DatabaseName 
	from 
		__ShardManagement.ShardsGlobal
	where
		Readable = 1

	set @result = 1
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
	end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spAddShardMapGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spAddShardMapGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier,
			@name nvarchar(50),
			@mapType int,
			@keyType int

	select 
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@name = x.value('(ShardMap/Name)[1]', 'nvarchar(50)'),
		@mapType = x.value('(ShardMap/Kind)[1]', 'int'),
		@keyType = x.value('(ShardMap/KeyKind)[1]', 'int')
	from 
		@input.nodes('/AddShardMapGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null or @name is null or @mapType is null or @keyType is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;
		
	-- try to insert the row for the shard map, duplicate violation will be detected by the
	-- uniqueness constraint on the Name
	begin try
		insert into 
			__ShardManagement.ShardMapsGlobal 
			(ShardMapId, Name, ShardMapType, KeyType)
		values 
			(@shardMapId, @name, @mapType, @keyType) 
	end try
	begin catch
		if (error_number() = 2627)
			goto Error_ShardMapAlreadyExists;
		else
		begin
			declare @errorMessage nvarchar(max) = error_message(),
					@errorNumber int = error_number(),
					@errorSeverity int = error_severity(),
					@errorState int = error_state(),
					@errorLine int = error_line(),
					@errorProcedure nvarchar(128) = isnull(error_procedure(), '-');

			select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;
			
			raiserror (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
			
			rollback transaction; -- To avoid extra error message in response.
			goto Error_UnexpectedError;
        end
	end catch
		
	set @result = 1
	goto Exit_Procedure;
		
Error_ShardMapAlreadyExists:
	set @result = 101
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_UnexpectedError:
	set @result = 53
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spRemoveShardMapGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spRemoveShardMapGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier

	select 
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', ' uniqueidentifier')
	from 
		@input.nodes('/RemoveShardMapGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	declare @currentShardMapId uniqueidentifier

	select 
		@currentShardMapId = ShardMapId
	from
		__ShardManagement.ShardMapsGlobal with (updlock)
	where
		ShardMapId = @shardMapId

	if (@currentShardMapId is null)
		goto Error_ShardMapNotFound;

	if exists (
		select 
			ShardId 
		from 
			__ShardManagement.ShardsGlobal 
		where 
			ShardMapId = @shardMapId)
		goto Error_ShardMapHasShards;

	delete from 
		__ShardManagement.ShardMapsGlobal 
	where 
		ShardMapId = @shardMapId 

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_ShardMapHasShards:
	set @result = 103
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- Shards
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetAllShardsGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier

	select 
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
	from 
		@input.nodes('/GetAllShardsGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	if not exists (
		select 
			ShardMapId 
		from 
			__ShardManagement.ShardMapsGlobal 
		where 
			ShardMapId = @shardMapId)
		goto Error_ShardMapNotFound;

	select 
		2, ShardId, Version, ShardMapId, Protocol, ServerName, Port, DatabaseName, Status
	from 
		__ShardManagement.ShardsGlobal 
	where 
		ShardMapId = @shardMapId and Readable = 1

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardByLocationGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spFindShardByLocationGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier,
			@protocol int,
			@serverName nvarchar(128),
			@port int,
			@databaseName nvarchar(128)

	select 
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@protocol = x.value('(Location/Protocol)[1]', 'int'),
		@serverName = x.value('(Location/ServerName)[1]', 'nvarchar(128)'),
		@port = x.value('(Location/Port)[1]', 'int'),
		@databaseName = x.value('(Location/DatabaseName)[1]', 'nvarchar(128)')
	from
		@input.nodes('/FindShardByLocationGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null or 
		@protocol is null or @serverName is null or @port is null or @databaseName is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	if not exists (
	select 
		ShardMapId 
	from
		__ShardManagement.ShardMapsGlobal
	where
		ShardMapId = @shardMapId)
		goto Error_ShardMapNotFound;

	select 
		2, ShardId, Version, ShardMapId, Protocol, ServerName, Port, DatabaseName, Status
	from 
		__ShardManagement.ShardsGlobal 
	where
		ShardMapId = @shardMapId and
		Protocol = @protocol and ServerName = @serverName and Port = @port and DatabaseName = @databaseName and 
		Readable = 1

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spBulkOperationShardsGlobalBegin
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spBulkOperationShardsGlobalBegin
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@operationId uniqueidentifier,
			@operationCode int,
			@stepsCount int,
			@shardMapId uniqueidentifier

	-- get operation information as well as number of steps
	select 
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@operationCode = x.value('(@OperationCode)[1]', 'int'),
		@stepsCount = x.value('(@StepsCount)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
	from
		@input.nodes('/BulkOperationShardsGlobal') as t(x)

	if (@gsmVersionClient is null or @operationId is null or @operationCode is null or 
		@stepsCount is null or @shardMapId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	-- check if shard map exists
	if not exists (
		select 
			ShardMapId 
		from 
			__ShardManagement.ShardMapsGlobal with (updlock)
		where
			ShardMapId = @shardMapId)
		goto Error_ShardMapNotFound;

	-- add log record
	begin try
		insert into __ShardManagement.OperationsLogGlobal(
			OperationId,
			OperationCode,
			Data,
			ShardVersionRemoves,
			ShardVersionAdds)
		values (
			@operationId,
			@operationCode,
			@input,
			null,
			null)
	end try
	begin catch
		-- if log record already exists, ignore
		if (error_number() <> 2627)
		begin
			declare @errorMessage nvarchar(max) = error_message(),
					@errorNumber int = error_number(),
					@errorSeverity int = error_severity(),
					@errorState int = error_state(),
					@errorLine int = error_line(),
					@errorProcedure nvarchar(128) = isnull(error_procedure(), '-');

			select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;
			
			raiserror (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
			
			rollback transaction; -- To avoid extra error message in response.
			goto Error_UnexpectedError;
        end
	end catch

	-- Remove/Update/Add specific
	declare @currentStep xml,
			@stepIndex int = 1,
			@stepType int,
			@stepShardId uniqueidentifier,
			@stepShardVersion uniqueidentifier,
			@currentShardVersion uniqueidentifier,
			@currentShardOperationId uniqueidentifier

	-- Add specific
	declare	@stepProtocol int,
			@stepServerName nvarchar(128),
			@stepPort int,
			@stepDatabaseName nvarchar(128),
			@stepShardStatus int

	while (@stepIndex <= @stepsCount)
	begin
		select 
			@currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]') 
		from
			@input.nodes('/BulkOperationShardsGlobal/Steps') as t(x)

		-- Identify the step type.
		select 
			@stepType = x.value('(@Kind)[1]', 'int'),
			@stepShardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
			@stepShardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier')
		from 
			@currentStep.nodes('./Step') as t(x)

		if (@stepType is null or @stepShardId is null or @stepShardVersion is null)
			goto Error_MissingParameters;

		if (@stepType = 1 or @stepType = 2)
		begin
			-- Remove/Update Shard

			-- Check re-entrancy or pending operations
			select
				@currentShardVersion = Version,
				@currentShardOperationId = OperationId
			from
				__ShardManagement.ShardsGlobal with (updlock)
			where
				ShardMapId = @shardMapId and ShardId = @stepShardId and Readable = 1

			-- re-entrancy
			if (@currentShardOperationId = @operationId)
				goto Success_Exit;

			-- pending operation
			if (@currentShardOperationId is not null)
				goto Error_ShardPendingOperation;

			if (@currentShardVersion is null)
				goto Error_ShardDoesNotExist;

			if (@currentShardVersion <> @stepShardVersion)
				goto Error_ShardVersionMismatch;

			-- check if mappings exist for the shard being deleted
			if (@stepType = 1)
			begin
			if exists (
				select 
					ShardId 
				from 
					__ShardManagement.ShardMappingsGlobal 
				where 
					ShardMapId = @shardMapId and ShardId = @stepShardId)
				goto Error_ShardHasMappings;
			end

			-- mark pending operation on current shard
			update 
				__ShardManagement.ShardsGlobal
			set
				OperationId = @operationId
			where
				ShardMapId = @shardMapId and ShardId = @stepShardId
		end
		else
		if (@stepType = 3)
		begin
			-- Add Shard

			-- read the information for add only
			select
				@stepProtocol = x.value('(Shard/Location/Protocol)[1]', 'int'),
				@stepServerName = x.value('(Shard/Location/ServerName)[1]', 'nvarchar(128)'),
				@stepPort = x.value('(Shard/Location/Port)[1]', 'int'),
				@stepDatabaseName = x.value('(Shard/Location/DatabaseName)[1]', 'nvarchar(128)'),
				@stepShardStatus = x.value('(Shard/Status)[1]', 'int')
			from
				@currentStep.nodes('./Step') as t(x)

			if (@stepProtocol is null or @stepServerName is null or @stepPort is null or @stepDatabaseName is null or @stepShardStatus is null)
				goto Error_MissingParameters;

			-- Check re-entrancy or pending operations
			select 
				@currentShardVersion = Version,
				@currentShardOperationId = OperationId
			from
				__ShardManagement.ShardsGlobal with (updlock)
			where
				ShardMapId = @shardMapId and ShardId = @stepShardId

			-- re-entrancy
			if (@currentShardOperationId = @operationId)
				goto Success_Exit;

			-- pending operation
			if (@currentShardOperationId is not null)
				goto Error_ShardPendingOperation;
	
			if (@currentShardVersion is not null)
				goto Error_ShardAlreadyExists;

			-- check for duplicate locations for add shard
			set @currentShardVersion = null
			set @currentShardOperationId = null

			select 
				@currentShardVersion = Version, 
				@currentShardOperationId = OperationId
			from  
				__ShardManagement.ShardsGlobal 
			where
				ShardMapId = @shardMapId and
				Protocol = @stepProtocol and 
				ServerName = @stepServerName and
				Port = @stepPort and
				DatabaseName = @stepDatabaseName

			-- Previous pending operation also had the same shard location
			-- We need to reconcile the previous operation first.
			if (@currentShardOperationId is not null)
				goto Error_ShardPendingOperation;

			-- Another shard with same location already exists.		
			if (@currentShardVersion is not null)
				goto Error_ShardLocationAlreadyExists;

			-- perform the add/update
			begin try
				insert into 
					__ShardManagement.ShardsGlobal(
					ShardId, 
					Readable, 
					Version, 
					ShardMapId, 
					OperationId, 
					Protocol, 
					ServerName, 
					Port, 
					DatabaseName, 
					Status)
				values (
					@stepShardId, 
					0,
					@stepShardVersion, 
					@shardMapId,
					@operationId, 
					@stepProtocol, 
					@stepServerName, 
					@stepPort, 
					@stepDatabaseName, 
					@stepShardStatus) 
			end try
			begin catch
				if (error_number() = 2627)
					goto Error_ShardLocationAlreadyExists;
				else
				begin
					set @errorMessage = error_message()
					set	@errorNumber = error_number()
					set @errorSeverity = error_severity()
					set @errorState = error_state()
					set @errorLine = error_line()
					set @errorProcedure = isnull(error_procedure(), '-')

					select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;
			
					raiserror (@errorMessage, @errorSeverity, 2, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
			
					rollback transaction; -- To avoid extra error message in response.
					goto Error_UnexpectedError;
				end
			end catch

			-- reset state for next iteration
			set @stepProtocol = null
			set @stepServerName = null
			set @stepPort = null
			set @stepDatabaseName = null
			set @stepShardStatus = null
		end

		-- reset state for next iteration
		set @stepType = null
		set @stepShardId = null
		set @stepShardVersion = null
		set @currentShardVersion = null
		set @currentShardOperationId = null

		set @stepIndex = @stepIndex + 1
	end

	goto Success_Exit;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_ShardAlreadyExists:
	set @result = 201
	goto Exit_Procedure;

Error_ShardDoesNotExist:
	set @result = 202
	goto Exit_Procedure;

Error_ShardHasMappings:
	set @result = 203
	goto Exit_Procedure;

Error_ShardVersionMismatch:
	set @result = 204
	goto Exit_Procedure;

Error_ShardLocationAlreadyExists:
	set @result = 205
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_ShardPendingOperation:
	set @result = 52
	exec __ShardManagement.spGetOperationLogEntryGlobalHelper @currentShardOperationId
	goto Exit_Procedure;

Error_UnexpectedError:
	set @result = 53
	goto Exit_Procedure;

Success_Exit:
	set @result = 1
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spBulkOperationShardsGlobalEnd
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spBulkOperationShardsGlobalEnd
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@operationId uniqueidentifier,
			@operationCode int,
			@undo bit,
			@stepsCount int,
			@shardMapId uniqueidentifier

	-- get operation information as well as number of steps
	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@operationCode = x.value('(@OperationCode)[1]', 'int'),
		@undo = x.value('(@Undo)[1]', 'bit'),
		@stepsCount = x.value('(@StepsCount)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
	from
		@input.nodes('/BulkOperationShardsGlobal') as t(x)

	if (@gsmVersionClient is null or @operationId is null or @operationCode is null or @undo is null or
		@stepsCount is null or @shardMapId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	-- check if shard map exists
	if not exists (
		select 
			ShardMapId 
		from 
			__ShardManagement.ShardMapsGlobal with (updlock)
		where
			ShardMapId = @shardMapId)
		goto Error_ShardMapNotFound;

	-- Remove/Update/Add specific
	declare @currentStep xml,
			@stepIndex int = 1,
			@stepType int,
			@stepShardId uniqueidentifier
	
	while (@stepIndex <= @stepsCount)
	begin
		select 
			@currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]') 
		from
			@input.nodes('/BulkOperationShardsGlobal/Steps') as t(x)

		-- Identify the step type.
		select 
			@stepType = x.value('(@Kind)[1]', 'int'),
			@stepShardId = x.value('(Shard/Id)[1]', 'uniqueidentifier')
		from 
			@currentStep.nodes('./Step') as t(x)

		if (@stepType is null or @stepShardId is null)
			goto Error_MissingParameters;

		if (@stepType = 1)
		begin
			if (@undo = 1)
			begin
				-- keep the Readable row as is
				update 
					__ShardManagement.ShardsGlobal
				set
					OperationId = null
				where
					ShardMapId = @shardMapId and ShardId = @stepShardId and OperationId = @operationId 
			end
			else
			begin
				-- remove the row to be deleted
				delete from 
					__ShardManagement.ShardsGlobal
				where
					ShardMapId = @shardMapId and ShardId = @stepShardId and OperationId = @operationId
			end
		end
		else
		if (@stepType = 2)
		begin
			declare @newShardVersion uniqueidentifier,
					@newStatus int

			if (@undo = 1)
			begin
				-- keep the Readable row as is
				update 
					__ShardManagement.ShardsGlobal
				set
					OperationId = null
				where
					ShardMapId = @shardMapId and ShardId = @stepShardId and OperationId = @operationId 
			end
			else
			begin
				-- Update the row with new Version/Status information
				select 
					@newShardVersion = x.value('(Update/Shard/Version)[1]', 'uniqueidentifier'),
					@newStatus = x.value('(Update/Shard/Status)[1]', 'int')
				from 
					@currentStep.nodes('./Step') as t(x)

				update 
					__ShardManagement.ShardsGlobal
				set
					Version = @newShardVersion,
					Status = @newStatus,
					OperationId = null
				where
					ShardMapId = @shardMapId and ShardId = @stepShardId and OperationId = @operationId 
			end

			set @newShardVersion = null
			set @newStatus = null
		end
		else
		if (@stepType = 3)
		begin
			if (@undo = 1)
			begin
				-- remove the row that we tried to add
				delete from 
					__ShardManagement.ShardsGlobal
				where
					ShardMapId = @shardMapId and ShardId = @stepShardId and OperationId = @operationId
			end
			else
			begin
				-- mark the new row Readable
				update 
					__ShardManagement.ShardsGlobal
				set
					Readable = 1,
					OperationId = null
				where
					ShardMapId = @shardMapId and ShardId = @stepShardId and OperationId = @operationId 
			end
		end

		-- reset state for next iteration
		set @stepShardId = null

		set @stepIndex = @stepIndex + 1
	end

	-- remove log record
	delete from
		__ShardManagement.OperationsLogGlobal
	where 
		OperationId = @operationId

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- Shard Mappings
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardMappingsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetAllShardMappingsGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier,
			@shardId uniqueidentifier,
			@shardVersion uniqueidentifier,
			@minValue varbinary(128),
			@maxValue varbinary(128)

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@shardId = x.value('(Shard[@Null="0"]/Id)[1]', 'uniqueidentifier'),
		@shardVersion = x.value('(Shard[@Null="0"]/Version)[1]', 'uniqueidentifier'),
		@minValue = convert(varbinary(128), x.value('(Range[@Null="0"]/MinValue)[1]', 'varchar(258)'), 1),
		@maxValue = convert(varbinary(128), x.value('(Range[@Null="0"]/MaxValue[@Null="0"])[1]', 'varchar(258)'), 1)
	from
		@input.nodes('/GetAllShardMappingsGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	declare @shardMapType int

	select 
		@shardMapType = ShardMapType
	from
		__ShardManagement.ShardMapsGlobal
	where
		ShardMapId = @shardMapId

	if (@shardMapType is null)
		goto Error_ShardMapNotFound;

	declare @currentShardVersion uniqueidentifier

	if (@shardId is not null)
	begin
		if (@shardVersion is null)
			goto Error_MissingParameters;
			
		select 
			@currentShardVersion = Version
		from
			__ShardManagement.ShardsGlobal
		where
			ShardMapId = @shardMapId and ShardId = @shardId and Readable = 1

		if (@currentShardVersion is null)
			goto Error_ShardDoesNotExist;

		-- DEVNOTE(wbasheer): Bring this back if we want to be strict.		
		--if (@currentShardVersion <> @shardVersion)
		--	goto Error_ShardVersionMismatch;
	end
			
	declare @tvShards table (
		ShardId uniqueidentifier not null, 
		Version uniqueidentifier not null, 
		Protocol int not null,
		ServerName nvarchar(128) collate SQL_Latin1_General_CP1_CI_AS not null, 
		Port int not null,
		DatabaseName nvarchar(128) collate SQL_Latin1_General_CP1_CI_AS not null, 
		Status int not null,
		primary key (ShardId)
	)

	insert into
		@tvShards
	select
		ShardId = s.ShardId,
		Version = s.Version,
		Protocol = s.Protocol,
		ServerName = s.ServerName,
		Port = s.Port,
		DatabaseName = s.DatabaseName,
		Status = s.Status
	from
		__ShardManagement.ShardsGlobal s
	where
		(@shardId is null or s.ShardId = @shardId) and s.ShardMapId = @shardMapId
		

	declare @minValueCalculated varbinary(128) = 0x,
			@maxValueCalculated varbinary(128) = null

	-- check if range is supplied and update accordingly.
	if (@minValue is not null)
		set @minValueCalculated = @minValue

	if (@maxValue is not null)
		set @maxValueCalculated = @maxValue

	if (@shardMapType = 1)
	begin
		select
			3, m.MappingId, m.ShardMapId, m.MinValue, m.MaxValue, m.Status, m.LockOwnerId,  -- fields for SqlMapping
			s.ShardId, s.Version, m.ShardMapId, s.Protocol, s.ServerName, s.Port, s.DatabaseName, s.Status -- fields for SqlShard, ShardMapId is repeated here
		from
			__ShardManagement.ShardMappingsGlobal m
		join 
			@tvShards s 
		on 
			m.ShardId = s.ShardId
		where
			m.ShardMapId = @shardMapId and 
			m.Readable = 1 and
			(@shardId is null or m.ShardId = @shardId) and 
			MinValue >= @minValueCalculated and 
			((@maxValueCalculated is null) or (MinValue < @maxValueCalculated))
		order by 
			m.MinValue
	end
	else
	begin
		select
			3, m.MappingId, m.ShardMapId, m.MinValue, m.MaxValue, m.Status, m.LockOwnerId,  -- fields for SqlMapping
			s.ShardId, s.Version, m.ShardMapId, s.Protocol, s.ServerName, s.Port, s.DatabaseName, s.Status -- fields for SqlShard, ShardMapId is repeated here
		from
			__ShardManagement.ShardMappingsGlobal m
		join 
			@tvShards s 
		on 
			m.ShardId = s.ShardId
		where
			m.ShardMapId = @shardMapId and 
			m.Readable = 1 and
			(@shardId is null or m.ShardId = @shardId) and 
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

Error_ShardDoesNotExist:
	set @result = 202
	goto Exit_Procedure;

-- DEVNOTE(wbasheer): Bring this back if we want to be strict.		
--Error_ShardVersionMismatch:
--	set @result = 204
--	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardMappingByKeyGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spFindShardMappingByKeyGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier,
			@keyValue varbinary(128)

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@keyValue = convert(varbinary(128), x.value('(Key/Value)[1]', 'varchar(258)'), 1)
	from
		@input.nodes('/FindShardMappingByKeyGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null or @keyValue is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	declare @shardMapType int

	select 
		@shardMapType = ShardMapType
	from
		__ShardManagement.ShardMapsGlobal
	where
		ShardMapId = @shardMapId

	if (@shardMapType is null)
		goto Error_ShardMapNotFound;
		
	declare @currentMappingId uniqueidentifier,
			@currentShardId uniqueidentifier,
			@currentMinValue varbinary(128),
			@currentMaxValue varbinary(128),
			@currentStatus int,
			@currentLockOwnerId uniqueidentifier

	if (@shardMapType = 1)
	begin	
		select
			@currentMappingId = MappingId,
			@currentShardId = ShardId,
			@currentMinValue = MinValue,
			@currentMaxValue = MaxValue,
			@currentStatus = Status,
			@currentLockOwnerId = LockOwnerId
		from
			__ShardManagement.ShardMappingsGlobal
		where
			ShardMapId = @shardMapId and 
			Readable = 1 and
			MinValue = @keyValue
	end
	else
	begin
		select 
			@currentMappingId = MappingId,
			@currentShardId = ShardId,
			@currentMinValue = MinValue,
			@currentMaxValue = MaxValue,
			@currentStatus = Status,
			@currentLockOwnerId = LockOwnerId
		from 
			__ShardManagement.ShardMappingsGlobal
		where
			ShardMapId = @shardMapId and 
			Readable = 1 and
			MinValue <= @keyValue and (MaxValue is null or MaxValue > @keyValue)
	end

	if (@@rowcount = 0)
		goto Error_KeyNotFound;

	select 
		3, @currentMappingId as MappingId, ShardMapId, @currentMinValue, @currentMaxValue, @currentStatus, @currentLockOwnerId, -- fields for SqlMapping
		ShardId, Version, ShardMapId, Protocol, ServerName, Port, DatabaseName, Status -- fields for SqlShard, ShardMapId is repeated here
	from 
		__ShardManagement.ShardsGlobal
	where
		ShardId = @currentShardId and
		ShardMapId = @shardMapId
	
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
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardMappingByIdGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spFindShardMappingByIdGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier,
			@mappingId uniqueidentifier

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@mappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier')
	from
		@input.nodes('/FindShardMappingByIdGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null or @mappingId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	declare @shardMapType int

	select 
		@shardMapType = ShardMapType
	from
		__ShardManagement.ShardMapsGlobal
	where
		ShardMapId = @shardMapId

	if (@shardMapType is null)
		goto Error_ShardMapNotFound;
		
	declare @currentShardId uniqueidentifier,
			@currentMinValue varbinary(128),
			@currentMaxValue varbinary(128),
			@currentStatus int,
			@currentLockOwnerId uniqueidentifier

	-- select just MinValue so only 'ucShardMappingsGlobal_MappingId' will be used
	select
		@currentMinValue = MinValue
	from
		__ShardManagement.ShardMappingsGlobal
	where
		ShardMapId = @shardMapId and 
		Readable = 1 and
		MappingId = @mappingId

	if (@@rowcount = 0)
		goto Error_MappingDoesNotExist;

	-- now filter using MinValue to use 'pk_tblShardMappingsGlobal_smid_minvalue'
	select
		@currentShardId = ShardId,
		@currentMaxValue = MaxValue,
		@currentStatus = Status,
		@currentLockOwnerId = LockOwnerId
	from
		__ShardManagement.ShardMappingsGlobal
	where
		ShardMapId = @shardMapId and 
		MinValue = @currentMinValue

	if (@@rowcount = 0)
		goto Error_MappingDoesNotExist;

	select
		3, @mappingId as MappingId, ShardMapId, @currentMinValue, @currentMaxValue, @currentStatus, @currentLockOwnerId, -- fields for SqlMapping
		ShardId, Version, ShardMapId, Protocol, ServerName, Port, DatabaseName, Status -- fields for SqlShard, ShardMapId is repeated here
	from
		__ShardManagement.ShardsGlobal
	where
		ShardId = @currentShardId and
		ShardMapId = @shardMapId

	if (@@rowcount = 0)
		goto Error_MappingDoesNotExist;

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MappingDoesNotExist:
	set @result = 301
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spBulkOperationShardMappingsGlobalBegin
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spBulkOperationShardMappingsGlobalBegin
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@operationId uniqueidentifier,
			@operationCode int,
			@stepsCount int,
			@shardMapId uniqueidentifier

	-- get operation information as well as number of steps information
	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@operationCode = x.value('(@OperationCode)[1]', 'int'),
		@stepsCount = x.value('(@StepsCount)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
	from
		@input.nodes('/BulkOperationShardMappingsGlobal') as t(x)

	if (@gsmVersionClient is null or @operationId is null or @operationCode is null or 
		@stepsCount is null or @shardMapId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	-- check if shard map exists
	declare @shardMapType int

	select 
		@shardMapType = ShardMapType
	from
		__ShardManagement.ShardMapsGlobal with (updlock)
	where
		ShardMapId = @shardMapId

	if (@shardMapType is null)
		goto Error_ShardMapNotFound;

	declare @shardIdForRemoves uniqueidentifier,
			@originalShardVersionForRemoves uniqueidentifier,
			@shardIdForAdds uniqueidentifier,
			@originalShardVersionForAdds uniqueidentifier,
			@currentShardOperationId uniqueidentifier

	select 
		@shardIdForRemoves = x.value('(Removes/Shard/Id)[1]', 'uniqueidentifier'),
		@shardIdForAdds = x.value('(Adds/Shard/Id)[1]', 'uniqueidentifier')
	from 
		@input.nodes('/BulkOperationShardMappingsGlobal') as t(x)

	if (@shardIdForRemoves is null or @shardIdForAdds is null)
		goto Error_MissingParameters;

	-- Check re-entrancy or pending operations
	select
		@originalShardVersionForRemoves = Version,
		@currentShardOperationId = OperationId
	from
		__ShardManagement.ShardsGlobal with (updlock)
	where
		ShardMapId = @shardMapId and ShardId = @shardIdForRemoves and Readable = 1
	
	-- re-entrancy
	if (@currentShardOperationId = @operationId)
		goto Success_Exit;

	-- pending operations
	if (@currentShardOperationId is not null)
		goto Error_ShardPendingOperation;

	if (@originalShardVersionForRemoves is null)
		goto Error_ShardDoesNotExist;

	-- mark the source shard for update
	update __ShardManagement.ShardsGlobal
	set
		OperationId = @operationId
	where
		ShardMapId = @shardMapId and ShardId = @shardIdForRemoves

	set @currentShardOperationId = null;

	if (@shardIdForRemoves <> @shardIdForAdds)
	begin
		-- Check re-entrancy or pending operations
		select
			@originalShardVersionForAdds = Version,
			@currentShardOperationId = OperationId
		from
			__ShardManagement.ShardsGlobal with (updlock)
		where
			ShardMapId = @shardMapId and ShardId = @shardIdForAdds and Readable = 1
	
		-- re-entrancy
		if (@currentShardOperationId = @operationId)
			goto Success_Exit;

		-- pending operations
		if (@currentShardOperationId is not null)
			goto Error_ShardPendingOperation;

		if (@originalShardVersionForAdds is null)
			goto Error_ShardDoesNotExist;

		-- mark the target shard for update
		update __ShardManagement.ShardsGlobal
		set
			OperationId = @operationId
		where
			ShardMapId = @shardMapId and ShardId = @shardIdForAdds
	end
	else
	begin
		set @originalShardVersionForAdds = @originalShardVersionForRemoves
	end
	
	-- add log record
	begin try
		insert into __ShardManagement.OperationsLogGlobal(
			OperationId,
			OperationCode,
			Data,
			ShardVersionRemoves,
			ShardVersionAdds)
		values (
			@operationId,
			@operationCode,
			@input,
			@originalShardVersionForRemoves,
			@originalShardVersionForAdds)
	end try
	begin catch
		-- if log record already exists, ignore
		if (error_number() <> 2627)
		begin
			declare @errorMessage nvarchar(max) = error_message(),
					@errorNumber int = error_number(),
					@errorSeverity int = error_severity(),
					@errorState int = error_state(),
					@errorLine int = error_line(),
					@errorProcedure nvarchar(128) = isnull(error_procedure(), '-');

			select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;
			
			raiserror (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
			
			rollback transaction; -- To avoid extra error message in response.
			goto Error_UnexpectedError;
        end
	end catch

	declare	@currentStep xml,
			@stepIndex int = 1,
			@stepType int,
			@stepMappingId uniqueidentifier,
			@stepLockOwnerId uniqueidentifier

	-- Remove/Update
	declare	@currentLockOwnerId uniqueidentifier,
			@currentStatus int

	-- Update/Add
	declare @stepStatus int,
			@stepShouldValidate bit,
			@stepMinValue varbinary(128),
			@stepMaxValue varbinary(128),
			@mappingIdFromValidate uniqueidentifier

	while (@stepIndex <= @stepsCount)
	begin
		select 
			@currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]') 
		from 
			@input.nodes('/BulkOperationShardMappingsGlobal/Steps') as t(x)

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

			-- Check for locks
			select 
				@stepLockOwnerId = x.value('(Lock/Id)[1]', 'uniqueidentifier')
			from 
				@currentStep.nodes('./Step') as t(x)

			if (@stepLockOwnerId is null)
				goto Error_MissingParameters;

			select 
				@currentLockOwnerId = LockOwnerId,
				@currentStatus = Status
			from
				__ShardManagement.ShardMappingsGlobal with (updlock)
			where
				ShardMapId = @shardMapId and MappingId = @stepMappingId and Readable = 1

			if (@currentLockOwnerId is null)	
				goto Error_MappingDoesNotExist;

			if (@currentLockOwnerId <> @stepLockOwnerId)
				goto Error_MappingLockOwnerIdMismatch;

			-- removepoint/removerange/removerangefromrange cannot work on online mappings
			if ((@currentStatus & 1) <> 0 and 
			    (@operationCode = 5 or 
				 @operationCode = 9 or 
				 @operationCode = 13))
				goto Error_MappingIsNotOffline;

			-- mark pending operation on current mapping
			update 
				__ShardManagement.ShardMappingsGlobal
			set
				OperationId = @operationId
			where
				ShardMapId = @shardMapId and MappingId = @stepMappingId

			-- reset state for next iteration
			set @currentLockOwnerId = null
			set @currentStatus = null
		end
		else
		if (@stepType = 2)
		begin
			-- UpdateMapping

			-- Check for locks
			select 
				@stepLockOwnerId = x.value('(Lock/Id)[1]', 'uniqueidentifier'),
				@stepStatus = x.value('(Update/Mapping/Status)[1]', 'int')
			from 
				@currentStep.nodes('./Step') as t(x)

			if (@stepLockOwnerId is null or @stepStatus is null)
				goto Error_MissingParameters;

			select
				@currentLockOwnerId = LockOwnerId,
				@currentStatus = Status
			from
				__ShardManagement.ShardMappingsGlobal with (updlock)
			where
				ShardMapId = @shardMapId and MappingId = @stepMappingId and Readable = 1

			if (@currentLockOwnerId is null)	
				goto Error_MappingDoesNotExist;

			if (@currentLockOwnerId <> @stepLockOwnerId)
				goto Error_MappingLockOwnerIdMismatch;

			-- online -> online and location change is not allowed
			if ((@currentStatus & 1) = 1 and (@stepStatus & 1) = 1 and @shardIdForRemoves <> @shardIdForAdds)
				goto Error_MappingIsNotOffline;

			-- mark pending operation on current mapping
			update 
				__ShardManagement.ShardMappingsGlobal
			set
				OperationId = @operationId
			where
				ShardMapId = @shardMapId and MappingId = @stepMappingId

			-- reset state for next iteration
			set @currentLockOwnerId = null
			set @currentStatus = null

			set @stepStatus = null
		end
		else
		if (@stepType = 3)
		begin
			-- AddMapping
			select 
				@stepShouldValidate = x.value('(@Validate)[1]', 'bit'),
				@stepMappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier'),
				@stepMinValue = convert(varbinary(128), x.value('(Mapping/MinValue)[1]', 'varchar(258)'), 1),
				@stepMaxValue = convert(varbinary(128), x.value('(Mapping/MaxValue[@Null="0"])[1]', 'varchar(258)'), 1),
				@stepStatus = x.value('(Mapping/Status)[1]', 'int'),
				@stepLockOwnerId = x.value('(Mapping/LockOwnerId)[1]', 'uniqueidentifier')
			from
				@currentStep.nodes('./Step') as t(x)

			if (@stepShouldValidate is null or @stepMappingId is null or @stepMinValue is null or @stepStatus is null or @stepLockOwnerId is null)
				goto Error_MissingParameters;

			-- if validation requested
			if (@stepShouldValidate = 1)
			begin
				if (@shardMapType = 1)
				begin
					select 
						@mappingIdFromValidate = MappingId,
						@currentShardOperationId = OperationId
					from
					__ShardManagement.ShardMappingsGlobal
					where
						ShardMapId = @shardMapId and
						MinValue = @stepMinValue

					if (@mappingIdFromValidate is not null)
					begin
						if (@currentShardOperationId is null or @currentShardOperationId = @operationId)
							goto Error_PointAlreadyMapped;
						else
							goto Error_ShardPendingOperation;
					end
				end
				else
				begin
					select 
						@mappingIdFromValidate = MappingId,
						@currentShardOperationId = OperationId
					from
						__ShardManagement.ShardMappingsGlobal
					where
						ShardMapId = @shardMapId and
						(MaxValue is null or MaxValue > @stepMinValue) and 
						(@stepMaxValue is null or MinValue < @stepMaxValue)

					if (@mappingIdFromValidate is not null)
					begin
						if (@currentShardOperationId is null or @currentShardOperationId = @operationId)
							goto Error_RangeAlreadyMapped;
						else
							goto Error_ShardPendingOperation;
					end
				end
			end

			-- add mapping
			insert into
				__ShardManagement.ShardMappingsGlobal(
				MappingId, 
				Readable,
				ShardId, 
				ShardMapId, 
				OperationId, 
				MinValue, 
				MaxValue, 
				Status,
				LockOwnerId)
			values (
				@stepMappingId, 
				0,
				@shardIdForAdds, 
				@shardMapId, 
				@operationId, 
				@stepMinValue, 
				@stepMaxValue, 
				@stepStatus,
				@stepLockOwnerId)

			-- reset state for next iteration
			set @stepStatus = null

			set @stepShouldValidate = null
			set @stepMinValue = null
			set @stepMaxValue = null
			set @mappingIdFromValidate = null
		end

		-- reset state for next iteration
		set @stepType = null
		set @stepMappingId = null
		set @stepLockOwnerId = null

		set @stepIndex = @stepIndex + 1
	end

	goto Success_Exit;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_ShardDoesNotExist:
	set @result = 202
	goto Exit_Procedure;

Error_MappingDoesNotExist:
	set @result = 301
	goto Exit_Procedure;

Error_RangeAlreadyMapped:
	set @result = 302
	goto Exit_Procedure;

Error_PointAlreadyMapped:
	set @result = 303
	goto Exit_Procedure;

Error_MappingLockOwnerIdMismatch:
	set @result = 307
	goto Exit_Procedure;

Error_MappingIsNotOffline:
	set @result = 306
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_ShardPendingOperation:
	set @result = 52
	exec __ShardManagement.spGetOperationLogEntryGlobalHelper @currentShardOperationId
	goto Exit_Procedure;

Error_UnexpectedError:
	set @result = 53
	goto Exit_Procedure;

Success_Exit:
	set @result = 1
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spBulkOperationShardMappingsGlobalEnd
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spBulkOperationShardMappingsGlobalEnd
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@operationId uniqueidentifier,
			@operationCode int,
			@undo int,
			@stepsCount int,
			@shardMapId uniqueidentifier

	-- get operation information as well as number of steps information
	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@operationCode = x.value('(@OperationCode)[1]', 'int'),
		@undo = x.value('(@Undo)[1]', 'int'),
		@stepsCount = x.value('(@StepsCount)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
	from
		@input.nodes('/BulkOperationShardMappingsGlobal') as t(x)

	if (@gsmVersionClient is null or @operationId is null or @operationCode is null or @undo is null or
		@stepsCount is null or @shardMapId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	-- check if shard map exists
	if not exists (
		select 
			ShardMapId 
		from 
			__ShardManagement.ShardMapsGlobal with (updlock)
		where
			ShardMapId = @shardMapId)
		goto Error_ShardMapNotFound;

	declare @shardIdForRemoves uniqueidentifier,
			@shardVersionForRemoves uniqueidentifier,
			@shardIdForAdds uniqueidentifier,
			@shardVersionForAdds uniqueidentifier

	select 
		@shardIdForRemoves = x.value('(Removes/Shard/Id)[1]', 'uniqueidentifier'),
		@shardIdForAdds = x.value('(Adds/Shard/Id)[1]', 'uniqueidentifier'),
		@shardVersionForRemoves = x.value('(Removes/Shard/Version)[1]', 'uniqueidentifier'),
		@shardVersionForAdds = x.value('(Adds/Shard/Version)[1]', 'uniqueidentifier')
	from 
		@input.nodes('/BulkOperationShardMappingsGlobal') as t(x)

	if (@shardIdForRemoves is null or @shardIdForAdds is null or @shardVersionForRemoves is null or @shardVersionForAdds is null)
		goto Error_MissingParameters;

	-- perform shard updates
	if (@undo = 1)
	begin
		-- Unmark the pending operation
		update 
			__ShardManagement.ShardsGlobal
		set
			OperationId = null
		where
			ShardMapId = @shardMapId and ShardId = @shardIdForRemoves

		if (@shardIdForRemoves <> @shardIdForAdds)
		begin
			update 
				__ShardManagement.ShardsGlobal
			set
				OperationId = null
			where
				ShardMapId = @shardMapId and ShardId = @shardIdForAdds
		end
	end
	else
	begin
		-- update the source shard row with new Version
		update 
			__ShardManagement.ShardsGlobal
		set
			Version = @shardVersionForRemoves,
			OperationId = null
		where
			ShardMapId = @shardMapId and ShardId = @shardIdForRemoves

		-- update the target shard row with new Version
		if (@shardIdForRemoves <> @shardIdForAdds)
		begin
		update 
			__ShardManagement.ShardsGlobal
		set
			Version = @shardVersionForAdds,
			OperationId = null
		where
			ShardMapId = @shardMapId and ShardId = @shardIdForAdds
		end
	end

	-- Remove/Update/Add specific
	declare @currentStep xml,
			@stepIndex int = 1,
			@stepType int,
			@stepMappingId uniqueidentifier
	
	while (@stepIndex <= @stepsCount)
	begin
		select 
			@currentStep = x.query('(./Step[@Id = sql:variable("@stepIndex")])[1]') 
		from
		@input.nodes('/BulkOperationShardMappingsGlobal/Steps') as t(x)

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
			if (@undo = 1)
			begin
				-- keep the Readable row as is
				update 
					__ShardManagement.ShardMappingsGlobal
				set
					OperationId = null
				where
					ShardMapId = @shardMapId and MappingId = @stepMappingId
			end
			else
			begin
				-- remove the row to be deleted
				delete from 
					__ShardManagement.ShardMappingsGlobal
				where
					ShardMapId = @shardMapId and MappingId = @stepMappingId
			end
		end
		else
		if (@stepType = 2)
		begin
			declare @newMappingId uniqueidentifier,
					@newMappingStatus int

			if (@undo = 1)
			begin
				-- keep the Readable row as is
				update 
					__ShardManagement.ShardMappingsGlobal
				set
					OperationId = null
				where
					ShardMapId = @shardMapId and MappingId = @stepMappingId
			end
			else
			begin
				-- Update the row with new Version/Status information
				select
					@newMappingId = x.value('(Update/Mapping/Id)[1]', 'uniqueidentifier'),
					@newMappingStatus = x.value('(Update/Mapping/Status)[1]', 'int')
				from
					@currentStep.nodes('./Step') as t(x)

				update 
					__ShardManagement.ShardMappingsGlobal
				set
					MappingId = @newMappingId,
					ShardId = @shardIdForAdds,
					Status = @newMappingStatus,
					OperationId = null
				where
					ShardMapId = @shardMapId and MappingId = @stepMappingId
			end

			set @newMappingId = null
			set @newMappingStatus = null
		end
		else
		if (@stepType = 3)
		begin
			if (@undo = 1)
			begin
				-- remove the row that we tried to add
				delete from 
					__ShardManagement.ShardMappingsGlobal
				where
					ShardMapId = @shardMapId and MappingId = @stepMappingId
			end
			else
			begin
				-- mark the new row Readable
				update 
					__ShardManagement.ShardMappingsGlobal
				set
					Readable = 1,
					OperationId = null
				where
					ShardMapId = @shardMapId and MappingId = @stepMappingId
			end
		end

		-- reset state for next iteration
		set @stepMappingId = null

		set @stepIndex = @stepIndex + 1
	end

	-- delete log record
	delete from 
		__ShardManagement.OperationsLogGlobal
	where
		OperationId = @operationId

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spLockOrUnlockShardMappingsGlobal
-- Constraints:
-- Locks the specified range
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spLockOrUnlockShardMappingsGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier,
			@mappingId uniqueidentifier,
			@lockOwnerId uniqueidentifier,
			@lockOperationType int

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@mappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier'),
		@lockOwnerId = x.value('(Lock/Id)[1]', 'uniqueidentifier'),
		@lockOperationType = x.value('(Lock/Operation)[1]', 'int')
	from
		@input.nodes('/LockOrUnlockShardMappingsGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null or @lockOwnerId is null or @lockOperationType is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	if (@lockOperationType <> 2 and @mappingId is null)
		goto Error_MissingParameters;

	if not exists (
		select 
			ShardMapId 
		from 
			__ShardManagement.ShardMapsGlobal with (updlock)
		where
			ShardMapId = @shardMapId)
		goto Error_ShardMapNotFound;

	declare @DefaultLockOwnerId uniqueidentifier = '00000000-0000-0000-0000-000000000000',
			@currentOperationId uniqueidentifier

	if (@lockOperationType <> 2)
	begin			
		declare @ForceUnLockLockOwnerId uniqueidentifier = 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF',
				@currentLockOwnerId uniqueidentifier

		select 
			@currentOperationId = OperationId,
			@currentLockOwnerId = LockOwnerId
		from 
			__ShardManagement.ShardMappingsGlobal with (updlock)
		where
			ShardMapId = @shardMapId and MappingId = @mappingId

		if (@currentLockOwnerId is null)
			goto Error_MappingDoesNotExist;

		if (@currentOperationId is not null)
			goto Error_ShardPendingOperation;

		if(@lockOperationType = 0 and @currentLockOwnerId <> @DefaultLockOwnerId)
			goto Error_MappingAlreadyLocked;

		if (@lockOperationType = 1 and (@lockOwnerId <> @currentLockOwnerId) and (@lockOwnerId <> @ForceUnLockLockOwnerId))
			goto Error_MappingLockOwnerIdMismatch;
	end

	update
		__ShardManagement.ShardMappingsGlobal
	set 
		LockOwnerId = case 
		when 
			@lockOperationType = 0 
		then 
			@lockOwnerId 
		when  
			@lockOperationType = 1 or @lockOperationType = 2
		then 
			@DefaultLockOwnerId 
		end
		where
			ShardMapId = @shardMapId and (@lockOperationType = 2 or MappingId = @mappingId)

Success_Exit:
	set @result = 1 -- success
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MappingDoesNotExist:
	set @result = 301
	goto Exit_Procedure;

Error_MappingLockOwnerIdMismatch:
	set @result = 307
	goto Exit_Procedure;

Error_MappingAlreadyLocked:
	set @result = 308
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_ShardPendingOperation:
	set @result = 52
	exec __ShardManagement.spGetOperationLogEntryGlobalHelper @currentOperationId
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- Schema Info
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spGetAllShardingSchemaInfosGlobal
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spGetAllShardingSchemaInfosGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int')
	from 
		@input.nodes('/GetAllShardingSchemaInfosGlobal') as t(x)

	if (@gsmVersionClient is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	select
		7, Name, SchemaInfo
	from
		__ShardManagement.ShardedDatabaseSchemaInfosGlobal

	set @result = 1
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spFindShardingSchemaInfoByNameGlobal
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spFindShardingSchemaInfoByNameGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@name nvarchar(128)

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@name = x.value('(SchemaInfo/Name)[1]', 'nvarchar(128)')
	from 
		@input.nodes('/FindShardingSchemaInfoGlobal') as t(x)

	if (@gsmVersionClient is null or @name is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient > __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	select
		7, Name, SchemaInfo
	from 
		__ShardManagement.ShardedDatabaseSchemaInfosGlobal
	where
		Name = @name

	if (@@rowcount = 0)
		goto Error_SchemaInfoNameDoesNotExist;

	set @result = 1
	goto Exit_Procedure;

Error_SchemaInfoNameDoesNotExist:
	set @result = 401
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spAddShardingSchemaInfoGlobal
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spAddShardingSchemaInfoGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@name nvarchar(128),
			@schemaInfo xml

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@name = x.value('(SchemaInfo/Name)[1]', 'nvarchar(128)'),
		@schemaInfo = x.query('SchemaInfo/Info/*')
	from 
		@input.nodes('/AddShardingSchemaInfoGlobal') as t(x)

	if (@gsmVersionClient is null or @name is null or @schemaInfo is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	if exists (
		select 
			Name 
		from 
			__ShardManagement.ShardedDatabaseSchemaInfosGlobal 
		where 
			Name = @name)
		goto Error_SchemaInfoAlreadyExists;
	
	insert into
		__ShardManagement.ShardedDatabaseSchemaInfosGlobal
		(Name, SchemaInfo)
	values
		(@name, @schemaInfo)

	set @result = 1
	goto Exit_Procedure;

Error_SchemaInfoAlreadyExists:
	set @result = 402
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spRemoveShardingSchemaInfoGlobal
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spRemoveShardingSchemaInfoGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@name nvarchar(128)

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@name = x.value('(SchemaInfo/Name)[1]', 'nvarchar(128)')
	from 
		@input.nodes('/RemoveShardingSchemaInfoGlobal') as t(x)

	if (@gsmVersionClient is null or @name is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	delete from
		__ShardManagement.ShardedDatabaseSchemaInfosGlobal
	where
		Name = @name

	if (@@rowcount = 0)
		goto Error_SchemaInfoNameDoesNotExist;

	set @result = 1
	goto Exit_Procedure;

Error_SchemaInfoNameDoesNotExist:
	set @result = 401
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spUpdateShardingSchemaInfoGlobal
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spUpdateShardingSchemaInfoGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@name nvarchar(128),
			@schemaInfo xml

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@name = x.value('(SchemaInfo/Name)[1]', 'nvarchar(128)'),
		@schemaInfo = x.query('SchemaInfo/Info/*')
	from 
		@input.nodes('/UpdateShardingSchemaInfoGlobal') as t(x)

	if (@gsmVersionClient is null or @name is null or @schemaInfo is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	update 
		__ShardManagement.ShardedDatabaseSchemaInfosGlobal 
	set 
		SchemaInfo = @schemaInfo
	where
		Name = @name

	if (@@rowcount = 0)
		goto Error_SchemaInfoNameDoesNotExist;

	set @result = 1
	goto Exit_Procedure;

Error_SchemaInfoNameDoesNotExist:
	set @result = 401
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- Recovery
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- __ShardManagement.spAttachShardGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spAttachShardGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@shardMapId uniqueidentifier,
			@name nvarchar(50),
			@mapType int,
			@keyType int,
			@shardId uniqueidentifier,
			@shardVersion uniqueidentifier,
			@protocol int,
			@serverName nvarchar(128),
			@port int,
			@databaseName nvarchar(128),
			@shardStatus int

	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@name = x.value('(ShardMap/Name)[1]', 'nvarchar(50)'),
		@mapType = x.value('(ShardMap/Kind)[1]', 'int'),
		@keyType = x.value('(ShardMap/KeyKind)[1]', 'int'),

		@shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
		@shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier'),
		@protocol = x.value('(Shard/Location/Protocol)[1]', 'int'),
		@serverName = x.value('(Shard/Location/ServerName)[1]', 'nvarchar(128)'),
		@port = x.value('(Shard/Location/Port)[1]', 'int'),
		@databaseName = x.value('(Shard/Location/DatabaseName)[1]', 'nvarchar(128)'),
		@shardStatus = x.value('(Shard/Status)[1]', 'int')
	from
		@input.nodes('/AttachShardGlobal') as t(x)

	if (@gsmVersionClient is null or @shardMapId is null or @name is null or @mapType is null or @keyType is null or
		@shardId is null or @shardVersion is null or @protocol is null or @serverName is null or 
		@port is null or @databaseName is null or @shardStatus is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	if exists (
	select 
		ShardMapId
	from
		__ShardManagement.ShardMapsGlobal 
	where
		(ShardMapId = @shardMapId and Name <> @name) or (ShardMapId <> @shardMapId and Name = @name))
		goto Error_ShardMapAlreadyExists;

	-- ignore duplicate shard maps
	begin try
		insert into 
			__ShardManagement.ShardMapsGlobal 
			(ShardMapId, Name, ShardMapType, KeyType)
		values 
			(@shardMapId, @name, @mapType, @keyType) 
	end try
	begin catch
		if (error_number() <> 2627)
		begin
			declare @errorMessage nvarchar(max) = error_message(),
					@errorNumber int = error_number(),
					@errorSeverity int = error_severity(),
					@errorState int = error_state(),
					@errorLine int = error_line(),
					@errorProcedure nvarchar(128) = isnull(error_procedure(), '-');

			select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;
			
			raiserror (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
			
			rollback transaction; -- To avoid extra error message in response.
			goto Error_UnexpectedError;
        end
	end catch

	-- attempt to add the shard
	begin try
		insert into 
			__ShardManagement.ShardsGlobal (
			ShardId, 
			Readable, 
			Version, 
			ShardMapId, 
			OperationId, 
			Protocol, 
			ServerName, 
			Port, 
			DatabaseName, 
			Status)
		values (
			@shardId, 
			1, 
			@shardVersion, 
			@shardMapId, 
			null, 
			@protocol, 
			@serverName, 
			@port, 
			@databaseName, 
			@shardStatus) 
	end try
	begin catch
		if (error_number() = 2627)
			goto Error_ShardLocationAlreadyExists;
		else
		begin
			set @errorMessage = error_message()
			set	@errorNumber = error_number()
			set @errorSeverity = error_severity()
			set @errorState = error_state()
			set @errorLine = error_line()
			set @errorProcedure = isnull(error_procedure(), '-')

			select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage;
			
			raiserror (@errorMessage, @errorSeverity, 2, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
			
			rollback transaction; -- To avoid extra error message in response.
			goto Error_UnexpectedError;
		end
	end catch
	
	set @result = 1
	goto Exit_Procedure;

Error_ShardMapAlreadyExists:
	set @result = 101
	goto Exit_Procedure;

Error_ShardLocationAlreadyExists:
	set @result = 205
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_UnexpectedError:
	set @result = 53
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spDetachShardGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spDetachShardGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@protocol int,
			@serverName nvarchar(128),
			@port int,
			@databaseName nvarchar(128),
			@name nvarchar(50)
	
	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@protocol = x.value('(Location/Protocol)[1]', 'int'),
		@serverName = x.value('(Location/ServerName)[1]', 'nvarchar(128)'),
		@port = x.value('(Location/Port)[1]', 'int'),
		@databaseName = x.value('(Location/DatabaseName)[1]', 'nvarchar(128)'),
		@name = x.value('(Shardmap[@Null="0"]/Name)[1]', 'nvarchar(50)')
	from
		@input.nodes('/DetachShardGlobal') as t(x)

	if (@gsmVersionClient is null or @protocol is null or @serverName is null or @port is null or @databaseName is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	declare @tvShardsToDetach table (ShardMapId uniqueidentifier, ShardId uniqueidentifier)

	-- note the detached shards
	insert into 
		@tvShardsToDetach
	select 
		tShardMaps.ShardMapId, tShards.ShardId
	from
		__ShardManagement.ShardMapsGlobal tShardMaps 
		join
		__ShardManagement.ShardsGlobal tShards
		on 
			tShards.ShardMapId = tShardMaps.ShardMapId and 
			tShards.Protocol = @protocol and
			tShards.ServerName = @serverName and 
			tShards.Port = @port and
			tShards.DatabaseName = @databaseName
	where
		@name is null or tShardMaps.Name = @name

	-- remove all mappings
	delete 
		tShardMappings 
	from
		__ShardManagement.ShardMappingsGlobal tShardMappings 
		join
		@tvShardsToDetach tShardsToDetach
		on 
		tShardsToDetach.ShardMapId = tShardMappings.ShardMapId and tShardsToDetach.ShardId = tShardMappings.ShardId

	-- remove all shards
	delete 
		tShards
	from
		__ShardManagement.ShardsGlobal tShards 
		join
		@tvShardsToDetach tShardsToDetach
		on 
		tShardsToDetach.ShardMapId = tShards.ShardMapId and tShardsToDetach.ShardId = tShards.ShardId

	set @result = 1
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spReplaceShardMappingsGlobal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spReplaceShardMappingsGlobal
@input xml,
@result int output
as
begin
	declare @gsmVersionClient int,
			@removeStepsCount int,
			@addStepsCount int,
			@shardMapId uniqueidentifier
	
	-- get operation information as well as number of steps information
	select
		@gsmVersionClient = x.value('(GsmVersion)[1]', 'int'),
		@removeStepsCount = x.value('(@RemoveStepsCount)[1]', 'int'),
		@addStepsCount = x.value('(@AddStepsCount)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier')
	from
		@input.nodes('ReplaceShardMappingsGlobal') as t(x)
	
	if (@gsmVersionClient is null or @removeStepsCount is null or @addStepsCount is null or @shardMapId is null)
		goto Error_MissingParameters;

	if (@gsmVersionClient <> __ShardManagement.fnGetStoreVersionGlobal())
		goto Error_GSMVersionMismatch;

	-- check if shard map exists
	if not exists (
		select 
			ShardMapId 
		from 
			__ShardManagement.ShardMapsGlobal with (updlock)
		where
			ShardMapId = @shardMapId)
		goto Error_ShardMapNotFound;

	declare	@stepShardId uniqueidentifier,
			@stepMappingId uniqueidentifier

	-- read the input for the remove operations
	if (@removeStepsCount > 0)
	begin
		-- read the shard information for removes
		select 
			@stepShardId = x.value('(Shard/Id)[1]', 'uniqueidentifier')
		from 
			@input.nodes('ReplaceShardMappingsGlobal/RemoveSteps') as t(x)

		if (@stepShardId is null)
			goto Error_MissingParameters;
	
		declare @currentRemoveStep xml,
				@removeStepIndex int = 1

		while (@removeStepIndex <= @removeStepsCount)
		begin
			select 
				@currentRemoveStep = x.query('(./Step[@Id = sql:variable("@removeStepIndex")])[1]') 
			from
				@input.nodes('ReplaceShardMappingsGlobal/RemoveSteps') as t(x)

			-- read the remove step
			select 
				@stepMappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier')
			from
				@currentRemoveStep.nodes('./Step') as t(x)

			if (@stepMappingId is null)
				goto Error_MissingParameters;

			delete from 
				__ShardManagement.ShardMappingsGlobal
			where
				ShardMapId = @shardMapId and MappingId = @stepMappingId and ShardId = @stepShardId

			-- reset state for next iteration
			set @stepMappingId = null

			set @removeStepIndex = @removeStepIndex + 1
		end

		-- reset state for add/update case
		set @stepShardId = null
	end

	-- read the input for the add operations
	if (@addStepsCount > 0)
	begin
		-- read the shard information for removes
		select 
			@stepShardId = x.value('(Shard/Id)[1]', 'uniqueidentifier')
		from 
			@input.nodes('ReplaceShardMappingsGlobal/AddSteps') as t(x)

		if (@stepShardId is null)
			goto Error_MissingParameters;

		declare @currentAddStep xml,
				@addStepIndex int = 1,
				@stepMinValue varbinary(128),
				@stepMaxValue varbinary(128),
				@stepStatus int
		
		while (@addStepIndex <= @addStepsCount)
		begin
			select 
				@currentAddStep = x.query('(./Step[@Id = sql:variable("@addStepIndex")])[1]') 
			from
				@input.nodes('ReplaceShardMappingsGlobal/AddSteps') as t(x)
		
			select
				@stepMappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier'),
				@stepMinValue = convert(varbinary(128), x.value('(Mapping/MinValue)[1]', 'varchar(258)'), 1),
				@stepMaxValue = convert(varbinary(128), x.value('(Mapping/MaxValue[@Null="0"])[1]', 'varchar(258)'), 1),
				@stepStatus = x.value('(Mapping/Status)[1]', 'int')
			from
				@currentAddStep.nodes('./Step') as t(x)
	
			if (@stepMappingId is null or @stepMinValue is null or @stepStatus is null)
				goto Error_MissingParameters;

			-- add mapping
			insert into
				__ShardManagement.ShardMappingsGlobal(
				MappingId, 
				Readable,
				ShardId, 
				ShardMapId, 
				OperationId, 
				MinValue, 
				MaxValue, 
				Status)
			values (
				@stepMappingId, 
				1,
				@stepShardId, 
				@shardMapId, 
				null, 
				@stepMinValue, 
				@stepMaxValue, 
				@stepStatus)

			-- reset state for next iteration
			set @stepMappingId = null
			set @stepMinValue = null
			set @stepMaxValue = null
			set @stepStatus = null

			set @addStepIndex = @addStepIndex + 1
		end
	end

	set @result = 1
	goto Exit_Procedure;

Error_ShardMapNotFound:
	set @result = 102
	goto Exit_Procedure;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Error_GSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionGlobalHelper
	goto Exit_Procedure;

Exit_Procedure:
end
go
