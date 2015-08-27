-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Local Shard Map from version 1.1 to 1.2
---------------------------------------------------------------------------------------------------

-- drop extra objects from version 1.1

if object_id(N'__ShardManagement.spUpdateShardLocal', N'P') is not null
begin
	drop procedure __ShardManagement.spUpdateShardLocal
end	
go

if object_id(N'__ShardManagement.spBulkOperationShardMappingsLocal', N'P') is not null
begin
	drop procedure __ShardManagement.spBulkOperationShardMappingsLocal
end	
go

if object_id(N'__ShardManagement.spAddShardLocal', N'P') is not null
begin
	drop procedure __ShardManagement.spAddShardLocal
end	
go

-- create new objects for version 1.2

---------------------------------------------------------------------------------------------------
-- __ShardManagement.spUpdateShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spUpdateShardLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionMajorClient int, 
			@lsmVersionMinorClient int,
			@operationId uniqueidentifier,
			@shardMapId uniqueidentifier,
			@shardId uniqueidentifier,
			@shardVersion uniqueidentifier,
			@protocol int,
			@serverName nvarchar(128),
			@port int,
			@databaseName nvarchar(128),
			@shardStatus int

	select 
		@lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
		@lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
		@shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier'),
		@protocol = x.value('(Shard/Location/Protocol)[1]', 'int'),
		@serverName = x.value('(Shard/Location/ServerName)[1]', 'nvarchar(128)'),
		@port = x.value('(Shard/Location/Port)[1]', 'int'),
		@databaseName = x.value('(Shard/Location/DatabaseName)[1]', 'nvarchar(128)'),
		@shardStatus = x.value('(Shard/Status)[1]', 'int')
	from 
		@input.nodes('/UpdateShardLocal') as t(x)

	if (@lsmVersionMajorClient is null or @lsmVersionMinorClient is null or @operationId is null or 
		@shardMapId is null or @shardId is null or @shardVersion is null or @shardStatus is null or
		@protocol is null or @serverName is null or @port is null or @databaseName is null)
		goto Error_MissingParameters;

	if (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
		goto Error_LSMVersionMismatch;

	update 
		__ShardManagement.ShardsLocal
	set
		Version = @shardVersion,
		Status = @shardStatus,
		Protocol = @protocol,
		ServerName = @serverName,
		Port = @port,
		DatabaseName = @databaseName,
		LastOperationId = @operationId
	where
		ShardMapId = @shardMapId and ShardId = @shardId

	if (@@rowcount = 0)
		goto Error_ShardDoesNotExist;

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

Error_ShardDoesNotExist:
	set @result = 202
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
	declare @lsmVersionMajorClient int,
			@lsmVersionMinorClient int,
			@operationId uniqueidentifier,
			@operationCode int,
			@undo int,
			@stepsCount int,
			@shardMapId uniqueidentifier,
			@sm_kind int,
			@shardId uniqueidentifier,
			@shardVersion uniqueidentifier

	-- get operation information as well as number of steps information
	select 
		@lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'),
		@lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@undo = x.value('(@Undo)[1]', 'int'),
		@stepsCount = x.value('(@StepsCount)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@shardId = x.value('(Shard/Id)[1]', 'uniqueidentifier'),
		@shardVersion = x.value('(Shard/Version)[1]', 'uniqueidentifier')
	from 
		@input.nodes('/BulkOperationShardMappingsLocal') as t(x)

	if (@lsmVersionMajorClient is null or @lsmVersionMinorClient is null or @operationId is null or @stepsCount is null or @shardMapId is null or @shardId is null or @shardVersion is null)
		goto Error_MissingParameters;

	if (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
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
			begin try
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
			end try
			begin catch
			if (@undo != 1)
			begin
				declare @errorMessage nvarchar(max) = error_message(),
					@errorNumber int = error_number(),
					@errorSeverity int = error_severity(),
					@errorState int = error_state(),
					@errorLine int = error_line(),
					@errorProcedure nvarchar(128) = isnull(error_procedure(), '-');
					
					select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage
					raiserror (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
					rollback transaction; -- To avoid extra error message in response.
					goto Error_UnexpectedError;
			end
			end catch

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
-- __ShardManagement.spAddShardLocal
-- Constraints:
---------------------------------------------------------------------------------------------------
create procedure __ShardManagement.spAddShardLocal
@input xml,
@result int output
as
begin
	declare @lsmVersionMajorClient int, 
			@lsmVersionMinorClient int,
			@operationId uniqueidentifier,
			@undo int,
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
			@shardStatus  int,
			@errorMessage nvarchar(max),
			@errorNumber int,
			@errorSeverity int,
			@errorState int,
			@errorLine int,
			@errorProcedure nvarchar(128)
	select 
		@lsmVersionMajorClient = x.value('(LsmVersion/MajorVersion)[1]', 'int'), 
		@lsmVersionMinorClient = x.value('(LsmVersion/MinorVersion)[1]', 'int'),
		@operationId = x.value('(@OperationId)[1]', 'uniqueidentifier'),
		@undo = x.value('(@Undo)[1]', 'int'),
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

	if (@lsmVersionMajorClient is null or @lsmVersionMinorClient is null or @shardMapId is null or @operationId is null or @name is null or @sm_kind is null or @sm_keykind is null or 
		@shardId is null or @shardVersion is null or @protocol is null or @serverName is null or 
		@port is null or @databaseName is null or @shardStatus is null)
		goto Error_MissingParameters;

	if (@lsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorLocal())
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

	-- add shard map row, ignore duplicate inserts in this is part of undo operation
	begin try
		insert into 
			__ShardManagement.ShardMapsLocal 
			(ShardMapId, Name, MapType, KeyType, LastOperationId)
		values 
			(@shardMapId, @name, @sm_kind, @sm_keykind, @operationId)
	end try
	begin catch
	if (@undo != 1)
	begin
		set @errorMessage = error_message();
		set @errorNumber = error_number();
		set @errorSeverity = error_severity();
		set @errorState = error_state();
		set @errorLine = error_line();
		set @errorProcedure  = isnull(error_procedure(), '-');					
			select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage
			raiserror (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
			rollback transaction; -- To avoid extra error message in response.
			goto Error_UnexpectedError;
	end
	end catch

	-- add shard row, ignore duplicate inserts if this is part of undo operation
	begin try
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
	end try
	begin catch
	if (@undo != 1)
	begin
		set @errorMessage = error_message();
		set @errorNumber = error_number();
		set @errorSeverity = error_severity();
		set @errorState = error_state();
		set @errorLine = error_line();
		set @errorProcedure  = isnull(error_procedure(), '-');
					
			select @errorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, Message: ' + @errorMessage
			raiserror (@errorMessage, @errorSeverity, 1, @errorNumber, @errorSeverity, @errorState, @errorProcedure, @errorLine);
			rollback transaction; -- To avoid extra error message in response.
			goto Error_UnexpectedError;
	end
	end catch 

	goto Success_Exit;

Error_MissingParameters:
	set @result = 50
	exec __ShardManagement.spGetStoreVersionLocalHelper
	goto Exit_Procedure;

Error_LSMVersionMismatch:
	set @result = 51
	exec __ShardManagement.spGetStoreVersionLocalHelper
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

-- update version as 1.2
update
	__ShardManagement.ShardMapManagerLocal 
set 
	StoreVersionMinor = 2
where
	StoreVersionMajor = 1 and StoreVersionMinor = 1
go
