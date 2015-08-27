-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Global Shard Map from version 1.1 to 1.2
-- Fix for VSTS# 3410606
---------------------------------------------------------------------------------------------------

-- drop extra objects from version 1.1

if object_id(N'__ShardManagement.spLockOrUnlockShardMappingsGlobal', N'P') is not null
begin
	drop procedure __ShardManagement.spLockOrUnlockShardMappingsGlobal
end
go

-- create new objects for version 1.2

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
	declare @gsmVersionMajorClient int, 
			@gsmVersionMinorClient int,
			@shardMapId uniqueidentifier,
			@mappingId uniqueidentifier,
			@lockOwnerId uniqueidentifier,
			@lockOperationType int

	select
		@gsmVersionMajorClient = x.value('(GsmVersion/MajorVersion)[1]', 'int'), 
		@gsmVersionMinorClient = x.value('(GsmVersion/MinorVersion)[1]', 'int'),
		@shardMapId = x.value('(ShardMap/Id)[1]', 'uniqueidentifier'),
		@mappingId = x.value('(Mapping/Id)[1]', 'uniqueidentifier'),
		@lockOwnerId = x.value('(Lock/Id)[1]', 'uniqueidentifier'),
		@lockOperationType = x.value('(Lock/Operation)[1]', 'int')
	from
		@input.nodes('/LockOrUnlockShardMappingsGlobal') as t(x)

	if (@gsmVersionMajorClient is null or @gsmVersionMinorClient is null or @shardMapId is null or @lockOwnerId is null or @lockOperationType is null)
		goto Error_MissingParameters;

	if (@gsmVersionMajorClient <> __ShardManagement.fnGetStoreVersionMajorGlobal())
		goto Error_GSMVersionMismatch;

	if (@lockOperationType < 2 and @mappingId is null)
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

	if (@lockOperationType < 2)
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
			@lockOperationType = 1 or @lockOperationType = 2 or @lockOperationType = 3
		then 
			@DefaultLockOwnerId 
		end
		where
			ShardMapId = @shardMapId and (@lockOperationType = 3 or -- unlock all mappings
										  (@lockOperationType = 2 and LockOwnerId = @lockOwnerId) or -- unlock all mappings for specified LockOwnerId
										  MappingId = @mappingId) -- lock/unlock specified mapping with specified LockOwnerId

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

-- update version as 1.2
update
	__ShardManagement.ShardMapManagerGlobal 
set 
	StoreVersionMinor = 2
where
	StoreVersionMajor = 1 and StoreVersionMinor = 1

go
