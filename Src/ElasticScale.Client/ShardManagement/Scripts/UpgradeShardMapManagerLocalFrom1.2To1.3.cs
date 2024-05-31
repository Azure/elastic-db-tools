// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
	/// <summary>
	/// Utility properties and methods used for managing scripts and errors.
	/// </summary>
	internal static partial class Scripts
    {
        internal static readonly UpgradeScript UpgradeShardMapManagerLocalFrom1_2To1_3 = new UpgradeScript(1, 2, @"
-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Local Shard Map from version 1.2 to 1.3
---------------------------------------------------------------------------------------------------

-- drop extra objects from version 1.2

if object_id(N'__ShardManagement.spKillSessionsForShardMappingLocal', N'P') is not null
begin
	drop procedure __ShardManagement.spKillSessionsForShardMappingLocal
end
go

-- create new objects for version 1.3

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
		end try
		begin catch
			-- if the process is no longer valid, assume that it is gone
			if (error_number() <> 6106)
				goto Error_UnableToKillSessions;
		end catch

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

-- update version as 1.3
update
	__ShardManagement.ShardMapManagerLocal
set
	StoreVersionMinor = 3
where
	StoreVersionMajor = 1 and StoreVersionMinor = 2
go
");
    }
}