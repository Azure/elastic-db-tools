---------------------------------------------------------------------------------------------------
-- Reads from shard map manager version information table if it exists.
---------------------------------------------------------------------------------------------------

declare @stmt varchar(128)
if object_id(N'__ShardManagement.ShardMapManagerGlobal', N'U') is not null
begin
	if exists(select Name from sys.columns where Name = N'StoreVersion' and object_id = object_id(N'__ShardManagement.ShardMapManagerGlobal'))
	begin
		set @stmt = 'select 5, StoreVersion from __ShardManagement.ShardMapManagerGlobal'
	end
	else
	begin
		set @stmt = 'select 5, StoreVersionMajor, StoreVersionMinor  from __ShardManagement.ShardMapManagerGlobal'
	end
	exec(@stmt)
end
go
