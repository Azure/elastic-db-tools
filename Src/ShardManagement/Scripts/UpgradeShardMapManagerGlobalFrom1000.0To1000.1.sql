---------------------------------------------------------------------------------------------------
-- Script to upgrade Global Shard Map from version 1000.0 to 1000.1
---------------------------------------------------------------------------------------------------

-- drop extra column from ShardMapManagerGlobal table which was added as first step to hold SCH-M lock during upgrade
if exists(select * from sys.columns where Name = N'UpgradeLock' and object_id = object_id(N'__ShardManagement.ShardMapManagerGlobal'))
begin
	alter table __ShardManagement.ShardMapManagerGlobal drop column UpgradeLock
end
go