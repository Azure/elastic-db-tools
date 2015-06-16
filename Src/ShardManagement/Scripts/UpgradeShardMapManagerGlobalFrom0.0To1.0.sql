---------------------------------------------------------------------------------------------------
-- Script to upgrade Global Shard Map from version 0.0 to 1.0
---------------------------------------------------------------------------------------------------

-- add a column to ShardMapManagerGlobal table to hold SCH-M lock during upgrade
alter table __ShardManagement.ShardMapManagerGlobal add UpgradeLock int null
go

