-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Local Shard Map from version 0.0 to 1.0
---------------------------------------------------------------------------------------------------

-- add a column to ShardMapManagerLocal table to hold SCH-M lock during upgrade
alter table __ShardManagement.ShardMapManagerLocal add UpgradeLock int null
go
