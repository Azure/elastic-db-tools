// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Utility properties and methods used for managing scripts and errors.
    /// </summary>
    internal static partial class Scripts
    {
        internal static readonly UpgradeScript UpgradeShardMapManagerGlobalFrom1000_0To1000_1 = new UpgradeScript(1000, 0, @"
-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Global Shard Map from version 1000.0 to 1000.1
---------------------------------------------------------------------------------------------------

-- drop extra column from ShardMapManagerGlobal table which was added as first step to hold SCH-M lock during upgrade
if exists(select * from sys.columns where Name = N'UpgradeLock' and object_id = object_id(N'__ShardManagement.ShardMapManagerGlobal'))
begin
	alter table __ShardManagement.ShardMapManagerGlobal drop column UpgradeLock
end
go
");
    }
}