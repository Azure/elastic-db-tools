// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Utility properties and methods used for managing scripts and errors.
    /// </summary>
    internal static partial class Scripts
    {
        internal static readonly UpgradeScript UpgradeShardMapManagerGlobalFrom0_0To1_0 = new UpgradeScript(0, 0, @"
-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Global Shard Map from version 0.0 to 1.0
---------------------------------------------------------------------------------------------------

-- add a column to ShardMapManagerGlobal table to hold SCH-M lock during upgrade
alter table __ShardManagement.ShardMapManagerGlobal add UpgradeLock int null
go
");
    }
}