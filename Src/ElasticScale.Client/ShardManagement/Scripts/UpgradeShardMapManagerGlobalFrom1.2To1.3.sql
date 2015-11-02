-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

---------------------------------------------------------------------------------------------------
-- Script to upgrade Global Shard Map from version 1.2 to 1.3
-- Fix for https://github.com/Azure/elastic-db-tools/issues/77
-- Only the Global Shard Map is affected
---------------------------------------------------------------------------------------------------

update __ShardManagement.ShardedDatabaseSchemaInfosGlobal
set SchemaInfo = 
	replace(
		replace(
			replace(
				replace(
					cast(SchemaInfo as nvarchar(max)), 
					'<_referenceTableSet i:type="ArrayOfReferenceTableInfo">', 
					'<ReferenceTableSet i:type="ArrayOfReferenceTableInfo">'),
				'</_referenceTableSet>',
				'</ReferenceTableSet>'),
		'<_shardedTableSet i:type="ArrayOfShardedTableInfo">', 
		'<ShardedTableSet i:type="ArrayOfShardedTableInfo">'),
	'</_shardedTableSet>',
	'</ShardedTableSet>')
go
