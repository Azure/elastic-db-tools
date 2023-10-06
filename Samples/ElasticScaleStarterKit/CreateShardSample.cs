// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleStarterKit
{
    internal class CreateShardSample
    {
        /// <summary>
        /// Creates a new shard (or uses an existing empty shard), adds it to the shard map,
        /// and assigns it the specified range if possible.
        /// </summary>
        public static void CreateShard(RangeShardMap<int> shardMap, Range<int> rangeForNewShard)
        {
            // Create a new shard, or get an existing empty shard (if a previous create partially succeeded).
            Shard shard = CreateOrGetEmptyShard(shardMap);

            // Create a mapping to that shard.
            RangeMapping<int> mappingForNewShard = shardMap.CreateRangeMapping(rangeForNewShard, shard);
            ConsoleUtils.WriteInfo("Mapped range {0} to shard {1}", mappingForNewShard.Value, shard.Location.Database);
        }

        /// <summary>
        /// Script file that will be executed to initialize a shard.
        /// </summary>
        private const string InitializeShardScriptFile = "InitializeShard.sql";

        /// <summary>
        /// Format to use for creating shard name. {0} is the number of shards that have already been created.
        /// </summary>
        private const string ShardNameFormat = "ElasticScaleStarterKit_Shard{0}";

        /// <summary>
        /// Creates a new shard, or gets an existing empty shard (i.e. a shard that has no mappings).
        /// The reason why an empty shard might exist is that it was created and initialized but we 
        /// failed to create a mapping to it.
        /// </summary>
        private static Shard CreateOrGetEmptyShard(RangeShardMap<int> shardMap)
        {
            // Get an empty shard if one already exists, otherwise create a new one
            Shard shard = FindEmptyShard(shardMap);
            if (shard == null)
            {
                // No empty shard exists, so create one

                // Choose the shard name
                string databaseName = string.Format(ShardNameFormat, shardMap.GetShards().Count());

                // Only create the database if it doesn't already exist. It might already exist if
                // we tried to create it previously but hit a transient fault.
                if (!SqlDatabaseUtils.DatabaseExists(Configuration.ShardMapManagerServerName, databaseName))
                {
                    SqlDatabaseUtils.CreateDatabase(Configuration.ShardMapManagerServerName, databaseName);
                }

                // Create schema and populate reference data on that database
                // The initialize script must be idempotent, in case it was already run on this database
                // and we failed to add it to the shard map previously
                SqlDatabaseUtils.ExecuteSqlScript(
                    Configuration.ShardMapManagerServerName, databaseName, InitializeShardScriptFile);

                // Add it to the shard map
                ShardLocation shardLocation = new ShardLocation(Configuration.ShardMapManagerServerName, databaseName);
                shard = ShardManagementUtils.CreateOrGetShard(shardMap, shardLocation);
            }

            return shard;
        }

        /// <summary>
        /// Finds an existing empty shard, or returns null if none exist.
        /// </summary>
        private static Shard FindEmptyShard(RangeShardMap<int> shardMap)
        {
            // Get all shards in the shard map
            IEnumerable<Shard> allShards = shardMap.GetShards();

            // Get all mappings in the shard map
            IEnumerable<RangeMapping<int>> allMappings = shardMap.GetMappings();

            // Determine which shards have mappings
            HashSet<Shard> shardsWithMappings = new HashSet<Shard>(allMappings.Select(m => m.Shard));

            // Get the first shard (ordered by name) that has no mappings, if it exists
            return allShards.OrderBy(s => s.Location.Database).FirstOrDefault(s => !shardsWithMappings.Contains(s));
        }
    }
}
