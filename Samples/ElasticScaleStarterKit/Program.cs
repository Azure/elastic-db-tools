// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema;

namespace ElasticScaleStarterKit
{
    internal class Program
    {
        public static void Main()
        {
            // Welcome screen
            Console.WriteLine("***********************************************************");
            Console.WriteLine("***    Welcome to Elastic Database Tools Starter Kit    ***");
            Console.WriteLine("***********************************************************");
            Console.WriteLine();

            // Verify that we can connect to the Sql Database that is specified in App.config settings
            if (!SqlDatabaseUtils.TryConnectToSqlDatabase())
            {
                // Connecting to the server failed - please update the settings in App.Config

                // Give the user a chance to read the mesage, if this program is being run
                // in debug mode in Visual Studio
                if (Debugger.IsAttached)
                {
                    Console.WriteLine("Press ENTER to continue...");
                    Console.ReadLine();
                }

                // Exit
                return;
            }

            // Connection succeeded. Begin interactive loop
            MenuLoop();
        }

        /// <summary>
        /// The shard map manager, or null if it does not exist. 
        /// It is recommended that you keep only one shard map manager instance in
        /// memory per AppDomain so that the mapping cache is not duplicated.
        /// </summary>
        private static ShardMapManager s_shardMapManager;

        #region Program control flow

        /// <summary>
        /// Main program loop.
        /// </summary>
        private static void MenuLoop()
        {
            // Get the shard map manager, if it already exists.
            // It is recommended that you keep only one shard map manager instance in
            // memory per AppDomain so that the mapping cache is not duplicated.
            s_shardMapManager = ShardManagementUtils.TryGetShardMapManager(
                Configuration.ShardMapManagerServerName,
                Configuration.ShardMapManagerDatabaseName);

            // Loop until the user chose "Exit".
            bool continueLoop;
            do
            {
                PrintShardMapState();
                Console.WriteLine();

                PrintMenu();
                Console.WriteLine();

                continueLoop = GetMenuChoiceAndExecute();
                Console.WriteLine();
            }
            while (continueLoop);
        }

        /// <summary>
        /// Writes the shard map's state to the console.
        /// </summary>
        private static void PrintShardMapState()
        {
            Console.WriteLine("Current Shard Map state:");
            RangeShardMap<int> shardMap = TryGetShardMap();
            if (shardMap == null)
            {
                return;
            }

            // Get all shards
            IEnumerable<Shard> allShards = shardMap.GetShards();

            // Get all mappings, grouped by the shard that they are on. We do this all in one go to minimise round trips.
            ILookup<Shard, RangeMapping<int>> mappingsGroupedByShard = shardMap.GetMappings().ToLookup(m => m.Shard);

            if (allShards.Any())
            {
                // The shard map contains some shards, so for each shard (sorted by database name)
                // write out the mappings for that shard
                foreach (Shard shard in shardMap.GetShards().OrderBy(s => s.Location.Database))
                {
                    IEnumerable<RangeMapping<int>> mappingsOnThisShard = mappingsGroupedByShard[shard];

                    if (mappingsOnThisShard.Any())
                    {
                        string mappingsString = string.Join(", ", mappingsOnThisShard.Select(m => m.Value));
                        Console.WriteLine("\t{0} contains key range {1}", shard.Location.Database, mappingsString);
                    }
                    else
                    {
                        Console.WriteLine("\t{0} contains no key ranges.", shard.Location.Database);
                    }
                }
            }
            else
            {
                Console.WriteLine("\tShard Map contains no shards");
            }
        }

        private const ConsoleColor EnabledColor = ConsoleColor.White; // color for items that are expected to succeed
        private const ConsoleColor DisabledColor = ConsoleColor.DarkGray; // color for items that are expected to fail

        /// <summary>
        /// Writes the program menu.
        /// </summary>
        private static void PrintMenu()
        {
            ConsoleColor createSmmColor; // color for create shard map manger menu item
            ConsoleColor otherMenuItemColor; // color for other menu items
            if (s_shardMapManager == null)
            {
                createSmmColor = EnabledColor;
                otherMenuItemColor = DisabledColor;
            }
            else
            {
                createSmmColor = DisabledColor;
                otherMenuItemColor = EnabledColor;
            }

            ConsoleUtils.WriteColor(createSmmColor, "1. Create shard map manager, and add a couple shards");
            ConsoleUtils.WriteColor(otherMenuItemColor, "2. Add another shard");
            ConsoleUtils.WriteColor(otherMenuItemColor, "3. Insert sample rows using Data-Dependent Routing");
            ConsoleUtils.WriteColor(otherMenuItemColor, "4. Execute sample Multi-Shard Query");
            ConsoleUtils.WriteColor(otherMenuItemColor, "5. Drop shard map manager database and all shards");
            ConsoleUtils.WriteColor(EnabledColor, "6. Exit");
        }

        /// <summary>
        /// Gets the user's chosen menu item and executes it.
        /// </summary>
        /// <returns>true if the program should continue executing.</returns>
        private static bool GetMenuChoiceAndExecute()
        {
            while (true)
            {
                int inputValue = ConsoleUtils.ReadIntegerInput("Enter an option [1-6] and press ENTER: ");

                switch (inputValue)
                {
                    case 1: // Create shard map manager
                        Console.WriteLine();
                        CreateShardMapManagerAndShard();
                        return true;
                    case 2: // Add shard
                        Console.WriteLine();
                        AddShard();
                        return true;
                    case 3: // Data Dependent Routing
                        Console.WriteLine();
                        DataDepdendentRouting();
                        return true;
                    case 4: // Multi-Shard Query
                        Console.WriteLine();
                        MultiShardQuery();
                        return true;
                    case 5: // Drop all
                        Console.WriteLine();
                        DropAll();
                        return true;
                    case 6: // Exit
                        return false;
                }
            }
        }

        #endregion      

        #region Menu item implementations

        /// <summary>
        /// Creates a shard map manager, creates a shard map, and creates a shard
        /// with a mapping for the full range of 32-bit integers.
        /// </summary>
        private static void CreateShardMapManagerAndShard()
        {
            if (s_shardMapManager != null)
            {
                ConsoleUtils.WriteWarning("Shard Map Manager already exists");
                return;
            }

            // Create shard map manager database
            if (!SqlDatabaseUtils.DatabaseExists(Configuration.ShardMapManagerServerName, Configuration.ShardMapManagerDatabaseName))
            {
                SqlDatabaseUtils.CreateDatabase(Configuration.ShardMapManagerServerName, Configuration.ShardMapManagerDatabaseName);
            }

            // Create shard map manager
            string shardMapManagerConnectionString =
                Configuration.GetConnectionString(
                    Configuration.ShardMapManagerServerName,
                    Configuration.ShardMapManagerDatabaseName);

            s_shardMapManager = ShardManagementUtils.CreateOrGetShardMapManager(shardMapManagerConnectionString);

            // Create shard map
            RangeShardMap<int> shardMap = ShardManagementUtils.CreateOrGetRangeShardMap<int>(
                s_shardMapManager, Configuration.ShardMapName);

            // Create schema info so that the split-merge service can be used to move data in sharded tables
            // and reference tables.
            CreateSchemaInfo(shardMap.Name);

            // If there are no shards, add two shards: one for [0,100) and one for [100,+inf)
            if (!shardMap.GetShards().Any())
            {
                CreateShardSample.CreateShard(shardMap, new Range<int>(0, 100));
                CreateShardSample.CreateShard(shardMap, new Range<int>(100, 200));
            }
        }

        /// <summary>
        /// Creates schema info for the schema defined in InitializeShard.sql.
        /// </summary>
        private static void CreateSchemaInfo(string shardMapName)
        {
            // Create schema info
            SchemaInfo schemaInfo = new SchemaInfo();
            schemaInfo.Add(new ReferenceTableInfo("Regions"));
            schemaInfo.Add(new ReferenceTableInfo("Products"));
            schemaInfo.Add(new ShardedTableInfo("Customers", "CustomerId"));
            schemaInfo.Add(new ShardedTableInfo("Orders", "CustomerId"));

            // Register it with the shard map manager for the given shard map name
            s_shardMapManager.GetSchemaInfoCollection().Add(shardMapName, schemaInfo);
        }

        /// <summary>
        /// Reads the user's choice of a split point, and creates a new shard with a mapping for the resulting range.
        /// </summary>
        private static void AddShard()
        {
            RangeShardMap<int> shardMap = TryGetShardMap();
            if (shardMap != null)
            {
                // Here we assume that the ranges start at 0, are contiguous, 
                // and are bounded (i.e. there is no range where HighIsMax == true)
                int currentMaxHighKey = shardMap.GetMappings().Max(m => m.Value.High);
                int defaultNewHighKey = currentMaxHighKey + 100;

                Console.WriteLine("A new range with low key {0} will be mapped to the new shard.", currentMaxHighKey);
                int newHighKey = ConsoleUtils.ReadIntegerInput(
                    string.Format("Enter the high key for the new range [default {0}]: ", defaultNewHighKey),
                    defaultNewHighKey,
                    input => input > currentMaxHighKey);

                Range<int> range = new Range<int>(currentMaxHighKey, newHighKey);

                Console.WriteLine();
                Console.WriteLine("Creating shard for range {0}", range);
                CreateShardSample.CreateShard(shardMap, range);
            }
        }

        /// <summary>
        /// Executes the Data-Dependent Routing sample.
        /// </summary>
        private static void DataDepdendentRouting()
        {
            RangeShardMap<int> shardMap = TryGetShardMap();
            if (shardMap != null)
            {
                DataDependentRoutingSample.ExecuteDataDependentRoutingQuery(
                    shardMap,
                    Configuration.GetCredentialsConnectionString());
            }
        }

        /// <summary>
        /// Executes the Multi-Shard Query sample.
        /// </summary>
        private static void MultiShardQuery()
        {
            RangeShardMap<int> shardMap = TryGetShardMap();
            if (shardMap != null)
            {
                MultiShardQuerySample.ExecuteMultiShardQuery(
                    shardMap,
                    Configuration.GetCredentialsConnectionString());
            }
        }

        /// <summary>
        /// Drops all shards and the shard map manager database (if it exists).
        /// </summary>
        private static void DropAll()
        {
            RangeShardMap<int> shardMap = TryGetShardMap();
            if (shardMap != null)
            {
                // Drop shards
                foreach (Shard shard in shardMap.GetShards())
                {
                    SqlDatabaseUtils.DropDatabase(shard.Location.DataSource, shard.Location.Database);
                }
            }

            if (SqlDatabaseUtils.DatabaseExists(Configuration.ShardMapManagerServerName, Configuration.ShardMapManagerDatabaseName))
            {
                // Drop shard map manager database
                SqlDatabaseUtils.DropDatabase(Configuration.ShardMapManagerServerName, Configuration.ShardMapManagerDatabaseName);
            }

            // Since we just dropped the shard map manager database, this shardMapManager reference is now non-functional.
            // So set it to null so that the program knows that the shard map manager is gone.
            s_shardMapManager = null;
        }

        #endregion

        #region Shard map helper methods

        /// <summary>
        /// Gets the shard map, if it exists. If it doesn't exist, writes out the reason and returns null.
        /// </summary>
        private static RangeShardMap<int> TryGetShardMap()
        {
            if (s_shardMapManager == null)
            {
                ConsoleUtils.WriteWarning("Shard Map Manager has not yet been created");
                return null;
            }

            RangeShardMap<int> shardMap;
            bool mapExists = s_shardMapManager.TryGetRangeShardMap(Configuration.ShardMapName, out shardMap);

            if (!mapExists)
            {
                ConsoleUtils.WriteWarning("Shard Map Manager has been created, but the Shard Map has not been created");
                return null;
            }

            return shardMap;
        }

        #endregion
    }
}
