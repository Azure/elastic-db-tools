// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Test related to Recovery manager.
    /// </summary>
    [TestClass]
    public class RecoveryManagerTests
    {
        /// <summary>
        /// Sharded databases to create for the test.
        /// </summary>
        private static string[] s_shardedDBs = new[]
        {
            "shard1" + Globals.TestDatabasePostfix,
            "shard2" + Globals.TestDatabasePostfix
        };

        /// <summary>
        /// GSM table names used in cleanup function.
        /// </summary>
        private static string[] s_gsmTables = new[]
        {
            "__ShardManagement.ShardMappingsGlobal",
            "__ShardManagement.ShardsGlobal",
            "__ShardManagement.ShardMapsGlobal",
            "__ShardManagement.OperationsLogGlobal"
        };

        /// <summary>
        /// LSM table names used in cleanup function.
        /// </summary>
        private static string[] s_lsmTables = new[]
        {
            "__ShardManagement.ShardMappingsLocal",
            "__ShardManagement.ShardsLocal",
            "__ShardManagement.ShardMapsLocal"
        };


        /// <summary>
        /// List shard map name.
        /// </summary>
        private static string s_listShardMapName = "Customers_list";

        /// <summary>
        /// Range shard map name.
        /// </summary>
        private static string s_rangeShardMapName = "Customers_range";

        #region Common Methods

        /// <summary>
        /// Helper function to create list and range shard maps.
        /// </summary>
        private static void CreateShardMapsHelper()
        {
            // Create list shard map.
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.CreateListShardMap<int>(RecoveryManagerTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Assert.AreEqual(RecoveryManagerTests.s_listShardMapName, lsm.Name);

            // Create range shard map.
            RangeShardMap<int> rsm = smm.CreateRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Assert.AreEqual(RecoveryManagerTests.s_rangeShardMapName, rsm.Name);
        }

        /// <summary>
        /// Helper function to clean SMM tables from all shards and GSM.
        /// </summary>
        private static void CleanTablesHelper()
        {
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                // Clean LSM tables.
                foreach (string dbName in RecoveryManagerTests.s_shardedDBs)
                {
                    foreach (string tableName in s_lsmTables)
                    {
                        using (SqlCommand cmd = new SqlCommand(
                            string.Format(Globals.CleanTableQuery, dbName, tableName),
                            conn))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                // Clean GSM tables
                foreach (string tableName in s_gsmTables)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CleanTableQuery, Globals.ShardMapManagerDatabaseName, tableName),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void RecoveryManagerTestsInitialize(TestContext testContext)
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                // Create ShardMapManager database
                using (SqlCommand cmd = new SqlCommand(
                    string.Format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                    conn))
                {
                    cmd.ExecuteNonQuery();
                }

                // Create shard databases
                for (int i = 0; i < RecoveryManagerTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, RecoveryManagerTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, RecoveryManagerTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            // Create shard map manager.
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);
        }

        /// <summary>
        /// Cleans up common state for the all tests in this class.
        /// </summary>
        [ClassCleanup()]
        public static void RecoveryManagerTestsCleanup()
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();
                // Drop shard databases
                for (int i = 0; i < RecoveryManagerTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, RecoveryManagerTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                // Drop shard map manager database
                using (SqlCommand cmd = new SqlCommand(
                    string.Format(Globals.DropDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                    conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Initializes common state per-test.
        /// </summary>
        [TestInitialize()]
        public void ShardMapperTestInitialize()
        {
            CreateShardMapsHelper();
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        [TestCleanup()]
        public void ShardMapperTestCleanup()
        {
            CleanTablesHelper();
        }


        #endregion Common Methods


        /// <summary>
        /// Test Detach and Attach Shard Scenario. (This is just a stub for now.)
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestDetachAttachShard()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            // I am still not fully clear on the use case for AttachShard and DetachShard, but here's a simple test validating that 
            // they don't throw exceptions if they get called against themselves.
            RecoveryManager rm = new RecoveryManager(smm);
            rm.DetachShard(sl);
            rm.AttachShard(sl);
        }

        /// <summary>
        /// Test that consistency detection works when there are no conflicts.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestConsistencyDetectionAndViewingWithNoConflicts()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            RecoveryManager rm = new RecoveryManager(smm);

            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Keys.Count, "An unexpected conflict was detected");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;

                    Assert.AreEqual(MappingLocation.MappingInShardMapAndShard, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }
            }
        }

        /// <summary>
        /// Test that consistency detection works when there are only version conflicts.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestConsistencyDetectionAndViewingWithVersionOnlyConflict()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            // Make sure no other rangemappings are floating around here.
            var rangeMappings = rsm.GetMappings();
            foreach (var rangeMapping in rangeMappings)
            {
                rsm.DeleteMapping(rangeMapping);
            }

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            // Corrupt the mapping id number on the global shardmap.

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("update {0}.__ShardManagement.ShardMappingsGlobal set MappingId = newid()", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(1, kvps.Keys.Count, "An unexpected conflict was detected");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;

                    Assert.AreEqual(MappingLocation.MappingInShardMapAndShard, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }
            }
        }

        /// <summary>
        /// Test that consistency detection works when the range in GSM is expanded while the LSM is left untouched.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestConsistencyDetectionAndViewingWithWiderRangeInLSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            // Corrupt the lsm by increasing the max range and decreasing min range. We should see two ranges show up in the list of differences. The shared range
            // in the middle artificially has the same version number, so it should not register as a conflicting range.

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("update shard1.__ShardManagement.ShardMappingsLocal set MinValue = MinValue - 1, MaxValue = MaxValue + 1", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(2, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(1, (int)range.High.Value - (int)range.Low.Value, "The ranges reported differed from those expected.");
                    Assert.AreEqual(MappingLocation.MappingInShardOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }
            }
        }

        /// <summary>
        /// Test that consistency detection works when the range in GSM is expanded while the LSM is left untouched.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestConsistencyDetectionAndViewingWithWiderRangeInGSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            // Corrupt the gsm by increasing the max range and decreasing min range. We should see two ranges show up in the list of differences. The shared range
            // in the middle artificially has the same version number, so it should not register as a conflicting range.

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("update {0}.__ShardManagement.ShardMappingsGlobal set MinValue = MinValue - 1, MaxValue = MaxValue + 1", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(2, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(1, (int)range.High.Value - (int)range.Low.Value, "The ranges reported differed from those expected.");
                    Assert.AreEqual(MappingLocation.MappingInShardMapOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }
            }
        }

        /// <summary>
        /// Test that consistency detection works the GSM is missing a range added to the LSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestConsistencyDetectionAndViewingWithAdditionalRangeInLSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            // Add a range to the gsm
            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(11, 20), s);


            Assert.IsNotNull(r1);

            // Now, delete the new range from the GSM
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("delete from {0}.__ShardManagement.ShardMappingsGlobal where MinValue = 0x8000000B", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(1, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(20, (int)range.High.Value, "The range reported differed from that expected.");
                    Assert.AreEqual(MappingLocation.MappingInShardOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }
            }
        }

        /// <summary>
        /// Test that consistency detection works with some arbitrary point mappings.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestConsistencyDetectionOnListMapping()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> rsm = smm.GetListShardMap<int>(RecoveryManagerTests.s_listShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);
            Shard s = rsm.CreateShard(sl);
            Assert.IsNotNull(s);

            for (int i = 0; i < 5; i++)
            {
                PointMapping<int> p = rsm.CreatePointMapping(2 * i, s);
                Assert.IsNotNull(p);
            }

            // Now, delete some points from both, and change the version of a shared shard mapping in the middle.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("delete from {0}.__ShardManagement.ShardMappingsGlobal where MinValue IN (0x80000000, 0x80000002)", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand("delete from shard1.__ShardManagement.ShardMappingsLocal where MinValue = 0x80000008", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand("update shard1.__ShardManagement.ShardMappingsLocal set MappingId = newid() where MinValue = 0x80000006", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(4, kvps.Keys.Count, "The count of differences does not match the expected.");
                Assert.AreEqual(1, kvps.Values.Where(l => l == MappingLocation.MappingInShardMapOnly).Count(), "The count of shardmap only differences does not match the expected.");
                Assert.AreEqual(2, kvps.Values.Where(l => l == MappingLocation.MappingInShardOnly).Count(), "The count of shard only differences does not match the expected.");
                Assert.AreEqual(1, kvps.Values.Where(l => l == MappingLocation.MappingInShardMapAndShard).Count(), "The count of shard only differences does not match the expected.");
            }
        }

        /// <summary>
        /// Test that consistency detection works when the ranges on the LSM and GSM are disjoint.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestConsistencyDetectionAndViewingWithDisjointRanges()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            // Make sure no other rangemappings are floating around here.
            var rangeMappings = rsm.GetMappings();
            foreach (var rangeMapping in rangeMappings)
            {
                rsm.DeleteMapping(rangeMapping);
            }

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            // Add a range to the gsm
            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(11, 20), s);

            Assert.IsNotNull(r1);

            // Delete the original range from the GSM.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("delete from {0}.__ShardManagement.ShardMappingsGlobal where MinValue = 0x80000001", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            // Delete the new range from the LSM, so the LSM and GSM now have non-intersecting ranges.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("delete from shard1.__ShardManagement.ShardMappingsLocal where MinValue = 0x8000000B", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(2, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    if ((int)range.High.Value == 10)
                    {
                        Assert.AreEqual(MappingLocation.MappingInShardOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                        continue;
                    }
                    else if ((int)range.High.Value == 20)
                    {
                        Assert.AreEqual(MappingLocation.MappingInShardMapOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                        continue;
                    }
                    Assert.Fail("Unexpected range detected.");
                }
            }
        }


        /// <summary>
        /// Test that consistency detection method produces usable LSMs when shards themselves disagree.
        /// In particular, make sure it reports on subintervals not tagged to the current LSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestConsistencyDetectionWithDivergence()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl1 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);
            ShardLocation sl2 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[1]);


            Shard s1 = rsm.CreateShard(sl1);
            Shard s2 = rsm.CreateShard(sl2);

            // set initial ranges as non-intersecting.
            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 6), s1);
            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(6, 10), s2);

            // Perturb the first LSM so that it has a 
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("update shard1.__ShardManagement.ShardMappingsLocal set MaxValue = 0x8000000B", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
            RecoveryManager rm = new RecoveryManager(smm);
            var gs = rm.DetectMappingDifferences(sl1);
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);

                Assert.AreEqual(2, kvps.Keys.Count, "The count of differences does not match the expected.");

                // We expect 6-10, and 10-11. If we did not detect intersected ranges, and only used tagged ranges, we would have only 6-11 as a single range, which would be insufficient for rebuild.
                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    if ((int)range.High.Value == 10)
                    {
                        Assert.AreEqual(MappingLocation.MappingInShardMapAndShard, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                        continue;
                    }
                    else if ((int)range.High.Value == 11)
                    {
                        Assert.AreEqual(MappingLocation.MappingInShardOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                        continue;
                    }
                    Assert.Fail("Unexpected range detected.");
                }
            }
        }

        /// <summary>
        /// Test the "resolve using GSM" scenario.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestCopyGSMToLSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            // Remove any garbage that might be floating around.
            var rangeMappings = rsm.GetMappings();
            foreach (var rangeMapping in rangeMappings)
            {
                rsm.DeleteMapping(rangeMapping);
            }

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            // Add a range to the gsm
            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(11, 20), s);


            Assert.IsNotNull(r1);

            // Delete the new range from the LSM, so the LSM is missing all mappings from the GSM.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("delete from shard1.__ShardManagement.ShardMappingsLocal", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            // Briefly validate that there are, in fact, the two ranges of inconsistency we are expecting.
            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            // Briefly validate that 
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(2, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(MappingLocation.MappingInShardMapOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }
                // Recover the LSM from the GSM
                rm.ResolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapMapping);
            }


            // Validate that there are no more differences.
            IEnumerable<RecoveryToken> gsAfterFix = rm.DetectMappingDifferences(sl);
            foreach (RecoveryToken g in gsAfterFix)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Keys.Count, "There were still differences after resolution.");
            }
        }

        /// <summary>
        /// Test the "resolve using LSM" scenario.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestCopyLSMToGSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            // Add a range to the gsm
            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(11, 20), s);


            Assert.IsNotNull(r1);

            // Delete everything from GSM (yes, this is overkill.)
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("delete from {0}.__ShardManagement.ShardMappingsGlobal", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            // Briefly validate that there are, in fact, the two ranges of inconsistency we are expecting.
            IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

            Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

            // Briefly validate that 
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(2, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(MappingLocation.MappingInShardOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }
                // Recover the GSM from the LSM
                rm.ResolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
            }

            // Validate that there are no more differences.
            IEnumerable<RecoveryToken> gsAfterFix = rm.DetectMappingDifferences(sl);
            foreach (RecoveryToken g in gsAfterFix)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Keys.Count, "There were still differences after resolution.");
            }
        }

        /// <summary>
        /// Test a restore of GSM from multiple different LSMs. (range)
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestRestoreGSMFromLSMsRange()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);
            IList<ShardLocation> sls = new List<ShardLocation>();
            int i = 0;
            List<RangeMapping<int>> ranges = new List<RangeMapping<int>>();
            foreach (string dbName in RecoveryManagerTests.s_shardedDBs)
            {
                ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, dbName);
                sls.Add(sl);
                Shard s = rsm.CreateShard(sl);
                Assert.IsNotNull(s);
                var r = rsm.CreateRangeMapping(new Range<int>(1 + i * 10, 10 + i * 10), s);
                Assert.IsNotNull(r);
                ranges.Add(r);
                i++;
            }

            // Delete all mappings from GSM
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("delete from {0}.__ShardManagement.ShardMappingsGlobal", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            // Validate that we detect the inconsistencies in all the LSMs.
            foreach (ShardLocation sl in sls)
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

                Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

                // Briefly validate that 
                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    Assert.AreEqual(1, kvps.Keys.Count, "The count of differences does not match the expected.");

                    foreach (var kvp in kvps)
                    {
                        ShardRange range = kvp.Key;
                        MappingLocation mappingLocation = kvp.Value;
                        Assert.AreEqual(MappingLocation.MappingInShardOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                    }
                }
            }

            // Recover the LSM from the GSM
            rm.RebuildMappingsOnShardMapManagerFromShards(sls);

            // Validate that we fixed all the inconsistencies.
            foreach (ShardLocation sl in sls)
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);
                // Briefly validate that 
                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    Assert.AreEqual(0, kvps.Keys.Count, "There were still differences after resolution.");
                }
            }
        }

        /// <summary>
        /// Test a restore of GSM from multiple different LSMs. (range)
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestRestoreGSMFromLSMsRangeWithGarbageInGSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);
            IList<ShardLocation> sls = new List<ShardLocation>();
            int i = 0;
            List<RangeMapping<int>> ranges = new List<RangeMapping<int>>();
            foreach (string dbName in RecoveryManagerTests.s_shardedDBs)
            {
                ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, dbName);
                sls.Add(sl);
                Shard s = rsm.CreateShard(sl);
                Assert.IsNotNull(s);
                var r = rsm.CreateRangeMapping(new Range<int>(1 + i * 10, 10 + i * 10), s);
                Assert.IsNotNull(r);
                ranges.Add(r);
                i++;
            }

            // Perturb the mappings in the GSM.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("update {0}.__ShardManagement.ShardMappingsGlobal set MaxValue = MaxValue + 1, MinValue = MinValue + 1", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            // Validate that we detect the inconsistencies in all the LSMs.
            foreach (ShardLocation sl in sls)
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

                Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

                // Briefly validate that 
                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    Assert.AreEqual(2, kvps.Keys.Count, "The count of differences does not match the expected.");
                }
            }

            // Recover the LSM from the GSM
            rm.RebuildMappingsOnShardMapManagerFromShards(sls);

            // Validate that we fixed all the inconsistencies.
            foreach (ShardLocation sl in sls)
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);
                // Briefly validate that 
                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    Assert.AreEqual(0, kvps.Keys.Count, "There were still differences after resolution.");
                }
            }
        }

        /// <summary>
        /// Test a restore of GSM from multiple different LSMs.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestRestoreGSMFromLSMsList()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            var lsm = smm.GetListShardMap<int>(RecoveryManagerTests.s_listShardMapName);

            Assert.IsNotNull(lsm);
            IList<ShardLocation> sls = new List<ShardLocation>();
            int i = Int32.MaxValue;
            List<PointMapping<int>> points = new List<PointMapping<int>>();
            foreach (string dbName in RecoveryManagerTests.s_shardedDBs)
            {
                ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, dbName);
                sls.Add(sl);
                Shard s = lsm.CreateShard(sl);
                Assert.IsNotNull(s);
                var p = lsm.CreatePointMapping(i, s);
                Assert.IsNotNull(p);
                points.Add(p);
                i--;
            }

            // Delete all mappings from GSM
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("delete from {0}.__ShardManagement.ShardMappingsGlobal", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            // Validate that we detect the inconsistencies in all the LSMs.
            foreach (ShardLocation sl in sls)
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

                Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

                // Briefly validate that 
                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    Assert.AreEqual(1, kvps.Keys.Count, "The count of differences does not match the expected.");

                    foreach (var kvp in kvps)
                    {
                        ShardRange range = kvp.Key;
                        MappingLocation mappingLocation = kvp.Value;
                        Assert.AreEqual(MappingLocation.MappingInShardOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                    }
                }
            }

            // Recover the LSM from the GSM
            rm.RebuildMappingsOnShardMapManagerFromShards(sls);

            // Validate that we fixed all the inconsistencies.
            foreach (ShardLocation sl in sls)
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);
                // Briefly validate that 
                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    Assert.AreEqual(0, kvps.Keys.Count, "There were still differences after resolution.");
                }
            }
        }

        /// <summary>
        /// Test a restore of GSM from multiple different LSMs.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestRestoreGSMFromLSMsListWithGarbageInGSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            var lsm = smm.GetListShardMap<int>(RecoveryManagerTests.s_listShardMapName);

            Assert.IsNotNull(lsm);
            IList<ShardLocation> sls = new List<ShardLocation>();
            int i = 0;
            List<PointMapping<int>> points = new List<PointMapping<int>>();
            foreach (string dbName in RecoveryManagerTests.s_shardedDBs)
            {
                ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, dbName);
                sls.Add(sl);
                Shard s = lsm.CreateShard(sl);
                Assert.IsNotNull(s);
                var p = lsm.CreatePointMapping(i, s);
                Assert.IsNotNull(p);
                points.Add(p);
                i++;
            }

            // Delete all mappings from GSM
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand(String.Format("delete from {0}.__ShardManagement.ShardMappingsGlobal", Globals.ShardMapManagerDatabaseName), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            // Validate that we detect the inconsistencies in all the LSMs.
            foreach (ShardLocation sl in sls)
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

                Assert.AreEqual(1, gs.Count(), "The test environment was not expecting more than one local shardmap.");

                // Briefly validate that 
                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    Assert.AreEqual(1, kvps.Keys.Count, "The count of differences does not match the expected.");
                }
            }

            // Recover the LSM from the GSM
            rm.RebuildMappingsOnShardMapManagerFromShards(sls);

            // Validate that we fixed all the inconsistencies.
            foreach (ShardLocation sl in sls)
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);
                // Briefly validate that 
                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    Assert.AreEqual(0, kvps.Keys.Count, "There were still differences after resolution.");
                }
            }
        }

        /// <summary>
        /// Test that the RebuildShard method produces usable LSMs for subsequent recovery action (range)
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestRebuildShardFromGSMRange()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            for (int i = 0; i < 5; i++)
            {
                RangeMapping<int> r = rsm.CreateRangeMapping(new Range<int>(1 + i, 2 + i), s);
                Assert.IsNotNull(r);
            }

            // Delete all the ranges and shardmaps from the shardlocation.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("delete from shard1.__ShardManagement.ShardMappingsLocal", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);
            var gs = rm.DetectMappingDifferences(sl);

            // Validate that all the shard locations are in fact missing from the LSM.
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(5, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(MappingLocation.MappingInShardMapOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }

                // Rebuild the range, leaving 2 inconsistencies (the last 2)
                rm.RebuildMappingsOnShard(g, kvps.Keys.Take(3));
            }

            gs = rm.DetectMappingDifferences(sl);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(2, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");

                // We expect that the last two ranges only are missing from the shards.
                var expectedLocations = new List<MappingLocation>()
                {
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapOnly,
                    MappingLocation.MappingInShardMapOnly,
                };

                Assert.IsTrue(expectedLocations.Zip(kvps.Values, (x, y) => x == y).Aggregate((x, y) => x && y), "RebuildRangeShardMap rebuilt the shards out of order with respect to its keeplist.");

                // Rebuild the range, leaving 1 inconsistency
                rm.RebuildMappingsOnShard(g, kvps.Keys.Skip(1));
            }

            gs = rm.DetectMappingDifferences(sl);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(1, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");

                // Rebuild the range, leaving no inconsistencies
                rm.RebuildMappingsOnShard(g, kvps.Keys);
            }

            gs = rm.DetectMappingDifferences(sl);

            // Everything should be semantically consistent now.
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");
                rm.ResolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
            }

            // As a sanity check, make sure the root is restorable from this LSM.
            gs = rm.DetectMappingDifferences(sl);
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Keys.Count, "The GSM is not restorable from a rebuilt local shard.");
            }
        }

        // Make sure that rebuildshard does not silently delete nonconflicting ranges.
        //
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestRebuildShardFromGSMRangeKeepNonconflicts()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(RecoveryManagerTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl1 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s1 = rsm.CreateShard(sl1);

            // set initial ranges as non-intersecting.
            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 6), s1);
            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(6, 10), s1);

            // Only mess up the range on the right.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("update shard1.__ShardManagement.ShardMappingsLocal set MaxValue = 0x8000000B, MappingId = newid() where MaxValue = 0x8000000A", conn))
                {
                    cmd.ExecuteNonQuery();
                }
                using (SqlCommand cmd = new SqlCommand("update shard1.__ShardManagement.ShardMappingsLocal set MinValue = 0x8000000B where MinValue = 0x8000000A", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
            RecoveryManager rm = new RecoveryManager(smm);
            var gs = rm.DetectMappingDifferences(sl1);
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);

                Assert.AreEqual(2, kvps.Keys.Count, "The count of differences does not match the expected.");

                // Let's make sure that rebuild does not unintuitively delete ranges 1-6.
                rm.RebuildMappingsOnShard(g, new List<ShardRange>());
            }

            gs = rm.DetectMappingDifferences(sl1);
            foreach (RecoveryToken g in gs)
            {
                // Take local.
                rm.ResolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
            }

            var resultingMappings = rsm.GetMappings(new Range<int>(1, 11), s1);

            // Make sure the mapping [1-6) is still around.
            Assert.AreEqual(1, resultingMappings.Count(), "RebuildShard unexpectedly removed a non-conflicting range.");
        }

        /// <summary>
        /// Test that the RebuildShard method produces usable LSMs for subsequent recovery action (list)
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestRebuildShardFromGSMList()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(RecoveryManagerTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            for (int i = 0; i < 5; i++)
            {
                PointMapping<int> r = lsm.CreatePointMapping(i, s);
                Assert.IsNotNull(r);
            }

            // Delete all the ranges and shardmaps from the shardlocation.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("delete from shard1.__ShardManagement.ShardMappingsLocal", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);
            var gs = rm.DetectMappingDifferences(sl);

            // Validate that all the shard locations are in fact missing from the LSM.
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(5, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(MappingLocation.MappingInShardMapOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }

                // Rebuild the range, leaving 2 inconsistencies (the last 2)
                rm.RebuildMappingsOnShard(g, kvps.Keys.Take(3));
            }

            gs = rm.DetectMappingDifferences(sl);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(2, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");

                // We expect that the last two ranges only are missing from the shards.
                var expectedLocations = new List<MappingLocation>()
                {
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapOnly,
                    MappingLocation.MappingInShardMapOnly,
                };

                Assert.IsTrue(expectedLocations.Zip(kvps.Values, (x, y) => x == y).Aggregate((x, y) => x && y), "RebuildRangeShardMap rebuilt the shards out of order with respect to its keeplist.");

                // Rebuild the range, leaving 1 inconsistency
                rm.RebuildMappingsOnShard(g, kvps.Keys.Skip(1));
            }

            gs = rm.DetectMappingDifferences(sl);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(1, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");

                // Rebuild the range, leaving no inconsistencies
                rm.RebuildMappingsOnShard(g, kvps.Keys);
            }

            gs = rm.DetectMappingDifferences(sl);

            // Everything should be semantically consistent now.
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");
                rm.ResolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
            }

            // As a sanity check, make sure the root is restorable from this LSM.
            gs = rm.DetectMappingDifferences(sl);
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Keys.Count, "The GSM is not restorable from a rebuilt local shard.");
            }
        }

        /// <summary>
        /// Basic sanity checks confirming that pointmappings work the same way rangemappings do in a recover from rebuilt shard scenario.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestPointMappingRecoverFromLSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> listsm = smm.GetListShardMap<int>(RecoveryManagerTests.s_listShardMapName);

            Assert.IsNotNull(listsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = listsm.CreateShard(sl);

            Assert.IsNotNull(s);

            for (int i = 0; i < 5; i++)
            {
                PointMapping<int> r = listsm.CreatePointMapping(creationInfo: new PointMappingCreationInfo<int>(i, s, MappingStatus.Online));
                Assert.IsNotNull(r);
            }

            // Delete all the ranges and shardmaps from the shardlocation.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("delete from shard1.__ShardManagement.ShardMappingsLocal", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);
            var gs = rm.DetectMappingDifferences(sl);

            // Validate that all the shard locations are in fact missing from the LSM.
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(5, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(MappingLocation.MappingInShardMapOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }
                rm.RebuildMappingsOnShard(g, kvps.Keys.Take(3));
            }

            gs = rm.DetectMappingDifferences(sl);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(2, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");

                // We expect that the last two ranges only are missing from the shards.
                var expectedLocations = new List<MappingLocation>()
                {
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapAndShard,
                    MappingLocation.MappingInShardMapOnly,
                    MappingLocation.MappingInShardMapOnly,
                };

                Assert.IsTrue(expectedLocations.Zip(kvps.Values, (x, y) => x == y).Aggregate((x, y) => x && y), "RebuildRangeShardMap rebuilt the shards out of order with respect to its keeplist.");

                // Rebuild the range, leaving 1 inconsistency
                rm.RebuildMappingsOnShard(g, kvps.Keys.Skip(1));
            }

            gs = rm.DetectMappingDifferences(sl);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(1, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");

                // Rebuild the range, leaving no inconsistencies
                rm.RebuildMappingsOnShard(g, kvps.Keys);
            }

            gs = rm.DetectMappingDifferences(sl);

            // Everything should be semantically consistent now.
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Values.Where(loc => loc != MappingLocation.MappingInShardMapAndShard).Count(), "The count of differences does not match the expected.");

                // As a sanity check, make sure the root is restorable from this LSM.
                rm.ResolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
            }

            gs = rm.DetectMappingDifferences(sl);
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Keys.Count, "The GSM is not restorable from a rebuilt local shard.");
            }
        }

        /// <summary>
        /// Basic sanity checks confirming that pointmappings work the same way rangemappings do in a recover-from-gsm scenario.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestPointMappingRecoverFromGSM()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> listsm = smm.GetListShardMap<int>(RecoveryManagerTests.s_listShardMapName);

            Assert.IsNotNull(listsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);

            Shard s = listsm.CreateShard(sl);

            Assert.IsNotNull(s);

            for (int i = 0; i < 5; i++)
            {
                PointMapping<int> r = listsm.CreatePointMapping(creationInfo: new PointMappingCreationInfo<int>(i, s, MappingStatus.Online));
                Assert.IsNotNull(r);
            }

            // Delete all the ranges and shardmaps from the shardlocation.
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("delete from shard1.__ShardManagement.ShardMappingsLocal", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);
            var gs = rm.DetectMappingDifferences(sl);

            // Validate that all the shard locations are in fact missing from the LSM.
            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(5, kvps.Keys.Count, "The count of differences does not match the expected.");

                foreach (var kvp in kvps)
                {
                    ShardRange range = kvp.Key;
                    MappingLocation mappingLocation = kvp.Value;
                    Assert.AreEqual(MappingLocation.MappingInShardMapOnly, mappingLocation, "An unexpected difference between global and local shardmaps was detected. This is likely a false positive and implies a bug in the detection code.");
                }

                // Recover the LSM from the GSM
                rm.ResolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapMapping);
            }


            gs = rm.DetectMappingDifferences(sl);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Values.Count(), "The count of differences does not match the expected.");
            }
        }

        /// <summary>
        /// Test geo failover scenario: rename one of the shards and then test detach/attach and consistency
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestGeoFailoverAttach()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> listsm = smm.GetListShardMap<int>(RecoveryManagerTests.s_listShardMapName);

            Assert.IsNotNull(listsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0]);
            ShardLocation slNew = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, RecoveryManagerTests.s_shardedDBs[0] + "_new");

            // deploy LSM version 1.1 at location 'sl' before calling CreateShard() so that createshard will not deploy latest LSM version
            smm.UpgradeLocalStore(sl, new Version(1, 1));

            Shard s = listsm.CreateShard(sl);

            Assert.IsNotNull(s);

            for (int i = 0; i < 5; i++)
            {
                PointMapping<int> r = listsm.CreatePointMapping(creationInfo: new PointMappingCreationInfo<int>(i, s, MappingStatus.Online));
                Assert.IsNotNull(r);
            }

            // rename shard1 as shard1_new
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("alter database shard1 set single_user with rollback immediate", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand("alter database shard1 modify name = shard1_new", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand("alter database shard1_new set multi_user with rollback immediate", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            RecoveryManager rm = new RecoveryManager(smm);

            rm.DetachShard(sl);

            rm.AttachShard(slNew);

            // Verify that shard location in LSM is updated to show databasename as 'shard1_new'

            IStoreResults result;

            using (IStoreOperationLocal op = smm.StoreOperationFactory.CreateGetShardsLocalOperation(
                smm,
                slNew,
                "RecoveryTest"))
            {
                result = op.Do();
            }

            Assert.AreEqual("shard1_new", result.StoreShards.First().Location.Database);

            // detect mapping differences and add local mappings to GSM
            var gs = rm.DetectMappingDifferences(slNew);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(5, kvps.Keys.Count, "Count of Mapping differences for shard1_new does not match expected value.");
                rm.ResolveMappingDifferences(g, MappingDifferenceResolution.KeepShardMapping);
            }

            gs = rm.DetectMappingDifferences(slNew);

            foreach (RecoveryToken g in gs)
            {
                var kvps = rm.GetMappingDifferences(g);
                Assert.AreEqual(0, kvps.Keys.Count, "GSM and LSM at shard1_new do not have consistent mappings");
            }

            // rename shard1_new back to shard1 so that test cleanup operations will succeed
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("alter database shard1_new set single_user with rollback immediate", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand("alter database shard1_new modify name = shard1", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand("alter database shard1 set multi_user with rollback immediate", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
