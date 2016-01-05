// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    [TestClass]
    public class ShardMapManagerUpgradeTests
    {
        /// <summary>
        /// Sharded databases to create for the tests.
        /// </summary>
        private static string[] s_shardedDBs = new[]
        {
            "shard1" + Globals.TestDatabasePostfix, "shard2" + Globals.TestDatabasePostfix, "shard3" + Globals.TestDatabasePostfix
        };


        /// <summary>
        /// Shard maps to create for the tests.
        /// </summary>
        private static string[] s_shardMapNames = new[]
        {
            "shardMap1", "shardMap2"
        };

        /// <summary>
        /// GSM version to deploy initially as part of class constructor.
        /// </summary>
        private static Version s_initialGsmVersion = new Version(1, 0);

        /// <summary>
        /// initial LSM version to deploy.
        /// </summary>
        private static Version s_initialLsmVersion = new Version(1, 0);

        #region Common Methods

        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void ShardMapManagerUpgradeTestsInitialize(TestContext testContext)
        {
        }

        /// <summary>
        /// Cleans up common state for the all tests in this class.
        /// </summary>
        [ClassCleanup()]
        public static void ShardMapManagerUpgradeTestsCleanup()
        {
        }

        /// <summary>
        /// Initializes common state per-test.
        /// </summary>
        [TestInitialize()]
        public void ShardMapManagerUpgradeTestInitialize()
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                // Create ShardMapManager database.
                using (SqlCommand cmd = new SqlCommand(
                    string.Format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                    conn))
                {
                    cmd.ExecuteNonQuery();
                }

                // Create shard databases.
                for (int i = 0; i < ShardMapManagerUpgradeTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ShardMapManagerUpgradeTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ShardMapManagerUpgradeTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            // Create shard map manager.
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting,
                s_initialGsmVersion);
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        [TestCleanup()]
        public void ShardMapManagerUpgradeTestCleanup()
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();
                // Drop shard databases
                for (int i = 0; i < ShardMapManagerUpgradeTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ShardMapManagerUpgradeTests.s_shardedDBs[i]),
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

        #endregion Common Methods


        /// <summary>
        /// Get distinct location from shard map manager.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void GetDistinctLocations()
        {
            // Get shard map manager and 2 shard maps.
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            // Upgrade GSM to latest version
            smm.UpgradeGlobalStore();

            // create shard maps
            foreach (string name in ShardMapManagerUpgradeTests.s_shardMapNames)
            {
                ShardMap sm = smm.CreateListShardMap<int>(name);
                Assert.IsNotNull(sm);
            }

            ShardMap sm1 = smm.GetShardMap(ShardMapManagerUpgradeTests.s_shardMapNames[0]);
            Assert.IsNotNull(sm1);

            ShardMap sm2 = smm.GetShardMap(ShardMapManagerUpgradeTests.s_shardMapNames[1]);
            Assert.IsNotNull(sm2);

            // Add shards to the shard maps.

            ShardLocation sl1 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapManagerUpgradeTests.s_shardedDBs[0]);
            ShardLocation sl2 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapManagerUpgradeTests.s_shardedDBs[1]);
            ShardLocation sl3 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapManagerUpgradeTests.s_shardedDBs[2]);

            Shard s1 = sm1.CreateShard(sl1);
            Shard s2 = sm1.CreateShard(sl2);
            Shard s3 = sm1.CreateShard(sl3);
            Shard s4 = sm2.CreateShard(sl2);
            Shard s5 = sm2.CreateShard(sl3);

            int count = 0;

            foreach (ShardLocation sl in smm.GetDistinctShardLocations())
            {
                count++;
            }

            Assert.AreEqual(3, count);
        }

        /// <summary>
        /// Upgrade GSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpgradeGsm()
        {
            // Get shard map manager
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            // Sanity check setup: version should be 1.0
            VerifyGlobalStore(smm, new Version(1, 0));

            // Upgrade to version 1.0: no-op
            smm.UpgradeGlobalStore(new Version(1, 0));
            VerifyGlobalStore(smm, new Version(1, 0));

            // Upgrade to version 1.1
            smm.UpgradeGlobalStore(new Version(1, 1));
            VerifyGlobalStore(smm, new Version(1, 1));

            // Upgrade to version 1.2
            smm.UpgradeGlobalStore(new Version(1, 2));
            VerifyGlobalStore(smm, new Version(1, 2));

            // Upgrade to latest version
            smm.UpgradeGlobalStore();
            VerifyGlobalStore(smm, GlobalConstants.GsmVersionClient);
        }

        /// <summary>
        /// Upgrade LSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpgradeLsm()
        {
            // Get shard map manager
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            // upgrade GSM to latest version.
            smm.UpgradeGlobalStore();
            VerifyGlobalStore(smm, GlobalConstants.GsmVersionClient);

            // deploy LSM initial version.
            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, s_shardedDBs[0]);
            smm.UpgradeLocalStore(sl, s_initialLsmVersion);

            // upgrade to version 1.1
            smm.UpgradeLocalStore(sl, new Version(1, 1));

            // Library is still at LSM major version 1, so adding shard with LSM 1.0 should succeed.
            // Library will see that LSM schema already exists at 'sl' and hence will not deploy LSM again, will just try to add the shard.
            // CreateShard will not work with LSM initial version (1.0) as it only has 'StoreVersion' column in ShardMApManagerLocal table, from 1.1 onwards it follows latest schema for version table.
            ListShardMap<int> listsm = smm.CreateListShardMap<int>(ShardMapManagerUpgradeTests.s_shardMapNames[0]);

            listsm.CreateShard(sl);


            // upgrade to version 1.2
            smm.UpgradeLocalStore(sl, new Version(1, 2));

            // upgrade to latest version (1.2): no-op
            smm.UpgradeLocalStore(sl);
        }

        /// <summary>
        /// Test locking issue with version 1.1 and its fix in version 1.2
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestLockingFixInVersion1_2()
        {
            // Get shard map manager
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            // Upgrade to version 1.1
            smm.UpgradeGlobalStore(new Version(1, 1));

            // Create a range shard map and add few mappings
            RangeShardMap<int> rsm = smm.CreateRangeShardMap<int>(ShardMapManagerUpgradeTests.s_shardMapNames[1]);
            Assert.IsNotNull(rsm);

            Shard s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapManagerUpgradeTests.s_shardedDBs[0]));
            Assert.IsNotNull(s);

            RangeMapping<int> m1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
            RangeMapping<int> m2 = rsm.CreateRangeMapping(new Range<int>(10, 20), s);
            RangeMapping<int> m3 = rsm.CreateRangeMapping(new Range<int>(20, 30), s);

            // Lock first 2 mappings with same lockownerid and third with a different lock owner id

            MappingLockToken t1 = MappingLockToken.Create();
            MappingLockToken t2 = MappingLockToken.Create();

            rsm.LockMapping(m1, t1);
            rsm.LockMapping(m2, t1);
            rsm.LockMapping(m3, t2);

            // now try to unlock using token t2. In store version 1.1 it will unlock all mappings
            rsm.UnlockMapping(t2);

            foreach (RangeMapping<int> m in rsm.GetMappings())
            {
                Assert.AreEqual(MappingLockToken.NoLock, rsm.GetMappingLockOwner(m));
            }

            // Now upgrade to version 1.2 and try same scenario above.
            smm.UpgradeGlobalStore(new Version(1, 2));

            rsm.LockMapping(m1, t1);
            rsm.LockMapping(m2, t1);
            rsm.LockMapping(m3, t2);

            // Unlock using token t1. It should just unlock 2 mappings and leave last one locked.
            rsm.UnlockMapping(t1);

            Assert.AreEqual(MappingLockToken.NoLock, rsm.GetMappingLockOwner(rsm.GetMappingForKey(5)));
            Assert.AreEqual(MappingLockToken.NoLock, rsm.GetMappingLockOwner(rsm.GetMappingForKey(15)));
            Assert.AreEqual(t2, rsm.GetMappingLockOwner(rsm.GetMappingForKey(25)));

            // Cleanup - Delete all mappings. shard will be removed in test cleanup.
            rsm.UnlockMapping(t2);
            RangeMappingUpdate ru = new RangeMappingUpdate();
            ru.Status = MappingStatus.Offline;

            foreach (RangeMapping<int> m in rsm.GetMappings())
            {
                rsm.DeleteMapping(rsm.UpdateMapping(m, ru));
            }
        }

        private void VerifyGlobalStore(ShardMapManager smm, Version targetVersion)
        {
            // Verify upgrade
            Assert.AreEqual(
                targetVersion,
                GetGlobalStoreVersion());

            string shardMapName = string.Format("MyShardMap_{0}", Guid.NewGuid());
            if (targetVersion != null && targetVersion < new Version(1, 1))
            {
                ShardManagementException sme = AssertExtensions.AssertThrows<ShardManagementException>(
                    () => smm.CreateListShardMap<int>(shardMapName));
                Assert.AreEqual(ShardManagementErrorCode.GlobalStoreVersionMismatch, sme.ErrorCode);
            }
            else
            {
                // Below call should succeed as latest supported major version of library matches major version of deployed store.
                ShardMap sm = smm.CreateListShardMap<int>(shardMapName);
                Assert.IsNotNull(sm);
            }
        }

        private Version GetGlobalStoreVersion()
        {
            return GetVersion(SqlUtils.CheckIfExistsGlobalScript.Single().ToString());
        }

        private Version GetVersion(string getVersionScript)
        {
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerConnectionString))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand(getVersionScript, conn);
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    if (reader.FieldCount == 2)
                    {
                        return new Version(reader.GetInt32(1), 0);
                    }
                    else if (reader.FieldCount == 3)
                    {
                        return new Version(reader.GetInt32(1), reader.GetInt32(2));
                    }
                    else
                    {
                        throw new AssertFailedException(String.Format("Unexpected FieldCount: {0}", reader.FieldCount));
                    }
                }
            }
        }
    }
}
