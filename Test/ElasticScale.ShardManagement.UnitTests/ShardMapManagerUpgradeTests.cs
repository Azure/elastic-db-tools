// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

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
        public void UpgradeGSM()
        {
            // Get shard map manager
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            try
            {
                ShardMap testsm = smm.CreateListShardMap<int>(ShardMapManagerUpgradeTests.s_shardMapNames[0]);
                Assert.IsNotNull(testsm);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCode.GlobalStoreVersionMismatch, sme.ErrorCode);
            }

            // Upgrade to version 1.0: no-op
            smm.UpgradeGlobalStore(new Version(1, 0));

            // Upgrade to version 1.1
            smm.UpgradeGlobalStore(new Version(1, 1));

            // Below call should succeed as latest supported major version of library matches major version of deployed store.
            ShardMap sm = smm.CreateListShardMap<int>(ShardMapManagerUpgradeTests.s_shardMapNames[0]);
            Assert.IsNotNull(sm);

            // Upgrade to version 1.2
            smm.UpgradeGlobalStore(new Version(1, 2));

            // Upgrade to latest version (1.2): no-op
            smm.UpgradeGlobalStore();
        }

        /// <summary>
        /// Upgrade LSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpgradeLSM()
        {
            // Get shard map manager
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            // upgrade GSM to latest version.
            smm.UpgradeGlobalStore();

            // deploy LSM initial version.
            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapManagerUpgradeTests.s_shardedDBs[0]);

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

        [TestMethod]
        public void TestSerializationFixInVersion1_3()
        {
            // Get shard map manager
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            // Store some SchemaInfo in the original version
            const string shardMapName1 = "MyShardMap1";
            const string shardMapName2 = "MyShardMap2";
            SchemaInfo expectedSchemaInfo1;
            SchemaInfo expectedSchemaInfo2;

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerConnectionString))
            {
                conn.Open();
                SqlCommand cmd = conn.CreateCommand();

                // Manually store some SchemaInfo that has the correct metadata created by EDCL v1.0.0
                cmd.CommandText = @"insert into __ShardManagement.ShardedDatabaseSchemaInfosGlobal values ('" + shardMapName1 + @"', ' <Schema xmlns:i=""http://www.w3.org/2001/XMLSchema-instance"">
  <ReferenceTableSet i:type=""ArrayOfReferenceTableInfo"">
    <ReferenceTableInfo>
      <SchemaName>r1</SchemaName>
      <TableName>r2</TableName>
    </ReferenceTableInfo>
  </ReferenceTableSet>
  <ShardedTableSet i:type=""ArrayOfShardedTableInfo"">
    <ShardedTableInfo>
      <SchemaName>s1</SchemaName>
      <TableName>s2</TableName>
      <KeyColumnName>s3</KeyColumnName>
    </ShardedTableInfo>
  </ShardedTableSet>
</Schema>')";
                cmd.ExecuteNonQuery();
                expectedSchemaInfo1 = new SchemaInfo();
                expectedSchemaInfo1.Add(new ReferenceTableInfo("r1", "r2"));
                expectedSchemaInfo1.Add(new ShardedTableInfo("s1", "s2", "s3"));

                // Manually store some SchemaInfo that has the incorrect metadata created by EDCL v1.1.0 (GSM v1.2)
                // We have some goofy values to sanity check that later we only rename the tags, not the data inside
                cmd.CommandText = @"insert into __ShardManagement.ShardedDatabaseSchemaInfosGlobal values ('" + shardMapName2 + @"', '<Schema xmlns:i=""http://www.w3.org/2001/XMLSchema-instance"">
  <_referenceTableSet i:type=""ArrayOfReferenceTableInfo"">
    <ReferenceTableInfo>
      <SchemaName>&lt;_referenceTableSet&gt;r1</SchemaName>
      <TableName>r2</TableName>
    </ReferenceTableInfo>
  </_referenceTableSet>
  <_shardedTableSet i:type=""ArrayOfShardedTableInfo"">
    <ShardedTableInfo>
      <SchemaName>&lt;/_shardedTableSet&gt;s1</SchemaName>
      <TableName>s2</TableName>
      <KeyColumnName>s3</KeyColumnName>
    </ShardedTableInfo>
  </_shardedTableSet>
</Schema>')";
                cmd.ExecuteNonQuery();
                expectedSchemaInfo2 = new SchemaInfo();
                expectedSchemaInfo2.Add(new ReferenceTableInfo("<_referenceTableSet>r1", "r2"));
                expectedSchemaInfo2.Add(new ShardedTableInfo("</_shardedTableSet>s1", "s2", "s3"));
            }

            // Upgrade to GSM v1.3 (EDCL v1.1.1)
            smm.UpgradeGlobalStore(new Version(1, 3));

            // Verify SchemaInfos are now read correctly
            SchemaInfoCollection schemaInfoCollectionFinal = smm.GetSchemaInfoCollection();
            AssertEqual(expectedSchemaInfo1, schemaInfoCollectionFinal.Get(shardMapName1));
            AssertEqual(expectedSchemaInfo2, schemaInfoCollectionFinal.Get(shardMapName2));
        }

        private void AssertEqual(SchemaInfo x, SchemaInfo y)
        {
            AssertExtensions.AssertSequenceEquivalent(x.ReferenceTables, y.ReferenceTables);
            AssertExtensions.AssertSequenceEquivalent(x.ShardedTables, y.ShardedTables);
        }

        public class ShardedTableInfoEqualityComparer : IEqualityComparer<ShardedTableInfo>
        {
            public bool Equals(ShardedTableInfo x, ShardedTableInfo y)
            {
                return x.SchemaName == y.SchemaName
                    && x.TableName == y.TableName
                    && x.KeyColumnName == y.KeyColumnName;
            }

            public int GetHashCode(ShardedTableInfo obj)
            {
                return obj.TableName.GetHashCode();
            }
        }

        public class ReferenceTableInfoEqualityComparer : IEqualityComparer<ReferenceTableInfo>
        {
            public bool Equals(ReferenceTableInfo x, ReferenceTableInfo y)
            {
                return x.SchemaName == y.SchemaName
                    && x.TableName == y.TableName;
            }

            public int GetHashCode(ReferenceTableInfo obj)
            {
                return obj.TableName.GetHashCode();
            }
        }
    }
}
