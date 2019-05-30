// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Test related to ShardMapper class and it's methods.
    /// </summary>
    [TestClass]
    public class ShardMapperTests
    {
        /// <summary>
        /// Sharded databases to create for the test.
        /// </summary>
        private static string[] s_shardedDBs = new[]
        {
            "shard1" + Globals.TestDatabasePostfix, "shard2" + Globals.TestDatabasePostfix
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
        /// Helper function to clean list and range shard maps.
        /// </summary>
        private static void CleanShardMapsHelper()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            // Remove all existing mappings from the list shard map.
            ListShardMap<int> lsm;
            if (smm.TryGetListShardMap<int>(ShardMapperTests.s_listShardMapName, out lsm))
            {
                Assert.IsNotNull(lsm);

                foreach (PointMapping<int> pm in lsm.GetMappings())
                {
                    PointMapping<int> pmOffline = lsm.MarkMappingOffline(pm);
                    Assert.IsNotNull(pmOffline);
                    lsm.DeleteMapping(pmOffline);
                }

                // Remove all shards from list shard map
                foreach (Shard s in lsm.GetShards())
                {
                    lsm.DeleteShard(s);
                }
            }

            // Remove all existing mappings from the range shard map.
            RangeShardMap<int> rsm;
            if (smm.TryGetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName, out rsm))
            {
                Assert.IsNotNull(rsm);

                foreach (RangeMapping<int> rm in rsm.GetMappings())
                {
                    MappingLockToken mappingLockToken = rsm.GetMappingLockOwner(rm);
                    rsm.UnlockMapping(rm, mappingLockToken);
                    RangeMapping<int> rmOffline = rsm.MarkMappingOffline(rm);
                    Assert.IsNotNull(rmOffline);
                    rsm.DeleteMapping(rmOffline);
                }

                // Remove all shards from range shard map
                foreach (Shard s in rsm.GetShards())
                {
                    rsm.DeleteShard(s);
                }
            }
        }

        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void ShardMapperTestsInitialize(TestContext testContext)
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
                for (int i = 0; i < ShardMapperTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ShardMapperTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ShardMapperTests.s_shardedDBs[i]),
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

            // Create list shard map.
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.CreateListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Assert.AreEqual(ShardMapperTests.s_listShardMapName, lsm.Name);

            // Create range shard map.
            RangeShardMap<int> rsm = smm.CreateRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Assert.AreEqual(ShardMapperTests.s_rangeShardMapName, rsm.Name);
        }

        /// <summary>
        /// Cleans up common state for the all tests in this class.
        /// </summary>
        [ClassCleanup()]
        public static void ShardMapperTestsCleanup()
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();
                // Drop shard databases
                for (int i = 0; i < ShardMapperTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ShardMapperTests.s_shardedDBs[i]),
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
            ShardMapperTests.CleanShardMapsHelper();
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        [TestCleanup()]
        public void ShardMapperTestCleanup()
        {
            ShardMapperTests.CleanShardMapsHelper();
        }

        #endregion Common Methods

        /// <summary>
        /// Shard map type conversion between list and range.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void ShardMapTypeFailures()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            // Try to get list<int> shard map as range<int>
            try
            {
                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_listShardMapName);
                Assert.Fail("GetRangeshardMap did not throw as expected");
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardMapTypeConversionError, sme.ErrorCode);
            }

            // Try to get range<int> shard map as list<int>
            try
            {
                ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_rangeShardMapName);
                Assert.Fail("GetListShardMap did not throw as expected");
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardMapTypeConversionError, sme.ErrorCode);
            }

            // Try to get list<int> shard map as list<guid>
            try
            {
                ListShardMap<Guid> lsm = smm.GetListShardMap<Guid>(ShardMapperTests.s_listShardMapName);
                Assert.Fail("GetListShardMap did not throw as expected");
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardMapTypeConversionError, sme.ErrorCode);
            }

            // Try to get range<int> shard map as range<long>
            try
            {
                RangeShardMap<long> rsm = smm.GetRangeShardMap<long>(ShardMapperTests.s_rangeShardMapName);
                Assert.Fail("GetRangeshardMap did not throw as expected");
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardMapTypeConversionError, sme.ErrorCode);
            }

            // Try to get range<int> shard map as list<guid>
            try
            {
                ListShardMap<Guid> lsm = smm.GetListShardMap<Guid>(ShardMapperTests.s_rangeShardMapName);
                Assert.Fail("GetListShardMap did not throw as expected");
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardMapTypeConversionError, sme.ErrorCode);
            }
        }

        #region ListMapperTests

        /// <summary>
        /// Add a point mapping to list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddPointMappingDefault()
        {
            AddPointMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<int>());
            AddPointMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<long>());
            AddPointMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<Guid>());
            AddPointMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<byte[]>());
            AddPointMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<DateTime>());
            AddPointMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<DateTimeOffset>());
            AddPointMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<TimeSpan>());
        }

        private void AddPointMappingDefault<T>(IEnumerable<T> keysToTest)
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<T> lsm = smm.CreateListShardMap<T>(string.Format("AddPointMappingDefault_{0}", typeof(T).Name));
            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);
            Shard s = lsm.CreateShard(sl);
            Assert.IsNotNull(s);

            foreach (T key in keysToTest)
            {
                Console.WriteLine("Key: {0}", key);

                PointMapping<T> p1 = lsm.CreatePointMapping(key, s);
                Assert.IsNotNull(p1);
                AssertExtensions.AssertScalarOrSequenceEqual(key, p1.Value);

                PointMapping<T> p2 = lsm.GetMappingForKey(key);
                Assert.IsNotNull(p2);
                AssertExtensions.AssertScalarOrSequenceEqual(key, p2.Value);

                Assert.AreEqual(0, countingCache.LookupMappingCount);
                Assert.AreEqual(0, countingCache.LookupMappingHitCount);

                // Validate mapping by trying to connect
                using (SqlConnection conn = lsm.OpenConnection(
                    p1,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
        }

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void GetPointMappingForKey_Ex()
        {
            GetPointMappingForKey(new ExceptionBasedTestShardMapVerifier());
        }

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void GetPointMappingForKey_Try()
        {
            GetPointMappingForKey(new TryBasedTestShardMapVerifier());
        }

        private void GetPointMappingForKey(TestShardMapVerifier verifier)
        {
            // Use 2 different SMM's so that we can verify when cache is used
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMapManager smm2 = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            TestShardMap<int> lsm = new TestShardMap<int>(smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName));
            TestShardMap<int> lsm2 = new TestShardMap<int>(smm2.GetListShardMap<int>(ShardMapperTests.s_listShardMapName));

            GetMappingForKey(lsm, lsm2, verifier);
        }

        /// <summary>
        /// All combinations of getting point mappings from a list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void GetPointMappingsForRange()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Shard s1 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s1);
            Assert.IsNotNull(p1);

            PointMapping<int> p2 = lsm.CreatePointMapping(10, s1);
            Assert.IsNotNull(p2);

            PointMapping<int> p3 = lsm.CreatePointMapping(5, s2);
            Assert.IsNotNull(p2);

            // Get all mappings in shard map.
            int count = 0;
            IEnumerable<PointMapping<int>> allMappings = lsm.GetMappings();
            using (IEnumerator<PointMapping<int>> mEnum = allMappings.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(3, count);

            // Get all mappings in specified range.
            count = 0;
            IEnumerable<PointMapping<int>> mappingsInRange = lsm.GetMappings(new Range<int>(5, 15));
            using (IEnumerator<PointMapping<int>> mEnum = mappingsInRange.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(2, count);

            // Get all mappings for a shard.
            count = 0;
            IEnumerable<PointMapping<int>> mappingsForShard = lsm.GetMappings(s1);
            using (IEnumerator<PointMapping<int>> mEnum = mappingsForShard.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(2, count);

            // Get all mappings in specified range for a particular shard.
            count = 0;
            IEnumerable<PointMapping<int>> mappingsInRangeForShard = lsm.GetMappings(new Range<int>(5, 15), s1);
            using (IEnumerator<PointMapping<int>> mEnum = mappingsInRangeForShard.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(1, count);
        }

        /// <summary>
        /// Add a duplicate point mapping to list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddPointMappingDuplicate()
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            Assert.IsNotNull(p1);

            bool addFailed = false;
            try
            {
                // add same point mapping again.
                PointMapping<int> pNew = lsm.CreatePointMapping(1, s);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingPointAlreadyMapped, sme.ErrorCode);
                addFailed = true;
            }

            Assert.IsTrue(addFailed);

            PointMapping<int> p2 = lsm.GetMappingForKey(1);

            Assert.IsNotNull(p2);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);
        }

        /// <summary>
        /// Delete existing point mapping from list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeletePointMappingDefault()
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            PointMapping<int> p2 = lsm.GetMappingForKey(1);

            Assert.IsNotNull(p2);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);

            // The mapping must be made offline first before it can be deleted.
            PointMappingUpdate ru = new PointMappingUpdate();
            ru.Status = MappingStatus.Offline;

            PointMapping<int> mappingToDelete = lsm.UpdateMapping(p1, ru);

            lsm.DeleteMapping(mappingToDelete);

            // Try to get from store. Because the mapping is missing from the store, we will try to
            // invalidate the cache, but since it is also missing from cache there will be an cache miss.
            bool lookupFailed = false;
            try
            {
                PointMapping<int> pLookup = lsm.GetMappingForKey(1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingNotFoundForKey, sme.ErrorCode);
                lookupFailed = true;
            }

            Assert.IsTrue(lookupFailed);
            Assert.AreEqual(1, countingCache.LookupMappingMissCount);
        }

        /// <summary>
        /// Delete non-existing point mapping from list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeletePointMappingNonExisting()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            Assert.IsNotNull(p1);

            PointMappingUpdate ru = new PointMappingUpdate();
            ru.Status = MappingStatus.Offline;

            // The mapping must be made offline before it can be deleted.
            p1 = lsm.UpdateMapping(p1, ru);

            lsm.DeleteMapping(p1);

            bool removeFailed = false;

            try
            {
                lsm.DeleteMapping(p1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingDoesNotExist, sme.ErrorCode);
                removeFailed = true;
            }

            Assert.IsTrue(removeFailed);
        }

        /// <summary>
        /// Delete point mapping with version mismatch from list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeletePointMappingVersionMismatch()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            Assert.IsNotNull(p1);

            PointMappingUpdate pu = new PointMappingUpdate();
            pu.Status = MappingStatus.Offline;

            PointMapping<int> pNew = lsm.UpdateMapping(p1, pu);
            Assert.IsNotNull(pNew);

            bool removeFailed = false;

            try
            {
                lsm.DeleteMapping(p1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingDoesNotExist, sme.ErrorCode);
                removeFailed = true;
            }

            Assert.IsTrue(removeFailed);
        }

        /// <summary>
        /// Update existing point mapping in list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdatePointMappingDefault()
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            PointMappingUpdate pu = new PointMappingUpdate();
            pu.Status = MappingStatus.Offline;

            PointMapping<int> pNew = lsm.UpdateMapping(p1, pu);
            Assert.IsNotNull(pNew);

            PointMapping<int> p2 = lsm.GetMappingForKey(1);

            Assert.IsNotNull(p2);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);

            // Mark the mapping online again so that it will be cleaned up
            pu.Status = MappingStatus.Online;
            PointMapping<int> pUpdated = lsm.UpdateMapping(pNew, pu);
            Assert.IsNotNull(pUpdated);
        }

        /// <summary>
        /// Take a mapping offline, verify that the existing connection is killed.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void KillConnectionOnOfflinePointMapping()
        {
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            using (SqlConnection conn = lsm.OpenConnectionForKeyAsync(1, Globals.ShardUserConnectionString).Result)
            {
                Assert.AreEqual(ConnectionState.Open, conn.State);

                PointMappingUpdate pu = new PointMappingUpdate();
                pu.Status = MappingStatus.Offline;

                PointMapping<int> pNew = lsm.UpdateMapping(p1, pu);
                Assert.IsNotNull(pNew);

                bool failed = false;

                try
                {
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1";
                        cmd.CommandType = CommandType.Text;

                        using (SqlDataReader rdr = cmd.ExecuteReader())
                        {
                        }
                    }
                }
                catch (SqlException)
                {
                    failed = true;
                }

                Assert.AreEqual(true, failed);
                Assert.AreEqual(ConnectionState.Closed, conn.State);

                failed = false;

                // Open 2nd connection.
                try
                {
                    using (SqlConnection conn2 = lsm.OpenConnectionForKeyAsync(1, Globals.ShardUserConnectionString).Result)
                    {
                    }
                }
                catch (AggregateException ex)
                {
                    var sme = ex.InnerException as ShardManagementException;
                    if (sme != null)
                    {
                        failed = true;
                        Assert.AreEqual(ShardManagementErrorCode.MappingIsOffline, sme.ErrorCode);
                    }
                }

                Assert.AreEqual(true, failed);

                // Mark the mapping online again so that it will be cleaned up
                pu.Status = MappingStatus.Online;
                PointMapping<int> pUpdated = lsm.UpdateMapping(pNew, pu);
                Assert.IsNotNull(pUpdated);

                failed = false;

                // Open 3rd connection. This should succeed.
                try
                {
                    using (SqlConnection conn3 = lsm.OpenConnectionForKey(1, Globals.ShardUserConnectionString))
                    {
                    }
                }
                catch (ShardManagementException)
                {
                    failed = true;
                }

                Assert.AreEqual(false, failed);
            }
        }

        /// <summary>
        /// Take a mapping offline, verify that the existing connection is not killed using mapping options.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DoNotKillConnectionOnOfflinePointMapping()
        {
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            ValidateConnectionPreservedOnOfflineMapping(lsm, p1);
        }

        /// <summary>
        /// Update location of existing point mapping in list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdatePointMappingLocation()
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Shard s1 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s1);

            PointMappingUpdate pu = new PointMappingUpdate();
            pu.Status = MappingStatus.Offline; // Shard location in a mapping cannot be changed unless it is offline.

            PointMapping<int> pOffline = lsm.UpdateMapping(p1, pu);

            Assert.IsNotNull(pOffline);
            Assert.AreEqual(pu.Status, pOffline.Status);
            pu.Shard = s2;

            PointMapping<int> pNew = lsm.UpdateMapping(pOffline, pu);
            Assert.IsNotNull(pNew);

            PointMapping<int> p2 = lsm.GetMappingForKey(1);

            Assert.IsNotNull(p2);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);
            Assert.AreEqual(s2.Id, p2.Shard.Id);
        }

        /// <summary>
        /// Update location of existing point mapping in list shard map with idemptency checks
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdatePointMappingIdempotency()
        {
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Shard s1 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s1);

            // Online -> Offline - No Location Change
            PointMappingUpdate pu = new PointMappingUpdate
            {
                Status = MappingStatus.Offline
            };

            PointMapping<int> presult = lsm.UpdateMapping(p1, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Offline);

            // Offline -> Offline - No Location Change
            pu = new PointMappingUpdate
            {
                Status = MappingStatus.Offline
            };

            presult = lsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Offline);

            // Offline -> Offline - Location Change
            pu = new PointMappingUpdate
            {
                Shard = s2
            };

            presult = lsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Offline);
            Assert.AreEqual(s2.Location, presult.Shard.Location);

            // Offline -> Online - No Location Change
            pu = new PointMappingUpdate
            {
                Status = MappingStatus.Online
            };

            presult = lsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Online);

            // Online -> Offline - Location Change
            pu = new PointMappingUpdate
            {
                Status = MappingStatus.Offline,
                Shard = s1
            };

            presult = lsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Offline);
            Assert.AreEqual(s1.Location, presult.Shard.Location);

            // Offline -> Online - Location Change
            pu = new PointMappingUpdate
            {
                Status = MappingStatus.Online,
                Shard = s2
            };

            presult = lsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Online);
            Assert.AreEqual(s2.Location, presult.Shard.Location);

            // Online -> Online - No Location Change
            pu = new PointMappingUpdate
            {
                Status = MappingStatus.Online
            };

            presult = lsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Online);

            // Online -> Online - Location Change
            pu = new PointMappingUpdate
            {
                Shard = s1
            };

            bool failed = false;

            try
            {
                presult = lsm.UpdateMapping(presult, pu);
            }
            catch (ShardManagementException sme)
            {
                failed = true;
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingIsNotOffline, sme.ErrorCode);
            }

            Assert.IsTrue(failed);
        }

        #endregion ListMapperTests

        #region RangeMapperTests

        /// <summary>
        /// Add a range mapping to range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddRangeMappingDefault()
        {
            AddRangeMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<int>().ToArray());
            AddRangeMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<long>().ToArray());
            AddRangeMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<Guid>().ToArray());
            AddRangeMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<byte[]>().ToArray());
            AddRangeMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<DateTime>().ToArray());
            AddRangeMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<DateTimeOffset>().ToArray());
            AddRangeMappingDefault(ShardKeyInfo.AllTestShardKeyValues.OfType<TimeSpan>().ToArray());
        }

        private void AddRangeMappingDefault<T>(IList<T> keysToTest)
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<T> rsm = smm.CreateRangeShardMap<T>(string.Format("AddRangeMappingDefault_{0}", typeof(T).Name));
            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);
            Shard s = rsm.CreateShard(sl);
            Assert.IsNotNull(s);

            for (int i = 0; i < keysToTest.Count - 1; i++)
            {
                // https://github.com/Azure/elastic-db-tools/issues/117
                // Bug? DateTimeOffsets with the same universal time but different offset are equal as ShardKeys.
                // According to SQL (and our normalization format), they should be unequal, although according to .NET they should be equal.
                // We need to skip empty ranges because if we use them in this test then we end up with duplicate mappings
                if (typeof(T) == typeof(DateTimeOffset) && (DateTimeOffset)(object)keysToTest[i] == (DateTimeOffset)(object)keysToTest[i + 1])
                {
                    Console.WriteLine("Skipping {0} == {1}", keysToTest[i], keysToTest[i + 1]);
                    continue;
                }

                Range<T> range = new Range<T>(keysToTest[i], keysToTest[i + 1]);
                Console.WriteLine("Range: {0}", range);

                RangeMapping<T> p1 = rsm.CreateRangeMapping(range, s);
                Assert.IsNotNull(p1);
                AssertExtensions.AssertScalarOrSequenceEqual(range, p1.Value);

                RangeMapping<T> p2 = rsm.GetMappingForKey(range.Low);
                Assert.IsNotNull(p2);
                AssertExtensions.AssertScalarOrSequenceEqual(range, p2.Value);

                Assert.AreEqual(0, countingCache.LookupMappingCount);
                Assert.AreEqual(0, countingCache.LookupMappingHitCount);

                // Validate mapping by trying to connect
                using (SqlConnection conn = rsm.OpenConnection(
                    p1,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
        }

        /// <summary>
        /// Add multiple range mapping to range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddRangeMappingMultiple()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(0, 10), s);
            Assert.IsNotNull(r1);

            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(20, 30), s);
            Assert.IsNotNull(r2);

            bool addFailed = false;

            int[,] ranges = new int[6, 2] { { 5, 15 }, { 5, 7 }, { -5, 5 }, { -5, 15 }, { 15, 25 }, { int.MinValue, int.MaxValue } };

            for (int i = 0; i < 6; i++)
            {
                try
                {
                    addFailed = false;
                    RangeMapping<int> r3 = rsm.CreateRangeMapping(new Range<int>(ranges[i, 0], ranges[i, 1]), s);
                }
                catch (ShardManagementException sme)
                {
                    Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                    Assert.AreEqual(ShardManagementErrorCode.MappingRangeAlreadyMapped, sme.ErrorCode);
                    addFailed = true;
                }
                Assert.IsTrue(addFailed);
            }

            RangeMapping<int> r4 = rsm.CreateRangeMapping(new Range<int>(10, 20), s);
            Assert.IsNotNull(r4);
        }

        /// <summary>
        /// Exercise IEquatable for shard objects.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestEquatableForShards()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl1 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            ShardLocation sl2 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]);

            Shard s1 = rsm.CreateShard(sl1);
            Assert.IsNotNull(s1);

            Shard s2 = rsm.CreateShard(sl2);
            Assert.IsNotNull(s2);


            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(0, 10), s1);
            Assert.IsNotNull(r1);

            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(10, 20), s2);
            Assert.IsNotNull(r2);

            RangeMapping<int> r3 = rsm.CreateRangeMapping(new Range<int>(20, 30), s1);
            Assert.IsNotNull(r3);

            Shard sLookup = (rsm.GetMappingForKey(5)).Shard;

            Assert.IsFalse(sLookup.Equals(s1));

            Assert.IsFalse(sLookup.Equals(s2));

            IEnumerable<Shard> myShardSelection = rsm
                .GetMappings(new Range<int>(0, 300))
                .Select(r => r.Shard)
                .Distinct();

            Assert.IsTrue(myShardSelection.Count() == 2);
        }

        /// <summary>
        /// Add a range mapping to cover entire range in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddRangeMappingEntireRange()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(int.MinValue), s);

            Assert.IsNotNull(r1);
        }

        /// <summary>
        /// Add a range mapping to cover entire range in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddRangeMappingTestBoundaries()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            int[,] ranges = new int[3, 2] {
                { int.MinValue, int.MinValue + 1 },
                { int.MinValue + 1, int.MaxValue - 1},
                { int.MaxValue - 1, int.MaxValue}
            };

            for (int i = 0; i < 3; i++)
            {
                RangeMapping<int> r = rsm.CreateRangeMapping(new Range<int>(ranges[i, 0], ranges[i, 1]), s);
                Assert.IsNotNull(r);
            }

            // Add range [2147483647, +inf). This range is actually representing a single point int.MaxValue
            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(int.MaxValue), s);
            Assert.IsNotNull(r1);
        }

        /// <summary>
        /// Add a duplicate range mapping to range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddRangeMappingDuplicate()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            ShardManagementException exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.CreateRangeMapping(new Range<int>(1, 10), s));

            Assert.IsTrue(
                exception.ErrorCode == ShardManagementErrorCode.MappingRangeAlreadyMapped &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap,
                "Expected MappingRangeAlreadyMapped error!");
        }

        /// <summary>
        /// Delete existing range mapping from range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteRangeMappingDefault()
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            MappingLockToken mappingLockToken = MappingLockToken.Create();
            rsm.LockMapping(r1, mappingLockToken);

            RangeMapping<int> rLookup = rsm.GetMappingForKey(1);

            Assert.IsNotNull(rLookup);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);

            // The mapping must be made offline first before it can be deleted.
            RangeMappingUpdate ru = new RangeMappingUpdate();
            ru.Status = MappingStatus.Offline;

            // Should throw if the correct lock owner id isn't passed
            ShardManagementException exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.UpdateMapping(r1, ru));

            Assert.IsTrue(
                exception.ErrorCode == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap);

            RangeMapping<int> mappingToDelete = rsm.UpdateMapping(r1, ru, mappingLockToken);

            exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.DeleteMapping(mappingToDelete));

            Assert.IsTrue(
                exception.ErrorCode == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap);

            rsm.DeleteMapping(mappingToDelete, mappingLockToken);

            // Try to get from store. Because the mapping is missing from the store, we will try to
            // invalidate the cache, but since it is also missing from cache there will be an cache miss.
            exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.GetMappingForKey(1));

            Assert.IsTrue(
                exception.ErrorCode == ShardManagementErrorCode.MappingNotFoundForKey &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap);

            Assert.AreEqual(1, countingCache.LookupMappingMissCount);
        }

        /// <summary>
        /// Delete non-existing range mapping from range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteRangeMappingNonExisting()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            RangeMappingUpdate ru = new RangeMappingUpdate();
            ru.Status = MappingStatus.Offline;

            // The mapping must be made offline before it can be deleted.
            r1 = rsm.UpdateMapping(r1, ru);

            rsm.DeleteMapping(r1);

            bool removeFailed = false;

            try
            {
                rsm.DeleteMapping(r1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingDoesNotExist, sme.ErrorCode);
                removeFailed = true;
            }

            Assert.IsTrue(removeFailed);
        }

        /// <summary>
        /// Delete range mapping with version mismatch from range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteRangeMappingVersionMismatch()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            RangeMappingUpdate ru = new RangeMappingUpdate();
            ru.Status = MappingStatus.Offline;

            // upate range mapping to change version
            RangeMapping<int> rNew = rsm.UpdateMapping(r1, ru);
            Assert.IsNotNull(rNew);

            bool removeFailed = false;

            try
            {
                rsm.DeleteMapping(r1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingDoesNotExist, sme.ErrorCode);
                removeFailed = true;
            }

            Assert.IsTrue(removeFailed);
        }

        /// <summary>
        /// Update range mapping in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingDefault()
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            // Lock the mapping
            MappingLockToken mappingLockToken = MappingLockToken.Create();
            rsm.LockMapping(r1, mappingLockToken);

            RangeMappingUpdate ru = new RangeMappingUpdate()
            {
                Status = MappingStatus.Offline
            };

            RangeMapping<int> rNew = rsm.UpdateMapping(r1, ru, mappingLockToken);

            Assert.IsNotNull(rNew);

            MappingLockToken storeMappingLockToken = rsm.GetMappingLockOwner(rNew);
            Assert.AreEqual(storeMappingLockToken, mappingLockToken, "LockownerId does not match that in store!");

            rsm.UnlockMapping(rNew, mappingLockToken);
            RangeMapping<int> r2 = rsm.GetMappingForKey(1);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);
            Assert.AreNotEqual(r1.Id, r2.Id);
        }

        /// <summary>
        /// Take a mapping offline, verify that the existing connection is killed.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void KillConnectionOnOfflineRangeMapping()
        {
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            using (SqlConnection conn = rsm.OpenConnectionForKeyAsync(1, Globals.ShardUserConnectionString).Result)
            {
                Assert.AreEqual(ConnectionState.Open, conn.State);

                RangeMappingUpdate ru = new RangeMappingUpdate();
                ru.Status = MappingStatus.Offline;

                RangeMapping<int> rNew = rsm.UpdateMapping(r1, ru);
                Assert.IsNotNull(rNew);

                bool failed = false;

                try
                {
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1";
                        cmd.CommandType = CommandType.Text;

                        using (SqlDataReader rdr = cmd.ExecuteReader())
                        {
                        }
                    }
                }
                catch (SqlException)
                {
                    failed = true;
                }

                Assert.AreEqual(true, failed);
                Assert.AreEqual(ConnectionState.Closed, conn.State);

                failed = false;

                // Open 2nd connection.
                try
                {
                    using (SqlConnection conn2 = rsm.OpenConnectionForKeyAsync(1, Globals.ShardUserConnectionString).Result)
                    {
                    }
                }
                catch (AggregateException ex)
                {
                    var sme = ex.InnerException as ShardManagementException;
                    if (sme != null)
                    {
                        failed = true;
                        Assert.AreEqual(ShardManagementErrorCode.MappingIsOffline, sme.ErrorCode);
                    }
                }

                Assert.AreEqual(true, failed);

                // Mark the mapping online again so that it will be cleaned up
                ru.Status = MappingStatus.Online;
                RangeMapping<int> rUpdated = rsm.UpdateMapping(rNew, ru);
                Assert.IsNotNull(rUpdated);

                failed = false;

                // Open 3rd connection. This should succeed.
                try
                {
                    using (SqlConnection conn3 = rsm.OpenConnectionForKey(1, Globals.ShardUserConnectionString))
                    {
                    }
                }
                catch (ShardManagementException)
                {
                    failed = true;
                }

                Assert.AreEqual(false, failed);
            }
        }

        /// <summary>
        /// Take a mapping offline, verify that the existing connection is not killed using mapping options.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DoNotKillConnectionOnOfflineRangeMapping()
        {
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            ValidateConnectionPreservedOnOfflineMapping(rsm, r1);
        }

        /// <summary>
        /// Update range mapping in range shard map to change location.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingLocation()
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl1 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);
            Shard s1 = rsm.CreateShard(sl1);
            Assert.IsNotNull(s1);

            ShardLocation sl2 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]);
            Shard s2 = rsm.CreateShard(sl2);
            Assert.IsNotNull(s2);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s1);

            RangeMappingUpdate ru = new RangeMappingUpdate();

            // Shard location in a mapping cannot be updated when online.
            ru.Status = MappingStatus.Offline;
            RangeMapping<int> rOffline = rsm.UpdateMapping(r1, ru);

            Assert.IsNotNull(rOffline);
            Assert.AreEqual(ru.Status, rOffline.Status);
            ru.Shard = s2;

            RangeMapping<int> rNew = rsm.UpdateMapping(rOffline, ru);
            Assert.IsNotNull(rNew);

            // Bring the mapping back online.
            ru.Status = MappingStatus.Online;

            rNew = rsm.UpdateMapping(rNew, ru);
            Assert.IsNotNull(rNew);

            RangeMapping<int> r2 = rsm.GetMappingForKey(1);

            Assert.IsNotNull(r2);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);
            Assert.AreEqual(s2.Id, r2.Shard.Id);
        }

        /// <summary>
        /// Update location of existing point mapping in list shard map with idemptency checks
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingIdempotency()
        {
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s1);

            // Online -> Offline - No Location Change
            RangeMappingUpdate pu = new RangeMappingUpdate
            {
                Status = MappingStatus.Offline
            };

            RangeMapping<int> presult = rsm.UpdateMapping(r1, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Offline);

            // Offline -> Offline - No Location Change
            pu = new RangeMappingUpdate
            {
                Status = MappingStatus.Offline
            };

            presult = rsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Offline);

            // Offline -> Offline - Location Change
            pu = new RangeMappingUpdate
            {
                Shard = s2
            };

            presult = rsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Offline);
            Assert.AreEqual(s2.Location, presult.Shard.Location);

            // Offline -> Online - No Location Change
            pu = new RangeMappingUpdate
            {
                Status = MappingStatus.Online
            };

            presult = rsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Online);

            // Online -> Offline - Location Change
            pu = new RangeMappingUpdate
            {
                Status = MappingStatus.Offline,
                Shard = s1
            };

            presult = rsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Offline);
            Assert.AreEqual(s1.Location, presult.Shard.Location);

            // Offline -> Online - Location Change
            pu = new RangeMappingUpdate
            {
                Status = MappingStatus.Online,
                Shard = s2
            };

            presult = rsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Online);
            Assert.AreEqual(s2.Location, presult.Shard.Location);

            // Online -> Online - No Location Change
            pu = new RangeMappingUpdate
            {
                Status = MappingStatus.Online
            };

            presult = rsm.UpdateMapping(presult, pu);
            Assert.IsNotNull(presult);
            Assert.IsTrue(presult.Status == MappingStatus.Online);

            // Online -> Online - Location Change
            pu = new RangeMappingUpdate
            {
                Shard = s1
            };

            bool failed = false;

            try
            {
                presult = rsm.UpdateMapping(presult, pu);
            }
            catch (ShardManagementException sme)
            {
                failed = true;
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingIsNotOffline, sme.ErrorCode);
            }

            Assert.IsTrue(failed);
        }

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void GetRangeMappingForKey_Ex()
        {
            GetRangeMappingForKey(new ExceptionBasedTestShardMapVerifier());
        }

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void GetRangeMappingForKey_Try()
        {
            GetRangeMappingForKey(new TryBasedTestShardMapVerifier());
        }

        private void GetRangeMappingForKey(TestShardMapVerifier verifier)
        {
            // Use 2 different SMM's so that we can verify when cache is used
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMapManager smm2 = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            TestShardMap<int> rsm = new TestShardMap<int>(smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName));
            TestShardMap<int> rsm2 = new TestShardMap<int>(smm2.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName));

            GetMappingForKey(rsm, rsm2, verifier);
        }

        /// <summary>
        /// All combinations of getting range mappings from a range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void GetRangeMappingsForRange()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);
            Assert.IsNotNull(r1);

            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(10, 20), s2);
            Assert.IsNotNull(r2);

            RangeMapping<int> r3 = rsm.CreateRangeMapping(new Range<int>(20, 30), s1);
            Assert.IsNotNull(r3);

            // Get all mappings in shard map.
            int count = 0;
            IEnumerable<RangeMapping<int>> allMappings = rsm.GetMappings();
            using (IEnumerator<RangeMapping<int>> mEnum = allMappings.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(3, count);

            // Get all mappings in specified range.
            count = 0;
            IEnumerable<RangeMapping<int>> mappingsInRange = rsm.GetMappings(new Range<int>(1, 15));
            using (IEnumerator<RangeMapping<int>> mEnum = mappingsInRange.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(2, count);

            // Get all mappings for a shard.
            count = 0;
            IEnumerable<RangeMapping<int>> mappingsForShard = rsm.GetMappings(s1);
            using (IEnumerator<RangeMapping<int>> mEnum = mappingsForShard.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(2, count);

            // Get all mappings in specified range for a particular shard.
            count = 0;
            IEnumerable<RangeMapping<int>> mappingsInRangeForShard = rsm.GetMappings(new Range<int>(1, 15), s1);
            using (IEnumerator<RangeMapping<int>> mEnum = mappingsInRangeForShard.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(1, count);
        }

        /// <summary>
        /// Split existing range mapping in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void SplitRangeDefault()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            // Lock the mapping
            MappingLockToken mappingLockToken = MappingLockToken.Create();
            rsm.LockMapping(r1, mappingLockToken);

            // Should throw if the correct lock owner id isn't passed
            ShardManagementException exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.SplitMapping(r1, 5));
            Assert.IsTrue(exception.ErrorCode == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap,
                "Expected MappingLockOwnerIdDoesNotMatch error when Updating mapping!");

            IReadOnlyList<RangeMapping<int>> rList = rsm.SplitMapping(r1, 5, mappingLockToken);

            Assert.AreEqual(2, rList.Count);

            foreach (RangeMapping<int> r in rList)
            {
                Assert.IsNotNull(r);
                Assert.AreEqual(mappingLockToken, rsm.GetMappingLockOwner(r),
                    "LockOwnerId of mapping: {0} does not match id in store!");

                // Unlock each mapping and verify
                rsm.UnlockMapping(r, mappingLockToken);
                Assert.AreEqual(MappingLockToken.NoLock, rsm.GetMappingLockOwner(r),
                    "Mapping: {0} not unlocked as expected!");
            }
        }

        /// <summary>
        /// Split existing range mapping at boundary in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void SplitRangeBoundary()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            // Lock the mapping
            MappingLockToken mappingLockToken = MappingLockToken.Create();
            rsm.LockMapping(r1, mappingLockToken);

            ArgumentOutOfRangeException exception = AssertExtensions.AssertThrows<ArgumentOutOfRangeException>
                (() => rsm.SplitMapping(r1, 1, mappingLockToken));

            // Unlock mapping
            rsm.UnlockMapping(r1, mappingLockToken);
        }

        /// <summary>
        /// Split a range at point outside range in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void SplitRangeOutside()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            ArgumentOutOfRangeException exception = AssertExtensions.AssertThrows<ArgumentOutOfRangeException>
                (() => rsm.SplitMapping(r1, 31));
        }

        /// <summary>
        /// Merge adjacent range mappings in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void MergeRangeMappingsDefault()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);
            MappingLockToken mappingLockTokenLeft = MappingLockToken.Create();
            rsm.LockMapping(r1, mappingLockTokenLeft);

            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(10, 20), s1);
            MappingLockToken mappingLockTokenRight = MappingLockToken.Create();
            rsm.LockMapping(r2, mappingLockTokenRight);

            // Should throw if the correct lock owner id isn't passed
            ShardManagementException exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.MergeMappings(r1, r2));
            Assert.IsTrue(exception.ErrorCode == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap,
                "Expected MappingLockOwnerIdDoesNotMatch error when Updating mapping!");

            // Pass in an incorrect right lockowner id
            exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.MergeMappings(r1, r2, MappingLockToken.NoLock, mappingLockTokenRight));
            Assert.IsTrue(exception.ErrorCode == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap,
                "Expected MappingLockOwnerIdDoesNotMatch error when Updating mapping!");

            RangeMapping<int> rMerged = rsm.MergeMappings(r1, r2, mappingLockTokenLeft, mappingLockTokenRight);

            Assert.IsNotNull(rMerged);

            MappingLockToken storeMappingLockToken = rsm.GetMappingLockOwner(rMerged);
            Assert.AreEqual(storeMappingLockToken, mappingLockTokenLeft, "Expected merged mapping lock id to equal left mapping id!");
            rsm.UnlockMapping(rMerged, storeMappingLockToken);
            storeMappingLockToken = rsm.GetMappingLockOwner(rMerged);
            Assert.AreEqual(storeMappingLockToken, MappingLockToken.NoLock, "Expected merged mapping lock id to equal default mapping id after unlock!");
        }

        /// <summary>
        /// Merge adjacent range mappings with different location in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void MergeRangeMappingsDifferentLocation()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            Shard s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(10, 20), s2);

            ArgumentException exception = AssertExtensions.AssertThrows<ArgumentException>
                (() => rsm.MergeMappings(r1, r2));
        }

        /// <summary>
        /// Merge non-adjacent range mappings in range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void MergeRangeMappingsNonAdjacent()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(15, 20), s1);

            ArgumentOutOfRangeException exception = AssertExtensions.AssertThrows<ArgumentOutOfRangeException>
                (() => rsm.MergeMappings(r1, r2));
        }

        /// <summary>
        /// Basic test to lock range mappings that
        /// - Creates a mapping and locks it
        /// - Verifies look-up APIs work as expected
        /// - Unlock works as expected
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LockOrUnlockRangeMappingBasic()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            // Create a range mapping
            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s1);

            // Lock the mapping
            // Try to lock with an invalid owner id first
            ArgumentException argException = AssertExtensions.AssertThrows<ArgumentException>
                (() => rsm.LockMapping(r1, new MappingLockToken(MappingLockToken.ForceUnlock.LockOwnerId)));

            MappingLockToken mappingLockToken = MappingLockToken.Create();
            rsm.LockMapping(r1, mappingLockToken);

            // Trying to lock it again should result in an exception
            ShardManagementException exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.LockMapping(r1, mappingLockToken));
            Assert.IsTrue(exception.ErrorCode == ShardManagementErrorCode.MappingIsAlreadyLocked &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap, "Expected MappingIsAlreadyLocked error!");

            // Lookup should work without a lockownerId
            RangeMapping<int> r1LookUp = rsm.GetMappingForKey(5);
            Assert.AreEqual(r1, r1LookUp, "Expected range mappings to be equal!");

            // Try to unlock the mapping with the wrong lock owner id
            exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.UnlockMapping(r1, MappingLockToken.NoLock));
            Assert.IsTrue(exception.ErrorCode == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap,
                "Expected MappingLockOwnerIdDoesNotMatch error. Found: ErrorCode: {0} ErrorCategory: {1}!",
                exception.ErrorCode, ShardManagementErrorCategory.RangeShardMap);

            rsm.UnlockMapping(r1, mappingLockToken);
        }

        /// <summary>
        /// Basic test to lock range mappings that
        /// - Creates a mapping and locks it
        /// - Verifies look-up APIs work as expected
        /// - Unlock works as expected
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LockOrUnlockListMappingBasic()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> rsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            // Create a range mapping
            PointMapping<int> r1 = rsm.CreatePointMapping(1, s1);

            // Lock the mapping
            // Try to lock with an invalid owner id first
            ArgumentException argException = AssertExtensions.AssertThrows<ArgumentException>
                (() => rsm.LockMapping(r1, new MappingLockToken(MappingLockToken.ForceUnlock.LockOwnerId)));

            MappingLockToken mappingLockToken = MappingLockToken.Create();
            rsm.LockMapping(r1, mappingLockToken);

            // Trying to lock it again should result in an exception
            ShardManagementException exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.LockMapping(r1, mappingLockToken));
            Assert.IsTrue(exception.ErrorCode == ShardManagementErrorCode.MappingIsAlreadyLocked &&
                exception.ErrorCategory == ShardManagementErrorCategory.ListShardMap, "Expected MappingIsAlreadyLocked error!");

            // Lookup should work without a lockownerId
            PointMapping<int> r1LookUp = rsm.GetMappingForKey(1);
            Assert.AreEqual(r1, r1LookUp, "Expected range mappings to be equal!");

            // Try to unlock the mapping with the wrong lock owner id
            exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.UnlockMapping(r1, MappingLockToken.NoLock));
            Assert.IsTrue(exception.ErrorCode == ShardManagementErrorCode.MappingLockOwnerIdDoesNotMatch &&
                exception.ErrorCategory == ShardManagementErrorCategory.ListShardMap,
                "Expected MappingLockOwnerIdDoesNotMatch error. Found: ErrorCode: {0} ErrorCategory: {1}!",
                exception.ErrorCode, ShardManagementErrorCategory.ListShardMap);

            rsm.UnlockMapping(r1, mappingLockToken);
        }

        /// <summary>
        /// Test the Unlock API that unlocks
        /// all mappings that belong to a given lock owner id
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UnlockAllMappingsWithLockOwnerId()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            // Create a few mappings and lock some of them
            List<RangeMapping<int>> mappings = new List<RangeMapping<int>>();
            MappingLockToken mappingLockToken = MappingLockToken.Create();

            for (int i = 0; i < 100; i += 10)
            {
                RangeMapping<int> mapping = rsm.CreateRangeMapping(new Range<int>(i, i + 10), s1);
                mappings.Add(mapping);

                if (mappings.Count < 5)
                {
                    rsm.LockMapping(mapping, mappingLockToken);
                }
            }

            // Unlock all of them
            rsm.UnlockMapping(mappingLockToken);

            foreach (var mapping in mappings)
            {
                Assert.AreEqual(MappingLockToken.NoLock, rsm.GetMappingLockOwner(mapping),
                    "Expected all mappings to be unlocked!");
            }
        }

        /// <summary>
        /// Test the Unlock API that unlocks
        /// all mappings that belong to a given lock owner id
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UnlockAllMappingsListMapWithLockOwnerId()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> rsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            // Create a few mappings and lock some of them
            List<PointMapping<int>> mappings = new List<PointMapping<int>>();
            MappingLockToken mappingLockToken = MappingLockToken.Create();

            for (int i = 0; i < 100; i += 10)
            {
                PointMapping<int> mapping = rsm.CreatePointMapping(i, s1);
                mappings.Add(mapping);

                if (mappings.Count < 5)
                {
                    rsm.LockMapping(mapping, mappingLockToken);
                }
            }

            // Unlock all of them
            rsm.UnlockMapping(mappingLockToken);

            foreach (var mapping in mappings)
            {
                Assert.AreEqual(MappingLockToken.NoLock, rsm.GetMappingLockOwner(mapping),
                    "Expected all mappings to be unlocked!");
            }
        }
        #endregion RangeMapperTests

        private static void GetMappingForKey(TestShardMap<int> lsm, TestShardMap<int> lsm2, TestShardMapVerifier verifier)
        {
            Assert.IsNotNull(lsm);
            Assert.IsNotNull(lsm2);

            Shard s1 = lsm.ShardMap.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName,
                ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = lsm.ShardMap.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName,
                ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            // Create point mapping via lsm
            IMappingInfoProvider p1 = lsm.CreateMapping(1, 2, s1);
            Assert.IsNotNull(p1);

            // Get mapping from cache - it should be in lsm cache, but not in lsm2 cache
            IMappingInfoProvider cache1 = verifier.GetMappingForKey(lsm, 1, LookupOptions.LookupInCache);
            Assert.AreEqual(p1, cache1);

            verifier.GetMappingForKey_NotExists(lsm2, 1, LookupOptions.LookupInCache);

            // Get mapping from cache, then storage - it should be in lsm cache, and be loaded into lsm2 cache from storage
            IMappingInfoProvider cacheThenStorage1 = verifier.GetMappingForKey(lsm, 1);
            Assert.AreEqual(p1, cacheThenStorage1);

            IMappingInfoProvider cacheThenStorage2 = verifier.GetMappingForKey(lsm2, 1);
            Assert.AreEqual(p1, cacheThenStorage2);

            // Verify that it is now loaded in lsm2 cache
            IMappingInfoProvider cache2 = verifier.GetMappingForKey(lsm2, 1, LookupOptions.LookupInCache);
            Assert.IsNotNull(cache2);
            Assert.AreEqual(p1, cache2);

            // Delete mapping via lsm
            IMappingInfoProvider p1Offline = lsm.MarkMappingOffline(p1);
            lsm.DeleteMapping(p1Offline);

            // Verify mapping is not in lsm cache, but it is still in lsm2 cache
            // Use `LookupOptions.Cache | LookupOptions.Store` to verify that if cache is found, then storage is not checked
            verifier.GetMappingForKey_NotExists(lsm, 1, LookupOptions.LookupInCache);

            IMappingInfoProvider staleCache2 = verifier.GetMappingForKey(lsm2, 1, LookupOptions.LookupInCache | LookupOptions.LookupInStore);
            Assert.IsNotNull(staleCache2);
            Assert.AreEqual(p1, staleCache2);

            // Verify mapping cannot be found via either lsm store (by default cache is never used, only store is used)
            verifier.GetMappingForKey_NotExists(lsm, 1);
            verifier.GetMappingForKey_NotExists(lsm2, 1);

            // Lookup again from cache - the mapping should have been removed from the cache
            verifier.GetMappingForKey_NotExists(lsm2, 1, LookupOptions.LookupInCache);
        }

        /// <summary>
        /// Mark a point mapping offline or online.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void MarkMappingOfflineOnline()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            PointMapping<int> pNew = lsm.MarkMappingOffline(p1);

            Assert.IsNotNull(pNew);
            Assert.AreEqual(MappingStatus.Offline, pNew.Status, "The point mapping was not successfully marked offline.");

            pNew = lsm.MarkMappingOnline(pNew);

            Assert.IsNotNull(pNew);
            Assert.AreEqual(MappingStatus.Online, pNew.Status, "The point mapping was not successfully marked online.");

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            s = rsm.CreateShard(sl);
            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 5), s);

            RangeMapping<int> rNew = rsm.MarkMappingOffline(r1);

            Assert.IsNotNull(rNew);
            Assert.AreEqual(MappingStatus.Offline, rNew.Status, "The range mapping was not successfully marked offline.");

            rNew = rsm.MarkMappingOnline(rNew);

            Assert.IsNotNull(rNew);
            Assert.AreEqual(MappingStatus.Online, rNew.Status, "The range mapping was not successfully marked online.");
        }

        /// <summary>
        /// Mark a point mapping offline or online using MappingOptions.None
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void MarkMappingOfflineOnlineWithMappingOptions()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            PointMapping<int> pNew = lsm.MarkMappingOffline(p1, MappingOptions.None);

            Assert.IsNotNull(pNew);
            Assert.AreEqual(MappingStatus.Offline, pNew.Status, "The point mapping was not successfully marked offline.");

            pNew = lsm.MarkMappingOnline(pNew);

            Assert.IsNotNull(pNew);
            Assert.AreEqual(MappingStatus.Online, pNew.Status, "The point mapping was not successfully marked online.");

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            s = rsm.CreateShard(sl);
            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 5), s);

            RangeMapping<int> rNew = rsm.MarkMappingOffline(r1, MappingLockToken.NoLock, MappingOptions.None);

            Assert.IsNotNull(rNew);
            Assert.AreEqual(MappingStatus.Offline, rNew.Status, "The range mapping was not successfully marked offline.");

            rNew = rsm.MarkMappingOnline(rNew);

            Assert.IsNotNull(rNew);
            Assert.AreEqual(MappingStatus.Online, rNew.Status, "The range mapping was not successfully marked online.");
        }

        #region Unavailable Server

        /// <summary>
        /// OpenConnectionForKey for unavailable server using ListShardMap.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UnavailableServerOpenConnectionForKeyListShardMap()
        {
            UnavailableServerOpenConnectionForKeyListShardMapInternal();
        }

        /// <summary>
        /// OpenConnectionForKeyAsync for unavailable server using ListShardMap.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UnavailableServerOpenConnectionForKeyAsyncListShardMap()
        {
            UnavailableServerOpenConnectionForKeyListShardMapInternal(openConnectionAsync: true);
        }

        /// <summary>
        /// OpenConnectionForKey for unavailable server using ListShardMap.
        /// </summary>
        /// <param name="openConnectionAsync">Whether the connection should be opened asynchronously</param>
        private void UnavailableServerOpenConnectionForKeyListShardMapInternal(bool openConnectionAsync = false)
        {
            StubSqlStoreConnectionFactory scf = null;

            bool shouldThrow = false;

            scf = new StubSqlStoreConnectionFactory()
            {
                CallBase = true,
                GetUserConnectionString = (cstr) =>
                {
                    if (shouldThrow)
                    {
                        throw ShardMapFaultHandlingTests.TransientSqlException;
                    }
                    else
                    {
                        var original = scf.GetUserConnectionString;

                        scf.GetUserConnectionString = null;

                        try
                        {
                            return scf.GetUserConnection(cstr);
                        }
                        finally
                        {
                            scf.GetUserConnectionString = original;
                        }
                    }
                }
            };

            int callCount = 0;

            // Counting implementation of FindMappingByKey
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateFindMappingByKeyGlobalOperationShardMapManagerStringIStoreShardMapShardKeyCacheStoreMappingUpdatePolicyShardManagementErrorCategoryBooleanBoolean =
                (_smm, _opname, _ssm, _sk, _pol, _ec, _cr, _if) =>
                {
                    StubFindMappingByKeyGlobalOperation op = new StubFindMappingByKeyGlobalOperation(_smm, _opname, _ssm, _sk, _pol, _ec, _cr, _if);
                    op.CallBase = true;
                    op.DoGlobalExecuteIStoreTransactionScope = (ts) =>
                    {
                        callCount++;

                        // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                        var original = op.DoGlobalExecuteIStoreTransactionScope;

                        op.DoGlobalExecuteIStoreTransactionScope = null;
                        try
                        {
                            return op.DoGlobalExecute(ts);
                        }
                        finally
                        {
                            op.DoGlobalExecuteIStoreTransactionScope = original;
                        }
                    };

                    op.DoGlobalExecuteAsyncIStoreTransactionScope = (ts) => Task.FromResult<IStoreResults>(op.DoGlobalExecuteIStoreTransactionScope(ts));
                    return op;
                }
            };


            StubCacheStore scs = null;

            ICacheStoreMapping currentMapping = null;

            StubICacheStoreMapping sics = new StubICacheStoreMapping
            {
                MappingGet = () => currentMapping.Mapping,
                CreationTimeGet = () => currentMapping.CreationTime,
                TimeToLiveMillisecondsGet = () => currentMapping.TimeToLiveMilliseconds,
                ResetTimeToLive = () => currentMapping.ResetTimeToLive(),
                HasTimeToLiveExpired = () => currentMapping.HasTimeToLiveExpired()
            };

            scs = new StubCacheStore()
            {
                CallBase = true,
                LookupMappingByKeyIStoreShardMapShardKey = (_ssm, _sk) =>
                {
                    var original = scs.LookupMappingByKeyIStoreShardMapShardKey;
                    scs.LookupMappingByKeyIStoreShardMapShardKey = null;
                    try
                    {
                        currentMapping = scs.LookupMappingByKey(_ssm, _sk);

                        return sics;
                    }
                    finally
                    {
                        scs.LookupMappingByKeyIStoreShardMapShardKey = original;
                    }
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                scf,
                sof,
                scs,
                ShardMapManagerLoadPolicy.Lazy,
                RetryPolicy.DefaultRetryPolicy, RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);
            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);
            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(2, s);
            Assert.IsNotNull(p1);

            // Mapping is there, now let's try to abort the OpenConnectionForKey
            shouldThrow = true;

            for (int i = 1; i <= 10; i++)
            {
                Assert.ThrowsException<SqlException>(() =>
                {
                    if (openConnectionAsync)
                    {
                        lsm.OpenConnectionForKeyAsync(2, Globals.ShardUserConnectionString).GetAwaiter().GetResult();
                    }
                    else
                    {
                        lsm.OpenConnectionForKey(2, Globals.ShardUserConnectionString);
                    }
                });
            }

            Assert.AreEqual(1, callCount);

            long currentTtl = ((ICacheStoreMapping)sics).TimeToLiveMilliseconds;

            Assert.IsTrue(currentTtl > 0);

            // Let's fake the TTL to be 0, to force another call to store.
            sics.TimeToLiveMillisecondsGet = () => 0;
            sics.HasTimeToLiveExpired = () => true;

            Assert.ThrowsException<SqlException>(() =>
            {
                if (openConnectionAsync)
                {
                    lsm.OpenConnectionForKeyAsync(2, Globals.ShardUserConnectionString).GetAwaiter().GetResult();
                }
                else
                {
                    lsm.OpenConnectionForKey(2, Globals.ShardUserConnectionString);
                }
            });

            Assert.AreEqual(2, callCount);

            sics.TimeToLiveMillisecondsGet = () => currentMapping.TimeToLiveMilliseconds;
            sics.HasTimeToLiveExpired = () => currentMapping.HasTimeToLiveExpired();

            Assert.ThrowsException<SqlException>(() =>
            {
                if (openConnectionAsync)
                {
                    lsm.OpenConnectionForKeyAsync(2, Globals.ShardUserConnectionString).GetAwaiter().GetResult();
                }
                else
                {
                    lsm.OpenConnectionForKey(2, Globals.ShardUserConnectionString);
                }
            });

            Assert.IsTrue(((ICacheStoreMapping)sics).TimeToLiveMilliseconds > currentTtl);

            shouldThrow = false;

            {
                if (openConnectionAsync)
                {
                    lsm.OpenConnectionForKeyAsync(2, Globals.ShardUserConnectionString).GetAwaiter().GetResult();
                }
                else
                {
                    lsm.OpenConnectionForKey(2, Globals.ShardUserConnectionString);
                }
            }

            Assert.AreEqual(0, ((ICacheStoreMapping)sics).TimeToLiveMilliseconds);
        }

        /// <summary>
        /// OpenConnectionForKey for unavailable server using RangeShardMap.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UnavailableServerOpenConnectionForKeyRangeShardMap()
        {
            UnavailableServerOpenConnectionForKeyRangeShardMapInternal();
        }

        /// <summary>
        /// OpenConnectionForKeyAsync for unavailable server using RangeShardMap.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UnavailableServerOpenConnectionForKeyAsyncRangeShardMap()
        {
            UnavailableServerOpenConnectionForKeyRangeShardMapInternal(openConnectionAsync: true);
        }

        /// <summary>
        /// OpenConnectionForKey for unavailable server using RangeShardMap.
        /// </summary>
        /// <param name="openConnectionAsync">Whether the connection should be opened asynchronously</param>
        private void UnavailableServerOpenConnectionForKeyRangeShardMapInternal(bool openConnectionAsync = false)
        {
            StubSqlStoreConnectionFactory scf = null;

            bool shouldThrow = false;

            scf = new StubSqlStoreConnectionFactory()
            {
                CallBase = true,
                GetUserConnectionString = (cstr) =>
                {
                    if (shouldThrow)
                    {
                        throw ShardMapFaultHandlingTests.TransientSqlException;
                    }
                    else
                    {
                        var original = scf.GetUserConnectionString;

                        scf.GetUserConnectionString = null;

                        try
                        {
                            return scf.GetUserConnection(cstr);
                        }
                        finally
                        {
                            scf.GetUserConnectionString = original;
                        }
                    }
                }
            };

            int callCount = 0;

            // Counting implementation of FindMappingByKey
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateFindMappingByKeyGlobalOperationShardMapManagerStringIStoreShardMapShardKeyCacheStoreMappingUpdatePolicyShardManagementErrorCategoryBooleanBoolean =
                (_smm, _opname, _ssm, _sk, _pol, _ec, _cr, _if) =>
                {
                    StubFindMappingByKeyGlobalOperation op = new StubFindMappingByKeyGlobalOperation(_smm, _opname, _ssm, _sk, _pol, _ec, _cr, _if);
                    op.CallBase = true;
                    op.DoGlobalExecuteIStoreTransactionScope = (ts) =>
                    {
                        callCount++;

                        // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                        var original = op.DoGlobalExecuteIStoreTransactionScope;

                        op.DoGlobalExecuteIStoreTransactionScope = null;
                        try
                        {
                            return op.DoGlobalExecute(ts);
                        }
                        finally
                        {
                            op.DoGlobalExecuteIStoreTransactionScope = original;
                        }
                    };

                    op.DoGlobalExecuteAsyncIStoreTransactionScope = (ts) => Task.FromResult<IStoreResults>(op.DoGlobalExecuteIStoreTransactionScope(ts));
                    return op;
                }
            };


            StubCacheStore scs = null;

            ICacheStoreMapping currentMapping = null;

            StubICacheStoreMapping sics = new StubICacheStoreMapping
            {
                MappingGet = () => currentMapping.Mapping,
                CreationTimeGet = () => currentMapping.CreationTime,
                TimeToLiveMillisecondsGet = () => currentMapping.TimeToLiveMilliseconds,
                ResetTimeToLive = () => currentMapping.ResetTimeToLive(),
                HasTimeToLiveExpired = () => currentMapping.HasTimeToLiveExpired()
            };

            scs = new StubCacheStore()
            {
                CallBase = true,
                LookupMappingByKeyIStoreShardMapShardKey = (_ssm, _sk) =>
                {
                    var original = scs.LookupMappingByKeyIStoreShardMapShardKey;
                    scs.LookupMappingByKeyIStoreShardMapShardKey = null;
                    try
                    {
                        currentMapping = scs.LookupMappingByKey(_ssm, _sk);

                        return sics;
                    }
                    finally
                    {
                        scs.LookupMappingByKeyIStoreShardMapShardKey = original;
                    }
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                scf,
                sof,
                scs,
                ShardMapManagerLoadPolicy.Lazy,
                RetryPolicy.DefaultRetryPolicy, RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);
            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);
            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(5, 20), s);
            Assert.IsNotNull(r1);

            // Mapping is there, now let's try to abort the OpenConnectionForKey
            shouldThrow = true;

            for (int i = 1; i <= 10; i++)
            {
                Assert.ThrowsException<SqlException>(() =>
                {
                    if (openConnectionAsync)
                    {
                        rsm.OpenConnectionForKeyAsync(10, Globals.ShardUserConnectionString).GetAwaiter().GetResult();
                    }
                    else
                    {
                        rsm.OpenConnectionForKey(10, Globals.ShardUserConnectionString);
                    }
                });
            }

            Assert.AreEqual(1, callCount);

            long currentTtl = ((ICacheStoreMapping)sics).TimeToLiveMilliseconds;

            Assert.IsTrue(currentTtl > 0);

            // Let's fake the TTL to be 0, to force another call to store.
            sics.TimeToLiveMillisecondsGet = () => 0;
            sics.HasTimeToLiveExpired = () => true;

            Assert.ThrowsException<SqlException>(() =>
            {
                if (openConnectionAsync)
                {
                    rsm.OpenConnectionForKeyAsync(12, Globals.ShardUserConnectionString).GetAwaiter().GetResult();
                }
                else
                {
                    rsm.OpenConnectionForKey(12, Globals.ShardUserConnectionString);
                }
            });

            Assert.AreEqual(2, callCount);

            sics.TimeToLiveMillisecondsGet = () => currentMapping.TimeToLiveMilliseconds;
            sics.HasTimeToLiveExpired = () => currentMapping.HasTimeToLiveExpired();

            Assert.ThrowsException<SqlException>(() =>
            {
                if (openConnectionAsync)
                {
                    rsm.OpenConnectionForKeyAsync(15, Globals.ShardUserConnectionString).GetAwaiter().GetResult();
                }
                else
                {
                    rsm.OpenConnectionForKey(15, Globals.ShardUserConnectionString);
                }
            });

            Assert.IsTrue(((ICacheStoreMapping)sics).TimeToLiveMilliseconds > currentTtl);

            shouldThrow = false;

            {
                if (openConnectionAsync)
                {
                    rsm.OpenConnectionForKeyAsync(7, Globals.ShardUserConnectionString).GetAwaiter().GetResult();
                }
                else
                {
                    rsm.OpenConnectionForKey(7, Globals.ShardUserConnectionString);
                }
            }

            Assert.AreEqual(0, ((ICacheStoreMapping)sics).TimeToLiveMilliseconds);
        }

        #endregion Unavailable Server

        #region CacheAbortTests

        /// <summary>
        /// Add point mapping in list shard map, do not update local cache.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddPointMappingNoCacheUpdate()
        {
            // Create a cache store that never inserts.
            CountingCacheStore cacheStore =
                new CountingCacheStore(
                    new StubCacheStore()
                    {
                        CallBase = true,
                        AddOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy = (ssm, p) => { }
                    });

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                cacheStore,
                ShardMapManagerLoadPolicy.Lazy,
                RetryPolicy.DefaultRetryPolicy, RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);
            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);
            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(2, s);
            Assert.IsNotNull(p1);

            PointMapping<int> p2 = lsm.GetMappingForKey(2);
            Assert.IsNotNull(p2);

            Assert.AreEqual(0, cacheStore.LookupMappingMissCount);
        }

        /// <summary>
        /// Add a range mapping to range shard map, donot update local cache.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddRangeMappingNoCacheUpdate()
        {
            // Create a cache store that never inserts.
            CountingCacheStore cacheStore =
                new CountingCacheStore(
                    new StubCacheStore()
                    {
                        CallBase = true,
                        AddOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy = (ssm, p) => { }
                    });

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                cacheStore,
                ShardMapManagerLoadPolicy.Lazy,
                RetryPolicy.DefaultRetryPolicy, RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);
            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);
            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
            Assert.IsNotNull(r1);

            RangeMapping<int> r2 = rsm.GetMappingForKey(2);

            Assert.IsNotNull(r2);
            Assert.AreEqual(0, cacheStore.LookupMappingMissCount);
        }

        #endregion CacheAbortTests

        #region GsmAbortTests

        /// <summary>
        /// Add a point mapping to list shard map, do not commit GSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddPointMappingAbortGSM()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                (_smm, _opcode, _ssm, _sm) =>
                {
                    StubAddMappingOperation op = new StubAddMappingOperation(_smm, _opcode, _ssm, _sm);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.DoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);


            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            bool storeOperationFailed = false;
            try
            {
                PointMapping<int> p1 = lsm.CreatePointMapping(2, s);
                Assert.IsNotNull(p1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            shouldThrow = false;

            // Validation: Adding same mapping again will succeed.
            PointMapping<int> p2 = lsm.CreatePointMapping(2, s);
            Assert.IsNotNull(p2);
        }

        /// <summary>
        /// Delete existing point mapping from list shard map, do not commit GSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeletePointMappingAbortGSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid =
                (_smm, _opcode, _ssm, _sm, _loid) =>
                {
                    StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm, _loid);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            PointMapping<int> pmOffline = lsm.MarkMappingOffline(p1);
            Assert.IsNotNull(pmOffline);

            bool storeOperationFailed = false;
            try
            {
                lsm.DeleteMapping(pmOffline);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // validation: Lookup point will succeed.
            PointMapping<int> pNew = lsm.GetMappingForKey(1);
            Assert.IsNotNull(pNew);
        }

        /// <summary>
        /// Update existing point mapping in list shard map, do not commit GSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdatePointMappingAbortGSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            // Take the mapping offline first before the shard location can be updated.
            PointMappingUpdate pu = new PointMappingUpdate()
            {
                Status = MappingStatus.Offline
            };

            bool storeOperationFailed = false;
            try
            {
                PointMapping<int> pNew = lsm.UpdateMapping(p1, pu);
                Assert.IsNotNull(pNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // validation: validate custom field of the mapping.
            PointMapping<int> pValidate = lsm.GetMappingForKey(1);
            Assert.AreEqual(p1.Status, pValidate.Status);
        }

        /// <summary>
        /// Update location of existing point mapping in list shard map, do not commit GSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdatePointMappingLocationAbortGSM()
        {
            bool shouldThrow = false;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    if (shouldThrow)
                    {
                        op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        };
                    }

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Shard s1 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s1);

            PointMappingUpdate pu1 = new PointMappingUpdate();
            // Take the mapping offline first before the shard location can be updated.
            pu1.Status = MappingStatus.Offline;
            PointMapping<int> pNew = lsm.UpdateMapping(p1, pu1);

            PointMappingUpdate pu2 = new PointMappingUpdate()
            {
                Shard = s2
            };

            shouldThrow = true;

            bool storeOperationFailed = false;
            try
            {
                pNew = lsm.UpdateMapping(pNew, pu2);
                Assert.IsNotNull(pNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // validation: validate location of the mapping.
            PointMapping<int> pValidate = lsm.GetMappingForKey(1);
            Assert.AreEqual(p1.Shard.Id, pValidate.Shard.Id);
        }

        /// <summary>
        /// Add a range mapping to range shard map, do not commit GSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddRangeMappingAbortGSM()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                (_smm, _opcode, _ssm, _sm) =>
                {
                    StubAddMappingOperation op = new StubAddMappingOperation(_smm, _opcode, _ssm, _sm);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.DoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            bool storeOperationFailed = false;
            try
            {
                RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
                Assert.IsNotNull(r1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            shouldThrow = false;

            // validation: adding same range mapping again will succeed.
            RangeMapping<int> rValidate = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
            Assert.IsNotNull(rValidate);
        }

        /// <summary>
        /// Delete existing range mapping from range shard map, abort transaction in GSM
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteRangeMappingAbortGSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid =
                (_smm, _opcode, _ssm, _sm, _loid) =>
                {
                    StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm, _loid);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            RangeMappingUpdate ru = new RangeMappingUpdate();
            ru.Status = MappingStatus.Offline;

            // The mapping must be made offline before it can be deleted.
            r1 = rsm.UpdateMapping(r1, ru);
            Assert.AreEqual(MappingStatus.Offline, r1.Status);

            bool storeOperationFailed = false;
            try
            {
                rsm.DeleteMapping(r1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: lookup for 5 returns a valid mapping.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(5);
            Assert.IsNotNull(rValidate);
            Assert.AreEqual(rValidate.Range, r1.Range);
        }

        /// <summary>
        /// Update range mapping in range shard map, do not commit transaction in GSM
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingAbortGSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            RangeMappingUpdate ru = new RangeMappingUpdate()
            {
                Status = MappingStatus.Offline
            };

            bool storeOperationFailed = false;
            try
            {
                RangeMapping<int> rNew = rsm.UpdateMapping(r1, ru);
                Assert.IsNotNull(rNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: check that custom is unchanged.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(1);
            Assert.AreEqual(r1.Status, rValidate.Status);
        }

        /// <summary>
        /// Update range mapping in range shard map to change location, do not commit transaction in GSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingLocationAbortGSM()
        {
            bool shouldThrow = false;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    if (shouldThrow)
                    {
                        op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        };
                    }

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl1 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);
            Shard s1 = rsm.CreateShard(sl1);
            Assert.IsNotNull(s1);

            ShardLocation sl2 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]);
            Shard s2 = rsm.CreateShard(sl2);
            Assert.IsNotNull(s2);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s1);

            RangeMappingUpdate ru1 = new RangeMappingUpdate();
            // Take the mapping offline first.
            ru1.Status = MappingStatus.Offline;
            RangeMapping<int> rNew = rsm.UpdateMapping(r1, ru1);
            Assert.IsNotNull(rNew);

            RangeMappingUpdate ru2 = new RangeMappingUpdate();
            ru2.Shard = s2;

            shouldThrow = true;

            bool storeOperationFailed = false;
            try
            {
                rNew = rsm.UpdateMapping(rNew, ru2);
                Assert.IsNotNull(rNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // validation: validate location of the mapping.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(1);
            Assert.AreEqual(s1.Id, rValidate.Shard.Id);
        }

        /// <summary>
        /// Split existing range mapping in range shard map, abort transaction in GSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void SplitRangeAbortGSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
                (_smm, _opcode, _ssm, _smo, _smn) =>
                {
                    StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo, _smn);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            bool storeOperationFailed = false;
            try
            {
                IReadOnlyList<RangeMapping<int>> rList = rsm.SplitMapping(r1, 5);
                Assert.AreEqual(2, rList.Count);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: get all mappings for [1,20) should return 1 mapping.
            Assert.AreEqual(1, rsm.GetMappings().Count());
        }

        /// <summary>
        /// Merge adjacent range mappings in range shard map, do not commit transaction in GSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void MergeRangeMappingsAbortGSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
                (_smm, _opcode, _ssm, _smo, _smn) =>
                {
                    StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo, _smn);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(10, 20), s1);

            bool storeOperationFailed = false;
            try
            {
                RangeMapping<int> rMerged = rsm.MergeMappings(r1, r2);
                Assert.IsNotNull(rMerged);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: get all mappings for [1,20) should return 2 mappings.
            Assert.AreEqual(2, rsm.GetMappings().Count());
        }

        #endregion GsmAbortTests

        #region LsmAbortTests

        /// <summary>
        /// Add a point mapping to list shard map, do not commit LSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddPointMappingAbortLSM()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                (_smm, _opcode, _ssm, _sm) =>
                {
                    StubAddMappingOperation op = new StubAddMappingOperation(_smm, _opcode, _ssm, _sm);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoLocalSourceExecuteIStoreTransactionScope;

                            op.DoLocalSourceExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoLocalSourceExecute(ts);
                            }
                            finally
                            {
                                op.DoLocalSourceExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            bool storeOperationFailed = false;
            try
            {
                PointMapping<int> p1 = lsm.CreatePointMapping(2, s);
                Assert.IsNotNull(p1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            shouldThrow = false;

            // Validation: Adding same mapping again will succeed.
            PointMapping<int> p2 = lsm.CreatePointMapping(2, s);
            Assert.IsNotNull(p2);
        }

        /// <summary>
        /// Delete existing point mapping from list shard map, do not commit LSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeletePointMappingAbortLSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid =
                (_smm, _opcode, _ssm, _sm, _loid) =>
                {
                    StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm, _loid);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            PointMappingUpdate ru = new PointMappingUpdate();
            ru.Status = MappingStatus.Offline;

            // The mapping must be made offline before it can be deleted.
            p1 = lsm.UpdateMapping(p1, ru);
            Assert.AreEqual(MappingStatus.Offline, p1.Status);

            bool storeOperationFailed = false;
            try
            {
                lsm.DeleteMapping(p1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // validation: Lookup point will succeed.
            PointMapping<int> pNew = lsm.GetMappingForKey(1);
            Assert.IsNotNull(pNew);
        }

        /// <summary>
        /// Update existing point mapping in list shard map, do not commit LSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdatePointMappingAbortLSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s);

            // Take the mapping offline first before the shard location can be updated.
            PointMappingUpdate pu = new PointMappingUpdate()
            {
                Status = MappingStatus.Offline
            };

            bool storeOperationFailed = false;
            try
            {
                PointMapping<int> pNew = lsm.UpdateMapping(p1, pu);
                Assert.IsNotNull(pNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // validation: validate custom field of the mapping.
            PointMapping<int> pValidate = lsm.GetMappingForKey(1);
            Assert.AreEqual(p1.Status, pValidate.Status);
        }

        /// <summary>
        /// Update location of existing point mapping in list shard map, do not commit LSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdatePointMappingLocationAbortLSM()
        {
            bool shouldThrow = false;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    if (shouldThrow)
                    {
                        // Abort on target.
                        op.DoLocalTargetExecuteIStoreTransactionScope = (ts) =>
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        };
                    }

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Shard s1 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            PointMapping<int> p1 = lsm.CreatePointMapping(1, s1);

            PointMappingUpdate pu1 = new PointMappingUpdate();
            // Take the mapping offline first before the shard location can be updated.
            pu1.Status = MappingStatus.Offline;
            PointMapping<int> pNew = lsm.UpdateMapping(p1, pu1);

            PointMappingUpdate pu2 = new PointMappingUpdate();
            pu2.Shard = s2;

            shouldThrow = true;

            bool storeOperationFailed = false;
            try
            {
                pNew = lsm.UpdateMapping(pNew, pu2);
                Assert.IsNotNull(pNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // validation: validate location of the mapping.
            PointMapping<int> pValidate = lsm.GetMappingForKey(1);
            Assert.AreEqual(p1.Shard.Id, pValidate.Shard.Id);
        }

        /// <summary>
        /// Add a range mapping to range shard map, do not commit LSM transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddRangeMappingAbortLSM()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                (_smm, _opcode, _ssm, _sm) =>
                {
                    StubAddMappingOperation op = new StubAddMappingOperation(_smm, _opcode, _ssm, _sm);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoLocalSourceExecuteIStoreTransactionScope;

                            op.DoLocalSourceExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoLocalSourceExecute(ts);
                            }
                            finally
                            {
                                op.DoLocalSourceExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            bool storeOperationFailed = false;
            try
            {
                RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
                Assert.IsNotNull(r1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            shouldThrow = false;

            // validation: adding same range mapping again will succeed.
            RangeMapping<int> rValidate = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
            Assert.IsNotNull(rValidate);
        }

        /// <summary>
        /// Delete existing range mapping from range shard map, abort transaction in LSM
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteRangeMappingAbortLSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid =
                (_smm, _opcode, _ssm, _sm, _loid) =>
                {
                    StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm, _loid);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            RangeMappingUpdate ru = new RangeMappingUpdate();
            ru.Status = MappingStatus.Offline;

            // The mapping must be made offline before it can be deleted.
            r1 = rsm.UpdateMapping(r1, ru);
            Assert.AreEqual(MappingStatus.Offline, r1.Status);

            bool storeOperationFailed = false;
            try
            {
                rsm.DeleteMapping(r1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: lookup for 5 returns a valid mapping.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(5);
            Assert.IsNotNull(rValidate);
        }

        /// <summary>
        /// Update range mapping in range shard map, do not commit transaction in LSM
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingAbortLSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            RangeMappingUpdate ru = new RangeMappingUpdate()
            {
                Status = MappingStatus.Offline
            };

            bool storeOperationFailed = false;
            try
            {
                RangeMapping<int> rNew = rsm.UpdateMapping(r1, ru);
                Assert.IsNotNull(rNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: check that custom is unchanged.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(1);
            Assert.AreEqual(r1.Status, rValidate.Status);
        }

        /// <summary>
        /// Update range mapping in range shard map to change location, do not commit transaction in LSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingLocationAbortLSM()
        {
            bool shouldThrow = false;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    if (shouldThrow)
                    {
                        // Abort on source.
                        op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        };
                    }

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl1 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);
            Shard s1 = rsm.CreateShard(sl1);
            Assert.IsNotNull(s1);

            ShardLocation sl2 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]);
            Shard s2 = rsm.CreateShard(sl2);
            Assert.IsNotNull(s2);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s1);

            RangeMappingUpdate ru1 = new RangeMappingUpdate();
            // Take the mapping offline first.
            ru1.Status = MappingStatus.Offline;
            RangeMapping<int> rNew = rsm.UpdateMapping(r1, ru1);
            Assert.IsNotNull(rNew);

            RangeMappingUpdate ru2 = new RangeMappingUpdate();
            ru2.Shard = s2;

            shouldThrow = true;
            bool storeOperationFailed = false;
            try
            {
                rNew = rsm.UpdateMapping(rNew, ru2);
                Assert.IsNotNull(rNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // validation: validate location of the mapping.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(1);
            Assert.AreEqual(s1.Id, rValidate.Shard.Id);
        }

        /// <summary>
        /// Split existing range mapping in range shard map, abort transaction in LSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void SplitRangeAbortLSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
                (_smm, _opcode, _ssm, _smo, _smn) =>
                {
                    StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo, _smn);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            bool storeOperationFailed = false;
            try
            {
                IReadOnlyList<RangeMapping<int>> rList = rsm.SplitMapping(r1, 5);
                Assert.AreEqual(2, rList.Count);
            }

            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: get all mappings for [1,20) should return 1 mapping.
            Assert.AreEqual(1, rsm.GetMappings().Count());
        }

        /// <summary>
        /// Merge adjacent range mappings in range shard map, do not commit transaction in LSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void MergeRangeMappingsAbortLSM()
        {
            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
                (_smm, _opcode, _ssm, _smo, _smn) =>
                {
                    StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo, _smn);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(10, 20), s1);

            bool storeOperationFailed = false;
            try
            {
                RangeMapping<int> rMerged = rsm.MergeMappings(r1, r2);
                Assert.IsNotNull(rMerged);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: get all mappings for [1,20) should return 2 mappings.
            Assert.AreEqual(2, rsm.GetMappings().Count());
        }

        #endregion LsmAbortTests

        #region GSMAbort With Failing Undo

        /// <summary>
        /// Add a point mapping to list shard map, do not commit GSM Do or Undo transaction.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void AddPointMappingAbortGSMDoAndGSMUndo()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                (_smm, _opcode, _ssm, _sm) =>
                {
                    StubAddMappingOperation op = new StubAddMappingOperation(_smm, _opcode, _ssm, _sm);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.DoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    op.UndoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.UndoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.UndoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.UndoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.UndoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);
            ShardLocation sl2 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]);

            Shard s = lsm.CreateShard(sl);
            Assert.IsNotNull(s);
            Shard s2 = lsm.CreateShard(sl2);
            Assert.IsNotNull(s2);

            bool storeOperationFailed = false;
            try
            {
                PointMapping<int> p1 = lsm.CreatePointMapping(2, s);
                Assert.IsNotNull(p1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Obtain the pending operations.
            var pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 1);

            shouldThrow = false;

            // Validation: Adding same mapping again even at different location should succeed.
            PointMapping<int> p2 = lsm.CreatePointMapping(2, s2);
            Assert.IsNotNull(p2);

            pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 0);
        }

        /// <summary>
        /// Delete existing range mapping from range shard map, abort transaction in LSM for Do and GSM for Undo.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteRangeMappingAbortLSMDoAndGSMUndo()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid =
                (_smm, _opcode, _ssm, _sm, _loid) =>
                {
                    StubRemoveMappingOperation op = new StubRemoveMappingOperation(_smm, _opcode, _ssm, _sm, _loid);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoLocalSourceExecuteIStoreTransactionScope;

                            op.DoLocalSourceExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoLocalSourceExecute(ts);
                            }
                            finally
                            {
                                op.DoLocalSourceExecuteIStoreTransactionScope = original;
                            }
                        }
                    };
                    op.UndoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.UndoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.UndoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.UndoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.UndoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);

            Assert.IsNotNull(r1);

            RangeMappingUpdate ru = new RangeMappingUpdate();
            ru.Status = MappingStatus.Offline;

            // The mapping must be made offline before it can be deleted.
            r1 = rsm.UpdateMapping(r1, ru);
            Assert.AreEqual(MappingStatus.Offline, r1.Status);

            bool storeOperationFailed = false;
            try
            {
                rsm.DeleteMapping(r1);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Obtain the pending operations.
            var pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 1);

            // Validation: lookup for 5 still returns a valid mapping since we never committed the remove.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(5);
            Assert.IsNotNull(rValidate);
            Assert.AreEqual(rValidate.Range, r1.Range);


            #region OpenConnection with Validation

            // Validation should fail with mapping is offline error since local mapping was not deleted.
            bool validationFailed = false;
            try
            {
                using (SqlConnection conn = rsm.OpenConnection(
                    rValidate,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
            catch (ShardManagementException smme)
            {
                validationFailed = true;
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingIsOffline);
            }

            Assert.AreEqual(true, validationFailed);

            #endregion OpenConnection with Validation

            shouldThrow = false;

            // Now we try an AddOperation, which should fail since we still have the mapping.
            ShardManagementException exception = AssertExtensions.AssertThrows<ShardManagementException>
                (() => rsm.CreateRangeMapping(new Range<int>(1, 10), s));

            Assert.IsTrue(
                exception.ErrorCode == ShardManagementErrorCode.MappingRangeAlreadyMapped &&
                exception.ErrorCategory == ShardManagementErrorCategory.RangeShardMap,
                "Expected MappingRangeAlreadyMapped error!");

            // No pending operation should be left now since the previous operation took care of it.
            pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 0);

            // Removal should succeed now.
            storeOperationFailed = false;
            try
            {
                rsm.DeleteMapping(r1);
            }
            catch (ShardManagementException)
            {
                storeOperationFailed = true;
            }

            Assert.IsFalse(storeOperationFailed);
        }

        /// <summary>
        /// Update range mapping in range shard map, do not commit transaction in GSM Do and LSM Source Undo.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingOfflineAbortGSMDoAndGSMUndoPostLocal()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.DoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    op.UndoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.UndoLocalSourceExecuteIStoreTransactionScope;

                            op.UndoLocalSourceExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.UndoLocalSourceExecute(ts);
                            }
                            finally
                            {
                                op.UndoLocalSourceExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            RangeMappingUpdate ru = new RangeMappingUpdate()
            {
                Status = MappingStatus.Offline
            };

            RangeMapping<int> rNew;

            bool storeOperationFailed = false;
            try
            {
                rNew = rsm.UpdateMapping(r1, ru);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Validation: check that custom is unchanged.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(1);
            Assert.AreEqual(r1.Status, rValidate.Status);

            shouldThrow = false;

            rNew = rsm.UpdateMapping(r1, ru);
            Assert.IsNotNull(rNew);
            Assert.AreEqual(rNew.Status, ru.Status);
        }

        /// <summary>
        /// Update range mapping in range shard map to change location, abort GSM post Local in Do and LSM target in Undo.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateRangeMappingLocationAbortGSMPostLocalDoAndLSMTargetUndo()
        {
            bool shouldThrow = false;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc) =>
                {
                    StubUpdateMappingOperation op = new StubUpdateMappingOperation(_smm, _opcode, _ssm, _sms, _smt, _p, _loid, _kc);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.DoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    op.UndoLocalTargetExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.UndoLocalTargetExecuteIStoreTransactionScope;

                            op.UndoLocalTargetExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.UndoLocalTargetExecute(ts);
                            }
                            finally
                            {
                                op.UndoLocalTargetExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl1 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);
            Shard s1 = rsm.CreateShard(sl1);
            Assert.IsNotNull(s1);

            ShardLocation sl2 = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[1]);
            Shard s2 = rsm.CreateShard(sl2);
            Assert.IsNotNull(s2);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s1);

            RangeMappingUpdate ru1 = new RangeMappingUpdate();
            // Take the mapping offline first.
            ru1.Status = MappingStatus.Offline;
            RangeMapping<int> rNew = rsm.UpdateMapping(r1, ru1);
            Assert.IsNotNull(rNew);

            RangeMappingUpdate ru2 = new RangeMappingUpdate();
            ru2.Shard = s2;

            shouldThrow = true;

            bool storeOperationFailed = false;
            try
            {
                rNew = rsm.UpdateMapping(rNew, ru2);
                Assert.IsNotNull(rNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Obtain the pending operations.
            var pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 1);

            // validation: validate location of the mapping.
            RangeMapping<int> rValidate = rsm.GetMappingForKey(1);
            Assert.AreEqual(s1.Id, rValidate.Shard.Id);

            #region OpenConnection with Validation

            // Validation should fail with mapping does not exist since source mapping was deleted.
            bool validationFailed = false;
            try
            {
                using (SqlConnection conn = rsm.OpenConnection(
                    rValidate,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
            catch (ShardManagementException smme)
            {
                validationFailed = true;
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
            }

            Assert.AreEqual(true, validationFailed);

            #endregion OpenConnection with Validation

            shouldThrow = false;

            // Removal should succeed now.
            storeOperationFailed = false;

            try
            {
                rsm.DeleteMapping(rNew);
            }
            catch (ShardManagementException)
            {
                storeOperationFailed = true;
            }

            Assert.IsFalse(storeOperationFailed);
        }

        /// <summary>
        /// Split range mapping in range shard map, abort GSM post Local in Do and GSM post local in Undo.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void SplitRangeMappingAbortGSMPostLocalDoAndGSMPostLocalUndo()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
                (_smm, _opcode, _ssm, _smo, _smn) =>
                {
                    StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo, _smn);
                    op.CallBase = true;
                    op.DoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.DoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.DoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    op.UndoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.UndoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.UndoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.UndoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.UndoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);

            bool storeOperationFailed = false;
            try
            {
                IReadOnlyList<RangeMapping<int>> rList = rsm.SplitMapping(r1, 10);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Obtain the pending operations.
            var pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 1);

            // Validation: Mapping range is not updated - lookup for point 10 returns mapping with version 0.
            RangeMapping<int> rValidateLeft = rsm.GetMappingForKey(5);
            RangeMapping<int> rValidateRight = rsm.GetMappingForKey(15);
            Assert.AreEqual(r1.Range, rValidateLeft.Range);
            Assert.AreEqual(r1.Range, rValidateRight.Range);

            #region OpenConnection with Validation

            // Validation should succeed since source mapping was never deleted.
            bool validationFailed = false;
            try
            {
                using (SqlConnection conn = rsm.OpenConnection(
                    rValidateLeft,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
            catch (ShardManagementException)
            {
                validationFailed = true;
            }

            Assert.AreEqual(false, validationFailed);

            #endregion OpenConnection with Validation

            shouldThrow = false;

            // Try splitting again.
            storeOperationFailed = false;

            try
            {
                rsm.SplitMapping(r1, 10);
            }
            catch (ShardManagementException)
            {
                storeOperationFailed = true;
            }

            Assert.IsFalse(storeOperationFailed);
        }

        /// <summary>
        /// Merge range mappings in range shard map, abort LSM Source Local in Do and GSM post local in Undo.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void MergeRangeMappingAbortSourceLocalDoAndGSMPostLocalUndo()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
                (_smm, _opcode, _ssm, _smo, _smn) =>
                {
                    StubReplaceMappingsOperation op = new StubReplaceMappingsOperation(_smm, _opcode, _ssm, _smo, _smn);
                    op.CallBase = true;
                    op.DoLocalSourceExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.DoLocalSourceExecuteIStoreTransactionScope;

                            op.DoLocalSourceExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.DoLocalSourceExecute(ts);
                            }
                            finally
                            {
                                op.DoLocalSourceExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    op.UndoGlobalPostLocalExecuteIStoreTransactionScope = (ts) =>
                    {
                        if (shouldThrow)
                        {
                            throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                        }
                        else
                        {
                            // Call the base function, hack for this behavior is to save current operation, set current to null, restore current operation.
                            var original = op.UndoGlobalPostLocalExecuteIStoreTransactionScope;

                            op.UndoGlobalPostLocalExecuteIStoreTransactionScope = null;
                            try
                            {
                                return op.UndoGlobalPostLocalExecute(ts);
                            }
                            finally
                            {
                                op.UndoGlobalPostLocalExecuteIStoreTransactionScope = original;
                            }
                        }
                    };

                    return op;
                }
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                sof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapperTests.s_shardedDBs[0]);

            Shard s = rsm.CreateShard(sl);

            Assert.IsNotNull(s);

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 20), s);
            RangeMapping<int> r2 = rsm.CreateRangeMapping(new Range<int>(20, 40), s);

            bool storeOperationFailed = false;
            try
            {
                RangeMapping<int> rMerged = rsm.MergeMappings(r1, r2);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.RangeShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Obtain the pending operations.
            var pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 1);

            // Validation: Mapping range is not updated - lookup for point 10 returns mapping with version 0.
            RangeMapping<int> rValidateLeft = rsm.GetMappingForKey(5);
            RangeMapping<int> rValidateRight = rsm.GetMappingForKey(25);
            Assert.AreEqual(r1.Range, rValidateLeft.Range);
            Assert.AreEqual(r2.Range, rValidateRight.Range);

            #region OpenConnection with Validation

            // Validation should succeed since source mapping was never deleted.
            bool validationFailed = false;
            try
            {
                using (SqlConnection conn = rsm.OpenConnection(
                    rValidateLeft,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }

                using (SqlConnection conn = rsm.OpenConnection(
                    rValidateRight,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
            catch (ShardManagementException)
            {
                validationFailed = true;
            }

            Assert.AreEqual(false, validationFailed);

            #endregion OpenConnection with Validation

            shouldThrow = false;

            // Split the range mapping on the left.
            storeOperationFailed = false;

            try
            {
                rsm.SplitMapping(r1, 10);
            }
            catch (ShardManagementException)
            {
                storeOperationFailed = true;
            }

            Assert.IsFalse(storeOperationFailed);
        }

        #endregion GSMAbort With Failing Undo

        #region Helper Methods

        internal static IEnumerable<IStoreLogEntry> GetPendingStoreOperations()
        {
            IStoreResults result;
            using (IStoreConnection conn = new SqlStoreConnectionFactory().GetConnection(
                StoreConnectionKind.Global,
                new SqlConnectionInfo(
                    Globals.ShardMapManagerConnectionString,
                    null)))
            {
                conn.Open();

                using (IStoreTransactionScope ts = conn.GetTransactionScope(StoreTransactionScopeKind.ReadOnly))
                {
                    result = ts.ExecuteCommandSingle(
                        new StringBuilder(
                        @"select
		                  6, OperationId, OperationCode, Data, UndoStartState, ShardVersionRemoves, ShardVersionAdds
	                      from
		                  __ShardManagement.OperationsLogGlobal"));
                }
            }

            return result.StoreOperations;
        }

        /// <summary>
        /// Take a mapping offline, verify that the existing connection is not killed using mapping options.
        /// </summary>
        internal void ValidateConnectionPreservedOnOfflineMapping(dynamic shardMapper, dynamic mapping)
        {

            using (SqlConnection conn = shardMapper.OpenConnectionForKeyAsync(1, Globals.ShardUserConnectionString).Result)
            {
                Assert.AreEqual(ConnectionState.Open, conn.State);

                var mNew = shardMapper.MarkMappingOffline(mapping, MappingOptions.None);
                Assert.IsNotNull(mNew);

                bool failed = false;

                try
                {
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1";
                        cmd.CommandType = CommandType.Text;

                        using (SqlDataReader rdr = cmd.ExecuteReader())
                        {
                        }
                    }
                }
                catch (SqlException)
                {
                    failed = true;
                }

                Assert.AreEqual(false, failed);
                Assert.AreEqual(ConnectionState.Open, conn.State);

                failed = false;

                // Open 2nd connection.
                try
                {
                    using (SqlConnection conn2 = shardMapper.OpenConnectionForKeyAsync(1, Globals.ShardUserConnectionString).Result)
                    {
                    }
                }
                catch (AggregateException ex)
                {
                    var sme = ex.InnerException as ShardManagementException;
                    if (sme != null)
                    {
                        failed = true;
                        Assert.AreEqual(ShardManagementErrorCode.MappingIsOffline, sme.ErrorCode);
                    }
                }

                Assert.AreEqual(true, failed);

                // Mark the mapping online again so that it will be cleaned up
                var mUpdated = shardMapper.MarkMappingOnline(mNew);
                Assert.IsNotNull(mUpdated);

                failed = false;

                // Open 3rd connection. This should succeed.
                try
                {
                    using (SqlConnection conn3 = shardMapper.OpenConnectionForKey(1, Globals.ShardUserConnectionString))
                    {
                    }
                }
                catch (ShardManagementException)
                {
                    failed = true;
                }

                Assert.AreEqual(false, failed);
            }
        }


        #endregion Helper Methods
    }
}
