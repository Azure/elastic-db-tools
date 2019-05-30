// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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
    public class DateTimeShardMapperTests
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
            ListShardMap<DateTime> lsm;
            if (smm.TryGetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName, out lsm))
            {
                Assert.IsNotNull(lsm);

                foreach (PointMapping<DateTime> pm in lsm.GetMappings())
                {
                    PointMapping<DateTime> pmOffline = lsm.MarkMappingOffline(pm);
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
            RangeShardMap<DateTime> rsm;
            if (smm.TryGetRangeShardMap<DateTime>(DateTimeShardMapperTests.s_rangeShardMapName, out rsm))
            {
                Assert.IsNotNull(rsm);

                foreach (RangeMapping<DateTime> rm in rsm.GetMappings())
                {
                    MappingLockToken mappingLockToken = rsm.GetMappingLockOwner(rm);
                    rsm.UnlockMapping(rm, mappingLockToken);
                    RangeMapping<DateTime> rmOffline = rsm.MarkMappingOffline(rm);
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
                for (int i = 0; i < DateTimeShardMapperTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
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

            ListShardMap<DateTime> lsm = smm.CreateListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Assert.AreEqual(DateTimeShardMapperTests.s_listShardMapName, lsm.Name);

            // Create range shard map.
            RangeShardMap<DateTime> rsm = smm.CreateRangeShardMap<DateTime>(DateTimeShardMapperTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Assert.AreEqual(DateTimeShardMapperTests.s_rangeShardMapName, rsm.Name);
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
                for (int i = 0; i < DateTimeShardMapperTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
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
            DateTimeShardMapperTests.CleanShardMapsHelper();
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        [TestCleanup()]
        public void ShardMapperTestCleanup()
        {
            DateTimeShardMapperTests.CleanShardMapsHelper();
        }

        #endregion Common Methods

        #region WithDates

        /// <summary>
        /// All combinations of getting point mappings from a list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DateGetPointMappingsForRange()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<DateTime> lsm = smm.GetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Shard s1 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[0]));
            Assert.IsNotNull(s1);

            Shard s2 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[1]));
            Assert.IsNotNull(s2);

            DateTime val1 = DateTime.Now.Subtract(TimeSpan.FromMinutes(10));
            PointMapping<DateTime> p1 = lsm.CreatePointMapping(val1, s1);
            Assert.IsNotNull(p1);

            DateTime val2 = DateTime.Now.Subtract(TimeSpan.FromMinutes(20));
            PointMapping<DateTime> p2 = lsm.CreatePointMapping(val2, s1);
            Assert.IsNotNull(p2);

            DateTime val3 = DateTime.Now.Subtract(TimeSpan.FromMinutes(30));
            PointMapping<DateTime> p3 = lsm.CreatePointMapping(val3, s2);
            Assert.IsNotNull(p2);

            // Get all mappings in shard map.
            int count = 0;
            IEnumerable<PointMapping<DateTime>> allMappings = lsm.GetMappings();
            using (IEnumerator<PointMapping<DateTime>> mEnum = allMappings.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(3, count);

            // Get all mappings in specified range.
            Range<DateTime> wantedRange = new Range<DateTime>(val3.AddMinutes(-5), val3.AddMinutes(15));
            count = 0;
            IEnumerable<PointMapping<DateTime>> mappingsInRange = lsm.GetMappings(wantedRange);
            using (IEnumerator<PointMapping<DateTime>> mEnum = mappingsInRange.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(2, count);

            // Get all mappings for a shard.
            count = 0;
            IEnumerable<PointMapping<DateTime>> mappingsForShard = lsm.GetMappings(s1);
            using (IEnumerator<PointMapping<DateTime>> mEnum = mappingsForShard.GetEnumerator())
            {
                while (mEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(2, count);

            // Get all mappings in specified range for a particular shard.
            count = 0;
            IEnumerable<PointMapping<DateTime>> mappingsInRangeForShard = lsm.GetMappings(wantedRange, s1);
            using (IEnumerator<PointMapping<DateTime>> mEnum = mappingsInRangeForShard.GetEnumerator())
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
        public void DateAddPointMappingDuplicate()
        {
            CountingCacheStore countingCache = new CountingCacheStore(new CacheStore());

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StoreOperationFactory(),
                countingCache,
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ListShardMap<DateTime> lsm = smm.GetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            DateTime val = DateTime.Now;
            PointMapping<DateTime> p1 = lsm.CreatePointMapping(val, s);

            Assert.IsNotNull(p1);

            bool addFailed = false;
            try
            {
                // add same point mapping again.
                PointMapping<DateTime> pNew = lsm.CreatePointMapping(val, s);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.MappingPointAlreadyMapped, sme.ErrorCode);
                addFailed = true;
            }

            Assert.IsTrue(addFailed);

            PointMapping<DateTime> p2 = lsm.GetMappingForKey(val);

            Assert.IsNotNull(p2);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);
        }

        /// <summary>
        /// Delete existing point mapping from list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DateDeletePointMappingDefault()
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

            ListShardMap<DateTime> lsm = smm.GetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            DateTime val = DateTime.Now;
            PointMapping<DateTime> p1 = lsm.CreatePointMapping(val, s);

            PointMapping<DateTime> p2 = lsm.GetMappingForKey(val);

            Assert.IsNotNull(p2);
            Assert.AreEqual(0, countingCache.LookupMappingHitCount);

            // The mapping must be made offline first before it can be deleted.
            PointMappingUpdate ru = new PointMappingUpdate();
            ru.Status = MappingStatus.Offline;

            PointMapping<DateTime> mappingToDelete = lsm.UpdateMapping(p1, ru);

            lsm.DeleteMapping(mappingToDelete);

            // Try to get from store. Because the mapping is missing from the store, we will try to
            // invalidate the cache, but since it is also missing from cache there will be an cache miss.
            bool lookupFailed = false;
            try
            {
                PointMapping<DateTime> pLookup = lsm.GetMappingForKey(val);
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
        public void DateDeletePointMappingNonExisting()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<DateTime> lsm = smm.GetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[0]);

            Shard s = lsm.CreateShard(sl);

            Assert.IsNotNull(s);

            DateTime val = DateTime.Now;
            PointMapping<DateTime> p1 = lsm.CreatePointMapping(val, s);

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
        #endregion

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

        #endregion Helper Methods
    }
}
