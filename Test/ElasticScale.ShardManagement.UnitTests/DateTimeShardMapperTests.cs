// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Decorators;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests;

/// <summary>
/// Test related to ShardMapper class and it's methods.
/// </summary>
[TestClass]
public class DateTimeShardMapperTests
{
    /// <summary>
    /// Sharded databases to create for the test.
    /// </summary>
    private static readonly string[] s_shardedDBs = new[]
    {
        "shard1" + Globals.TestDatabasePostfix, "shard2" + Globals.TestDatabasePostfix
    };

    /// <summary>
    /// List shard map name.
    /// </summary>
    private static readonly string s_listShardMapName = "Customers_list";

    /// <summary>
    /// Range shard map name.
    /// </summary>
    private static readonly string s_rangeShardMapName = "Customers_range";

    #region Common Methods

    /// <summary>
    /// Helper function to clean list and range shard maps.
    /// </summary>
    private static void CleanShardMapsHelper()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

        // Remove all existing mappings from the list shard map.
        if (smm.TryGetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName, out var lsm))
        {
            Assert.IsNotNull(lsm);

            foreach (var pm in lsm.GetMappings())
            {
                var pmOffline = lsm.MarkMappingOffline(pm);
                Assert.IsNotNull(pmOffline);
                lsm.DeleteMapping(pmOffline);
            }

            // Remove all shards from list shard map
            foreach (var s in lsm.GetShards())
            {
                lsm.DeleteShard(s);
            }
        }

        // Remove all existing mappings from the range shard map.
        if (smm.TryGetRangeShardMap<DateTime>(DateTimeShardMapperTests.s_rangeShardMapName, out var rsm))
        {
            Assert.IsNotNull(rsm);

            foreach (var rm in rsm.GetMappings())
            {
                var mappingLockToken = rsm.GetMappingLockOwner(rm);
                rsm.UnlockMapping(rm, mappingLockToken);
                var rmOffline = rsm.MarkMappingOffline(rm);
                Assert.IsNotNull(rmOffline);
                rsm.DeleteMapping(rmOffline);
            }

            // Remove all shards from range shard map
            foreach (var s in rsm.GetShards())
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

        using (var conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
        {
            conn.Open();

            // Create ShardMapManager database
            using (var cmd = new SqlCommand(
                string.Format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                conn))
            {
                _ = cmd.ExecuteNonQuery();
            }

            // Create shard databases
            for (var i = 0; i < DateTimeShardMapperTests.s_shardedDBs.Length; i++)
            {
                using (var cmd = new SqlCommand(
                    string.Format(Globals.DropDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
                    conn))
                {
                    _ = cmd.ExecuteNonQuery();
                }

                using (var cmd = new SqlCommand(
                    string.Format(Globals.CreateDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
                    conn))
                {
                    _ = cmd.ExecuteNonQuery();
                }
            }
        }

        // Create shard map manager.
        _ = ShardMapManagerFactory.CreateSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerCreateMode.ReplaceExisting);

        // Create list shard map.
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

        var lsm = smm.CreateListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

        Assert.IsNotNull(lsm);

        Assert.AreEqual(DateTimeShardMapperTests.s_listShardMapName, lsm.Name);

        // Create range shard map.
        var rsm = smm.CreateRangeShardMap<DateTime>(DateTimeShardMapperTests.s_rangeShardMapName);

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

        using var conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString);
        conn.Open();
        // Drop shard databases
        for (var i = 0; i < DateTimeShardMapperTests.s_shardedDBs.Length; i++)
        {
            using var cmd = new SqlCommand(
                string.Format(Globals.DropDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
                conn);
            _ = cmd.ExecuteNonQuery();
        }

        // Drop shard map manager database
        using (var cmd = new SqlCommand(
            string.Format(Globals.DropDatabaseQuery, Globals.ShardMapManagerDatabaseName),
            conn))
        {
            _ = cmd.ExecuteNonQuery();
        }
    }

    /// <summary>
    /// Initializes common state per-test.
    /// </summary>
    [TestInitialize()]
    public void ShardMapperTestInitialize() => DateTimeShardMapperTests.CleanShardMapsHelper();

    /// <summary>
    /// Cleans up common state per-test.
    /// </summary>
    [TestCleanup()]
    public void ShardMapperTestCleanup() => DateTimeShardMapperTests.CleanShardMapsHelper();

    #endregion Common Methods

    #region WithDates

    /// <summary>
    /// All combinations of getting point mappings from a list shard map
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void DateGetPointMappingsForRange()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

        var lsm = smm.GetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

        Assert.IsNotNull(lsm);

        var s1 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[0]));
        Assert.IsNotNull(s1);

        var s2 = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[1]));
        Assert.IsNotNull(s2);

        var val1 = DateTime.Now.Subtract(TimeSpan.FromMinutes(10));
        var p1 = lsm.CreatePointMapping(val1, s1);
        Assert.IsNotNull(p1);

        var val2 = DateTime.Now.Subtract(TimeSpan.FromMinutes(20));
        var p2 = lsm.CreatePointMapping(val2, s1);
        Assert.IsNotNull(p2);

        var val3 = DateTime.Now.Subtract(TimeSpan.FromMinutes(30));
        _ = lsm.CreatePointMapping(val3, s2);
        Assert.IsNotNull(p2);

        // Get all mappings in shard map.
        var count = 0;
        IEnumerable<PointMapping<DateTime>> allMappings = lsm.GetMappings();
        using (var mEnum = allMappings.GetEnumerator())
        {
            while (mEnum.MoveNext())
                count++;
        }

        Assert.AreEqual(3, count);

        // Get all mappings in specified range.
        var wantedRange = new Range<DateTime>(val3.AddMinutes(-5), val3.AddMinutes(15));
        count = 0;
        IEnumerable<PointMapping<DateTime>> mappingsInRange = lsm.GetMappings(wantedRange);
        using (var mEnum = mappingsInRange.GetEnumerator())
        {
            while (mEnum.MoveNext())
                count++;
        }

        Assert.AreEqual(2, count);

        // Get all mappings for a shard.
        count = 0;
        IEnumerable<PointMapping<DateTime>> mappingsForShard = lsm.GetMappings(s1);
        using (var mEnum = mappingsForShard.GetEnumerator())
        {
            while (mEnum.MoveNext())
                count++;
        }

        Assert.AreEqual(2, count);

        // Get all mappings in specified range for a particular shard.
        count = 0;
        IEnumerable<PointMapping<DateTime>> mappingsInRangeForShard = lsm.GetMappings(wantedRange, s1);
        using (var mEnum = mappingsInRangeForShard.GetEnumerator())
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
        var countingCache = new CountingCacheStore(new CacheStore());

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StoreOperationFactory(),
            countingCache,
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

        var lsm = smm.GetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

        Assert.IsNotNull(lsm);

        var sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[0]);

        var s = lsm.CreateShard(sl);

        Assert.IsNotNull(s);

        var val = DateTime.Now;
        var p1 = lsm.CreatePointMapping(val, s);

        Assert.IsNotNull(p1);

        var addFailed = false;
        try
        {
            // add same point mapping again.
            var pNew = lsm.CreatePointMapping(val, s);
        }
        catch (ShardManagementException sme)
        {
            Assert.AreEqual(ShardManagementErrorCategory.ListShardMap, sme.ErrorCategory);
            Assert.AreEqual(ShardManagementErrorCode.MappingPointAlreadyMapped, sme.ErrorCode);
            addFailed = true;
        }

        Assert.IsTrue(addFailed);

        var p2 = lsm.GetMappingForKey(val);

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
        var countingCache = new CountingCacheStore(new CacheStore());

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StoreOperationFactory(),
            countingCache,
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
            RetryBehavior.DefaultRetryBehavior);

        var lsm = smm.GetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

        Assert.IsNotNull(lsm);

        var sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[0]);

        var s = lsm.CreateShard(sl);

        Assert.IsNotNull(s);

        var val = DateTime.Now;
        var p1 = lsm.CreatePointMapping(val, s);

        var p2 = lsm.GetMappingForKey(val);

        Assert.IsNotNull(p2);
        Assert.AreEqual(0, countingCache.LookupMappingHitCount);

        // The mapping must be made offline first before it can be deleted.
        var ru = new PointMappingUpdate
        {
            Status = MappingStatus.Offline
        };

        var mappingToDelete = lsm.UpdateMapping(p1, ru);

        lsm.DeleteMapping(mappingToDelete);

        // Try to get from store. Because the mapping is missing from the store, we will try to
        // invalidate the cache, but since it is also missing from cache there will be an cache miss.
        var lookupFailed = false;
        try
        {
            var pLookup = lsm.GetMappingForKey(val);
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
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

        var lsm = smm.GetListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

        Assert.IsNotNull(lsm);

        var sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, DateTimeShardMapperTests.s_shardedDBs[0]);

        var s = lsm.CreateShard(sl);

        Assert.IsNotNull(s);

        var val = DateTime.Now;
        var p1 = lsm.CreatePointMapping(val, s);

        Assert.IsNotNull(p1);

        var ru = new PointMappingUpdate
        {
            Status = MappingStatus.Offline
        };

        // The mapping must be made offline before it can be deleted.
        p1 = lsm.UpdateMapping(p1, ru);

        lsm.DeleteMapping(p1);

        var removeFailed = false;

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
        using (var conn = new SqlStoreConnectionFactory().GetConnection(
            StoreConnectionKind.Global,
            new SqlConnectionInfo(
                Globals.ShardMapManagerConnectionString,
                null)))
        {
            conn.Open();

            using var ts = conn.GetTransactionScope(StoreTransactionScopeKind.ReadOnly);
            result = ts.ExecuteCommandSingle(
                new StringBuilder(
                @"select
		                  6, OperationId, OperationCode, Data, UndoStartState, ShardVersionRemoves, ShardVersionAdds
	                      from
		                  __ShardManagement.OperationsLogGlobal"));
        }

        return result.StoreOperations;
    }

    #endregion Helper Methods
}
