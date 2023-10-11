// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ClientTestCommon;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Decorators;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests;

/// <summary>
/// Test related to ShardMapManager class and it's methods.
/// </summary>
[TestClass]
public class ShardMapManagerTests
{
    /// <summary>
    /// Shard map name used in the tests.
    /// </summary>
    private static readonly string s_shardMapName = "Customer";

    #region Common Methods

    /// <summary>
    /// Initializes common state for tests in this class.
    /// </summary>
    /// <param name="testContext">The TestContext we are running in.</param>
    [ClassInitialize()]
    public static void ShardMapManagerTestsInitialize(TestContext testContext)
    {
        // Clear all connection pools.
        SqlConnection.ClearAllPools();

        using (var conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
        {
            conn.Open();

            // Create ShardMapManager database
            using var cmd = new SqlCommand(
                string.Format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                conn);
            _ = cmd.ExecuteNonQuery();
        }

        // Create the shard map manager.
        _ = ShardMapManagerFactory.CreateSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerCreateMode.ReplaceExisting);
    }

    /// <summary>
    /// Cleans up common state for the all tests in this class.
    /// </summary>
    [ClassCleanup()]
    public static void ShardMapManagerTestsCleanup()
    {
        // Clear all connection pools.
        SqlConnection.ClearAllPools();

        using var conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString);
        conn.Open();
        using var cmd = new SqlCommand(
            string.Format(Globals.DropDatabaseQuery, Globals.ShardMapManagerDatabaseName),
            conn);
        _ = cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Initializes common state per-test.
    /// </summary>
    [TestInitialize()]
    public void ShardMapManagerTestInitialize()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
                                Globals.ShardMapManagerConnectionString,
                                ShardMapManagerLoadPolicy.Lazy);
        try
        {
            var sm = smm.GetShardMap(ShardMapManagerTests.s_shardMapName);
            smm.DeleteShardMap(sm);
        }
        catch (ShardManagementException smme)
        {
            Assert.AreEqual(ShardManagementErrorCode.ShardMapLookupFailure, smme.ErrorCode);
        }
    }

    /// <summary>
    /// Cleans up common state per-test.
    /// </summary>
    [TestCleanup()]
    public void ShardMapManagerTestCleanup()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);
        try
        {
            var sm = smm.GetShardMap(ShardMapManagerTests.s_shardMapName);
            smm.DeleteShardMap(sm);
        }
        catch (ShardManagementException smme)
        {
            Assert.AreEqual(ShardManagementErrorCode.ShardMapLookupFailure, smme.ErrorCode);
        }
    }

    //#if DEBUG
    //        /// <summary>
    //        /// Subscriber to SqlStore.SqlStoreEvent event.
    //        /// </summary>
    //        /// <param name="sender">sender object (SqlStore)</param>
    //        /// <param name="arg">SqlStoreEvent event args to set TxnAbort.</param>
    //        void UpdateSqlStoreEvent(object sender, SqlStoreEventArgs arg)
    //        {
    //            Assert.IsNotNull(arg);
    //            arg.action = SqlStoreEventArgs.SqlStoreTxnFinishAction.TxnAbort;
    //        }

    //#endif // DEBUG

    #endregion Common Methods

    /// <summary>
    /// Get all shard maps from shard map manager.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void GetShardMapsDefault()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

        ShardMap sm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(sm);

        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, sm.Name);

        var shardmaps = smm.GetShardMaps();

        var count = 0;
        using (var mEnum = shardmaps.GetEnumerator())
        {
            while (mEnum.MoveNext())
                count++;
        }

        Assert.AreEqual(1, count);
    }

    /// <summary>
    /// Remove a default shard map from shard map manager.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void DeleteListShardMap()
    {
        var cacheStore =
            new CountingCacheStore(
                new CacheStore());

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StoreOperationFactory(),
            cacheStore,
            ShardMapManagerLoadPolicy.Lazy,
            RetryPolicy.DefaultRetryPolicy,
            RetryBehavior.DefaultRetryBehavior);

        ShardMap sm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(sm);

        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, sm.Name);

        var smLookup = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

        Assert.IsNotNull(smLookup);
        Assert.AreEqual(1, cacheStore.LookupShardMapCount);
        Assert.AreEqual(1, cacheStore.LookupShardMapHitCount);

        smm.DeleteShardMap(sm);

        Assert.AreEqual(1, cacheStore.DeleteShardMapCount);

        cacheStore.ResetCounters();

        // Verify that shard map is removed from cache.
        var smLookupFailure = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

        Assert.IsNull(smLookupFailure);
        Assert.AreEqual(1, cacheStore.LookupShardMapCount);
        Assert.AreEqual(1, cacheStore.LookupShardMapMissCount);
    }

    /// <summary>
    /// Remove non-existing shard map
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void DeleteShardMapNonExisting()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

        ShardMap sm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(sm);

        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, sm.Name);

        smm.DeleteShardMap(sm);

        smm.DeleteShardMap(sm);
    }

    /// <summary>
    /// Create list shard map.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void CreateListShardMapDefault()
    {
        var cacheStore =
            new CountingCacheStore(
                new CacheStore());

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StoreOperationFactory(),
            cacheStore,
            ShardMapManagerLoadPolicy.Lazy,
            RetryPolicy.DefaultRetryPolicy, RetryBehavior.DefaultRetryBehavior);

        var lsm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(lsm);

        var smLookup = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);
        Assert.IsNotNull(smLookup);
        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, smLookup.Name);
        Assert.AreEqual(1, cacheStore.LookupShardMapCount);
        Assert.AreEqual(1, cacheStore.LookupShardMapHitCount);
    }

    /// <summary>
    /// Add a list shard map with duplicate name to shard map manager.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void CreateListShardMapDuplicate()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

        ShardMap sm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(sm);

        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, sm.Name);

        var creationFailed = false;

        try
        {
            var lsm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);
        }
        catch (ShardManagementException sme)
        {
            Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
            Assert.AreEqual(ShardManagementErrorCode.ShardMapAlreadyExists, sme.ErrorCode);
            creationFailed = true;
        }

        Assert.IsTrue(creationFailed);
    }

    /// <summary>
    /// Create range shard map.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void CreateRangeShardMapDefault()
    {
        var cacheStore =
            new CountingCacheStore(
                new CacheStore());

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StoreOperationFactory(),
            cacheStore,
            ShardMapManagerLoadPolicy.Lazy,
            RetryPolicy.DefaultRetryPolicy,
            RetryBehavior.DefaultRetryBehavior);

        var rsm = smm.CreateRangeShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(rsm);
        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, rsm.Name);

        var smLookup = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

        Assert.IsNotNull(smLookup);
        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, smLookup.Name);
        Assert.AreEqual(1, cacheStore.LookupShardMapCount);
        Assert.AreEqual(1, cacheStore.LookupShardMapHitCount);
    }

    /// <summary>
    /// Add a range shard map with duplicate name to shard map manager.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void CreateRangeShardMapDuplicate()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

        ShardMap sm = smm.CreateRangeShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(sm);

        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, sm.Name);

        var creationFailed = false;

        try
        {
            var rsm = smm.CreateRangeShardMap<int>(ShardMapManagerTests.s_shardMapName);
        }
        catch (ShardManagementException sme)
        {
            Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
            Assert.AreEqual(ShardManagementErrorCode.ShardMapAlreadyExists, sme.ErrorCode);
            creationFailed = true;
        }

        Assert.IsTrue(creationFailed);
    }

    [TestMethod]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void TestShardMapManagerExceptionSerializability()
    {
        var errorCategory = ShardManagementErrorCategory.RangeShardMap;
        var errorCode = ShardManagementErrorCode.ShardMapDoesNotExist;

        var ex = new ShardManagementException(errorCategory, errorCode, "Testing");
        var deserialized = CommonTestUtils.SerializeDeserialize(ex);

        // Validate
        Assert.AreEqual(ex.ErrorCode, deserialized.ErrorCode, "ErrorCode");
        Assert.AreEqual(ex.ErrorCategory, deserialized.ErrorCategory, "ErrorCategory");
        Assert.AreEqual(ex.ToString(), deserialized.ToString(), "ToString()");
    }

    #region GsmAbortTests

    private class NTimeFailingAddShardMapGlobalOperation : AddShardMapGlobalOperation
    {
        private readonly int _failureCountMax;
        private int _currentFailureCount;

        internal NTimeFailingAddShardMapGlobalOperation(
            int failureCountMax,
            ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap) :
            base(shardMapManager, operationName, shardMap)
        {
            _failureCountMax = failureCountMax;
            _currentFailureCount = 0;
        }

        public override IStoreResults DoGlobalExecute(IStoreTransactionScope ts)
        {
            if (_currentFailureCount < _failureCountMax)
            {
                _currentFailureCount++;

                throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
            }
            else
            {
                return base.DoGlobalExecute(ts);
            }
        }
    }

    private class NTimeFailingRemoveShardMapGlobalOperation : AddShardMapGlobalOperation
    {
        private readonly int _failureCountMax;
        private int _currentFailureCount;

        internal NTimeFailingRemoveShardMapGlobalOperation(
            int failureCountMax,
            ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap) :
            base(shardMapManager, operationName, shardMap)
        {
            _failureCountMax = failureCountMax;
            _currentFailureCount = 0;
        }

        public override IStoreResults DoGlobalExecute(IStoreTransactionScope ts)
        {
            if (_currentFailureCount < _failureCountMax)
            {
                _currentFailureCount++;

                throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
            }
            else
            {
                return base.DoGlobalExecute(ts);
            }
        }
    }

    /// <summary>
    /// Remove a default shard map from shard map manager, do not commit GSM transaction.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void RemoveListShardMapAbortGSM()
    {
        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateRemoveShardMapGlobalOperationShardMapManagerStringIStoreShardMap =
                    (_smm, _opname, _ssm) => new NTimeFailingRemoveShardMapGlobalOperation(10, _smm, _opname, _ssm)
            },
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

        ShardMap sm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(sm);

        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, sm.Name);

        var storeOperationFailed = false;
        try
        {
            smm.DeleteShardMap(sm);
        }
        catch (ShardManagementException sme)
        {
            Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
            Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
            storeOperationFailed = true;
        }

        Assert.IsTrue(storeOperationFailed);

        // Verify that shard map still exist in store.
        var smNew = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, false);
        Assert.IsNotNull(smNew);
    }

    /// <summary>
    /// Create list shard map, do not commit GSM transaction.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void CreateListShardMapAbortGSM()
    {
        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap =
                    (_smm, _opname, _ssm) => new NTimeFailingAddShardMapGlobalOperation(10, _smm, _opname, _ssm)
            },
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

        var storeOperationFailed = false;
        try
        {
            var lsm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);
            Assert.IsNotNull(lsm);
            Assert.AreEqual(ShardMapManagerTests.s_shardMapName, lsm.Name);
        }
        catch (ShardManagementException sme)
        {
            Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
            Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
            storeOperationFailed = true;
        }

        Assert.IsTrue(storeOperationFailed);
    }

    /// <summary>
    /// Create range shard map, do not commit GSM transaction.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void CreateRangeShardMapAbortGSM()
    {
        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap =
                    (_smm, _opname, _ssm) => new NTimeFailingAddShardMapGlobalOperation(10, _smm, _opname, _ssm)
            },
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

        var storeOperationFailed = false;
        try
        {
            var rsm = smm.CreateRangeShardMap<int>(ShardMapManagerTests.s_shardMapName);
            Assert.IsNotNull(rsm);
            Assert.AreEqual(ShardMapManagerTests.s_shardMapName, rsm.Name);
        }
        catch (ShardManagementException sme)
        {
            Assert.AreEqual(ShardManagementErrorCategory.ShardMapManager, sme.ErrorCategory);
            Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
            storeOperationFailed = true;
        }

        Assert.IsTrue(storeOperationFailed);
    }

    #endregion GsmAbortTests

    #region CacheAbortTests

    /// <summary>
    /// Add a list shard map to shard map manager, do not add it to cache.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void AddListShardMapNoCacheUpdate()
    {
        // Create a cache store that always misses.
        var cacheStore =
            new CountingCacheStore(
                new StubCacheStore()
                {
                    CallBase = true,
                    LookupMappingByKeyIStoreShardMapShardKey = (ssm, sk) => null,
                    LookupShardMapByNameString = (n) => null
                });

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StoreOperationFactory(),
            cacheStore,
            ShardMapManagerLoadPolicy.Lazy,
            RetryPolicy.DefaultRetryPolicy, RetryBehavior.DefaultRetryBehavior);

        ShardMap sm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);
        Assert.IsNotNull(sm);
        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, sm.Name);
        Assert.AreEqual(1, cacheStore.AddShardMapCount);
        cacheStore.ResetCounters();

        var smLookup = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

        Assert.IsNotNull(smLookup);
        Assert.AreEqual(1, cacheStore.AddShardMapCount);
        Assert.AreEqual(1, cacheStore.LookupShardMapMissCount);
    }

    /// <summary>
    /// Remove a default shard map from shard map manager, do not remove it from cache.
    /// </summary>
    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void RemoveListShardMapNoCacheUpdate()
    {
        // Counting store that does not perform deletions of shard maps.
        var cacheStore =
            new CountingCacheStore(
                new StubCacheStore()
                {
                    CallBase = true,
                    DeleteShardMapIStoreShardMap = (csm) => { }
                }
                );

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StoreOperationFactory(),
            cacheStore,
            ShardMapManagerLoadPolicy.Lazy,
            RetryPolicy.DefaultRetryPolicy, RetryBehavior.DefaultRetryBehavior);

        ShardMap sm = smm.CreateListShardMap<int>(ShardMapManagerTests.s_shardMapName);

        Assert.IsNotNull(sm);

        Assert.AreEqual(ShardMapManagerTests.s_shardMapName, sm.Name);

        smm.DeleteShardMap(sm);

        Assert.AreEqual(1, cacheStore.DeleteShardMapCount);

        var smLookup = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

        Assert.IsNotNull(smLookup);
        Assert.AreEqual(1, cacheStore.LookupShardMapHitCount);
    }

    #endregion CacheAbortTests

    #region ShardLocationTests

    [TestMethod]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void TestShardLocationPort()
    {
        var serverName = "testservername";
        var databaseName = "testdatabasename";
        var protocol = SqlProtocol.Default;

        // Below valid range
        _ = AssertExtensions.AssertThrows<ArgumentOutOfRangeException>(
            () => new ShardLocation(serverName, databaseName, protocol, int.MinValue));
        _ = AssertExtensions.AssertThrows<ArgumentOutOfRangeException>(
            () => new ShardLocation(serverName, databaseName, protocol, -1));

        // In valid range
        _ = new ShardLocation(serverName, databaseName, protocol, 0);
        _ = new ShardLocation(serverName, databaseName, protocol, 1);
        _ = new ShardLocation(serverName, databaseName, protocol, 65535);

        // Above valid range
        _ = AssertExtensions.AssertThrows<ArgumentOutOfRangeException>(
            () => new ShardLocation(serverName, databaseName, protocol, 65536));
        _ = AssertExtensions.AssertThrows<ArgumentOutOfRangeException>(
            () => new ShardLocation(serverName, databaseName, protocol, int.MaxValue));
    }

    #endregion
}
