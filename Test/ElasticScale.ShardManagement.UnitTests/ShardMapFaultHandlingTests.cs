// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Reflection;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests;

/// <summary>
/// Test related to ShardMap fault handling.
/// </summary>
[TestClass]
public class ShardMapFaultHandlingTests
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
        var lsm = smm.GetListShardMap<int>(ShardMapFaultHandlingTests.s_listShardMapName);
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

        // Remove all existing mappings from the range shard map.
        var rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);
        Assert.IsNotNull(rsm);

        foreach (var rm in rsm.GetMappings())
        {
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

    /// <summary>
    /// Initializes common state for tests in this class.
    /// </summary>
    /// <param name="testContext">The TestContext we are running in.</param>
    [ClassInitialize()]
    public static void ShardMapFaultHandlingTestsInitialize(TestContext testContext)
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
            for (var i = 0; i < ShardMapFaultHandlingTests.s_shardedDBs.Length; i++)
            {
                using (var cmd = new SqlCommand(
                    string.Format(Globals.DropDatabaseQuery, ShardMapFaultHandlingTests.s_shardedDBs[i]),
                    conn))
                {
                    _ = cmd.ExecuteNonQuery();
                }

                using (var cmd = new SqlCommand(
                    string.Format(Globals.CreateDatabaseQuery, ShardMapFaultHandlingTests.s_shardedDBs[i]),
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

        var lsm = smm.CreateListShardMap<int>(ShardMapFaultHandlingTests.s_listShardMapName);

        Assert.IsNotNull(lsm);

        Assert.AreEqual(ShardMapFaultHandlingTests.s_listShardMapName, lsm.Name);

        // Create range shard map.
        var rsm = smm.CreateRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

        Assert.IsNotNull(rsm);

        Assert.AreEqual(ShardMapFaultHandlingTests.s_rangeShardMapName, rsm.Name);
    }

    /// <summary>
    /// Cleans up common state for the all tests in this class.
    /// </summary>
    [ClassCleanup()]
    public static void ShardMapFaultHandlingTestsCleanup()
    {
        // Clear all connection pools.
        SqlConnection.ClearAllPools();

        using var conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString);
        conn.Open();
        // Drop shard databases
        for (var i = 0; i < ShardMapFaultHandlingTests.s_shardedDBs.Length; i++)
        {
            using var cmd = new SqlCommand(
                string.Format(Globals.DropDatabaseQuery, ShardMapFaultHandlingTests.s_shardedDBs[i]),
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
    public void ShardMapperTestInitialize() => ShardMapFaultHandlingTests.CleanShardMapsHelper();

    /// <summary>
    /// Cleans up common state per-test.
    /// </summary>
    [TestCleanup()]
    public void ShardMapperTestCleanup() => ShardMapFaultHandlingTests.CleanShardMapsHelper();

    #endregion Common Methods

    private class NTimeFailingAddMappingOperation : AddMappingOperation
    {
        private readonly int _failureCountMax;
        private int _currentFailureCount;

        internal NTimeFailingAddMappingOperation(
            int failureCountMax,
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping) :
            base(shardMapManager, operationCode, shardMap, mapping)
        {
            _failureCountMax = failureCountMax;
            _currentFailureCount = 0;
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            if (_currentFailureCount < _failureCountMax)
            {
                _currentFailureCount++;
                throw new StoreException("", TransientSqlException);
            }
            else
            {
                return base.DoGlobalPostLocalExecute(ts);
            }
        }
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void AddPointMappingFailGSMAfterSuccessLSMSingleRetry()
    {
        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                    (_smm, _opcode, _ssm, _sm) => new NTimeFailingAddMappingOperation(1, _smm, _opcode, _ssm, _sm)
            },
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

        var lsm = smm.GetListShardMap<int>(ShardMapFaultHandlingTests.s_listShardMapName);

        Assert.IsNotNull(lsm);

        var s = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        Assert.IsNotNull(s);

        var failed = false;

        try
        {
            var p1 = lsm.CreatePointMapping(2, s);
        }
        catch (ShardManagementException)
        {
            failed = true;
        }

        Assert.IsFalse(failed);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void AddPointMappingFailGSMAfterSuccessLSM()
    {
        var ssof = new StubStoreOperationFactory()
        {
            CallBase = true,
            CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                (_smm, _opcode, _ssm, _sm) => new NTimeFailingAddMappingOperation(2, _smm, _opcode, _ssm, _sm)
        };

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            ssof,
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
            RetryBehavior.DefaultRetryBehavior);

        var lsm = smm.GetListShardMap<int>(ShardMapFaultHandlingTests.s_listShardMapName);

        Assert.IsNotNull(lsm);

        var s = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        Assert.IsNotNull(s);

        var failed = false;

        try
        {
            // Inject GSM transaction failure at GSM commit time.
            var p1 = lsm.CreatePointMapping(2, s);
        }
        catch (ShardManagementException)
        {
            failed = true;
        }

        Assert.IsTrue(failed);

        failed = false;

        ssof.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping = null;

        try
        {
            var p1 = lsm.CreatePointMapping(2, s);
        }
        catch (ShardManagementException)
        {
            failed = true;
        }

        Assert.IsFalse(failed);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void AddRangeMappingFailGSMAfterSuccessLSMSingleRetry()
    {
        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                    (_smm, _opcode, _ssm, _sm) => new NTimeFailingAddMappingOperation(1, _smm, _opcode, _ssm, _sm)
            },
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

        var rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

        Assert.IsNotNull(rsm);

        var s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        Assert.IsNotNull(s);

        var failed = false;

        try
        {
            // Inject GSM transaction failure at GSM commit time.
            var r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
        }
        catch (ShardManagementException)
        {
            failed = true;
        }

        Assert.IsFalse(failed);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void AddRangeMappingFailGSMAfterSuccessLSM()
    {
        var ssof = new StubStoreOperationFactory()
        {
            CallBase = true,
            CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                (_smm, _opcode, _ssm, _sm) => new NTimeFailingAddMappingOperation(2, _smm, _opcode, _ssm, _sm)
        };

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            ssof,
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

        var rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

        Assert.IsNotNull(rsm);

        var s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        Assert.IsNotNull(s);

        var failed = false;

        try
        {
            // Inject GSM transaction failure at GSM commit time.
            var r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
        }
        catch (ShardManagementException)
        {
            failed = true;
        }

        Assert.IsTrue(failed);

        failed = false;

        ssof.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping = null;

        try
        {
            var r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
        }
        catch (ShardManagementException)
        {
            failed = true;
        }

        Assert.IsFalse(failed);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void ShardMapOperationsFailureAfterGlobalPreLocal()
    {
        var ssof = new StubStoreOperationFactory()
        {
            CallBase = true,

            CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
            (_smm, _sm, _s) => new AddShardOperationFailAfterGlobalPreLocal(_smm, _sm, _s),

            CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
            (_smm, _sm, _s) => new RemoveShardOperationFailAfterGlobalPreLocal(_smm, _sm, _s),

            CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard =
            (_smm, _sm, _sold, _snew) => new UpdateShardOperationFailAfterGlobalPreLocal(_smm, _sm, _sold, _snew),

            CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
            (_smm, _opcode, _ssm, _sm) => new AddMappingOperationFailAfterGlobalPreLocal(_smm, _opcode, _ssm, _sm),

            CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid =
            (_smm, _opcode, _sm, _mapping, _loid) => new RemoveMappingOperationFailAfterGlobalPreLocal(_smm, _opcode, _sm, _mapping, _loid),

            CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
            (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                => new UpdateMappingOperationFailAfterGlobalPreLocal(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection),

            CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
            (_smm, _opcode, _sm, _mappingsource, _mappingtarget) => new ReplaceMappingsOperationFailAfterGlobalPreLocal(_smm, _opcode, _sm, _mappingsource, _mappingtarget)
        };

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            ssof,
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
            RetryBehavior.DefaultRetryBehavior);

        var rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

        Assert.IsNotNull(rsm);

        // test undo operations on shard

        // global pre-local only create shard
        var stemp = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        // now creating shard with GSM and LSM operations
        ssof.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
        var s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        // global pre-local only update shard

        _ = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

        // now update shard with GSM and LSM operations
        ssof.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard = null;
        var sNew = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

        // global pre-local only remove shard
        rsm.DeleteShard(sNew);

        // now remove with GSM and LSM operations
        ssof.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
        rsm.DeleteShard(sNew);

        // test undo operations for shard mapings

        var s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        Assert.IsNotNull(s1);

        var s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[1]));

        Assert.IsNotNull(s2);

        // first add mapping will just execute global pre-local and add operation into pending operations log
        var rtemp = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

        ssof.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping = null;

        // now add mapping will succeed after undoing pending operation

        var r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

        Assert.IsNotNull(r1);

        var ru = new RangeMappingUpdate { Status = MappingStatus.Offline };

        // below call will only execute global pre-local step to create operations log
        var r2 = rsm.UpdateMapping(r1, ru);

        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;

        // now update same mapping again, this will undo previous pending operation and then add this mapping

        var r3 = rsm.UpdateMapping(r1, ru);

        // try mapping update failures with change in shard location
        // first reset CreateUpdateMappingOperation to just perform global pre-local
        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
            (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                => new UpdateMappingOperationFailAfterGlobalPreLocal(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection);

        var r4 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

        // now try with actual update mapping operation
        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;
        var r5 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

        // split mapping toperform gsm-only pre-local operation

        var rlisttemp = rsm.SplitMapping(r5, 5);

        // try actual operation which will undo previous pending op
        ssof.CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray = null;

        var rlist = rsm.SplitMapping(r5, 5);

        // remove mapping to create operations log and then exit
        rsm.DeleteMapping(rlist[0]);

        ssof.CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid = null;

        // now actually remove the mapping
        rsm.DeleteMapping(rlist[0]);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void ShardMapOperationsFailureAfterLocalSource()
    {
        var ssof = new StubStoreOperationFactory()
        {
            CallBase = true,

            CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
            (_smm, _sm, _s) => new AddShardOperationFailAfterLocalSource(_smm, _sm, _s),

            CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
            (_smm, _sm, _s) => new RemoveShardOperationFailAfterLocalSource(_smm, _sm, _s),

            CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard =
            (_smm, _sm, _sold, _snew) => new UpdateShardOperationFailAfterLocalSource(_smm, _sm, _sold, _snew),

            CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
            (_smm, _opcode, _ssm, _sm) => new AddMappingOperationFailAfterLocalSource(_smm, _opcode, _ssm, _sm),

            CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid =
            (_smm, _opcode, _sm, _mapping, _loid) => new RemoveMappingOperationFailAfterLocalSource(_smm, _opcode, _sm, _mapping, _loid),

            CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
            (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                => new UpdateMappingOperationFailAfterLocalSource(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection),

            CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
            (_smm, _opcode, _sm, _mappingsource, _mappingtarget) => new ReplaceMappingsOperationFailAfterLocalSource(_smm, _opcode, _sm, _mappingsource, _mappingtarget)
        };

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            ssof,
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
            RetryBehavior.DefaultRetryBehavior);

        var rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

        Assert.IsNotNull(rsm);

        // test undo operations on shard

        // global pre-local only create shard
        var stemp = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        // now creating shard with GSM and LSM operations
        ssof.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
        var s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        // global pre-local only update shard

        _ = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

        // now update shard with GSM and LSM operations
        ssof.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard = null;
        var sNew = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

        // global pre-local only remove shard
        rsm.DeleteShard(sNew);

        // now remove with GSM and LSM operations
        ssof.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
        rsm.DeleteShard(sNew);

        // test undo operations for shard mapings

        var s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        Assert.IsNotNull(s1);

        var s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[1]));

        Assert.IsNotNull(s2);

        // first add mapping will just execute global pre-local and add operation into pending operations log
        var rtemp = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

        ssof.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping = null;

        // now add mapping will succeed after undoing pending operation

        var r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

        Assert.IsNotNull(r1);

        var ru = new RangeMappingUpdate { Status = MappingStatus.Offline };

        // below call will only execute global pre-local step to create operations log
        var r2 = rsm.UpdateMapping(r1, ru);

        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;

        // now update same mapping again, this will undo previous pending operation and then add this mapping

        var r3 = rsm.UpdateMapping(r1, ru);

        // try mapping update failures with change in shard location
        // first reset CreateUpdateMappingOperation to just perform global pre-local
        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
            (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                => new UpdateMappingOperationFailAfterLocalSource(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection);

        var r4 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

        // now try with actual update mapping operation
        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;
        var r5 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

        // split mapping toperform gsm-only pre-local operation

        var rlisttemp = rsm.SplitMapping(r5, 5);

        // try actual operation which will undo previous pending op
        ssof.CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray = null;

        var rlist = rsm.SplitMapping(r5, 5);

        // remove mapping to create operations log and then exit
        rsm.DeleteMapping(rlist[0]);

        ssof.CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid = null;

        // now actually remove the mapping
        rsm.DeleteMapping(rlist[0]);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void ShardMapOperationsFailureAfterLocalTarget()
    {
        var ssof = new StubStoreOperationFactory()
        {
            CallBase = true,

            CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
            (_smm, _sm, _s) => new AddShardOperationFailAfterLocalTarget(_smm, _sm, _s),

            CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
            (_smm, _sm, _s) => new RemoveShardOperationFailAfterLocalTarget(_smm, _sm, _s),

            CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard =
            (_smm, _sm, _sold, _snew) => new UpdateShardOperationFailAfterLocalTarget(_smm, _sm, _sold, _snew),

            CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
            (_smm, _opcode, _ssm, _sm) => new AddMappingOperationFailAfterLocalTarget(_smm, _opcode, _ssm, _sm),

            CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid =
            (_smm, _opcode, _sm, _mapping, _loid) => new RemoveMappingOperationFailAfterLocalTarget(_smm, _opcode, _sm, _mapping, _loid),

            CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
            (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                => new UpdateMappingOperationFailAfterLocalTarget(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection),

            CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray =
            (_smm, _opcode, _sm, _mappingsource, _mappingtarget) => new ReplaceMappingsOperationFailAfterLocalTarget(_smm, _opcode, _sm, _mappingsource, _mappingtarget)
        };

        var smm = new ShardMapManager(
            new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
            new SqlStoreConnectionFactory(),
            ssof,
            new CacheStore(),
            ShardMapManagerLoadPolicy.Lazy,
            new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
            RetryBehavior.DefaultRetryBehavior);

        var rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

        Assert.IsNotNull(rsm);

        // test undo operations on shard

        // global pre-local only create shard
        var stemp = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        // now creating shard with GSM and LSM operations
        ssof.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
        var s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        // global pre-local only update shard

        _ = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

        // now update shard with GSM and LSM operations
        ssof.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard = null;
        var sNew = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

        // global pre-local only remove shard
        rsm.DeleteShard(sNew);

        // now remove with GSM and LSM operations
        ssof.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
        rsm.DeleteShard(sNew);

        // test undo operations for shard mapings

        var s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

        Assert.IsNotNull(s1);

        var s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[1]));

        Assert.IsNotNull(s2);

        // first add mapping will just execute global pre-local and add operation into pending operations log
        var rtemp = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

        ssof.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping = null;

        // now add mapping will succeed after undoing pending operation

        var r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

        Assert.IsNotNull(r1);

        var ru = new RangeMappingUpdate { Status = MappingStatus.Offline };

        // below call will only execute global pre-local step to create operations log
        var r2 = rsm.UpdateMapping(r1, ru);

        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;

        // now update same mapping again, this will undo previous pending operation and then add this mapping

        var r3 = rsm.UpdateMapping(r1, ru);

        // try mapping update failures with change in shard location
        // first reset CreateUpdateMappingOperation to just perform global pre-local
        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
            (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                => new UpdateMappingOperationFailAfterLocalTarget(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection);

        var r4 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

        // now try with actual update mapping operation
        ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;
        var r5 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

        // split mapping toperform gsm-only pre-local operation

        var rlisttemp = rsm.SplitMapping(r5, 5);

        // try actual operation which will undo previous pending op
        ssof.CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray = null;

        var rlist = rsm.SplitMapping(r5, 5);

        // remove mapping to create operations log and then exit
        rsm.DeleteMapping(rlist[0]);

        ssof.CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid = null;

        // now actually remove the mapping
        rsm.DeleteMapping(rlist[0]);
    }

    private class AddShardOperationFailAfterGlobalPreLocal : AddShardOperation
    {
        internal AddShardOperationFailAfterGlobalPreLocal(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
            : base(
            shardMapManager,
            shardMap,
            shard)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class RemoveShardOperationFailAfterGlobalPreLocal : RemoveShardOperation
    {
        internal RemoveShardOperationFailAfterGlobalPreLocal(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
            : base(
            shardMapManager,
            shardMap,
            shard)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class UpdateShardOperationFailAfterGlobalPreLocal : UpdateShardOperation
    {
        internal UpdateShardOperationFailAfterGlobalPreLocal(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shardOld,
            IStoreShard shardNew)
            : base(
            shardMapManager,
            shardMap,
            shardOld,
            shardNew)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class AddMappingOperationFailAfterGlobalPreLocal : AddMappingOperation
    {
        internal AddMappingOperationFailAfterGlobalPreLocal(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mapping
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class RemoveMappingOperationFailAfterGlobalPreLocal : RemoveMappingOperation
    {
        internal RemoveMappingOperationFailAfterGlobalPreLocal(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockOwnerId)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mapping,
            lockOwnerId
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class UpdateMappingOperationFailAfterGlobalPreLocal : UpdateMappingOperation
    {
        internal UpdateMappingOperationFailAfterGlobalPreLocal(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget,
            string patternForKill,
            Guid lockOwnerId,
            bool killConnection)
        : base(
            shardMapManager,
            operationCode,
            shardMap,
            mappingSource,
            mappingTarget,
            patternForKill,
            lockOwnerId,
            killConnection)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class ReplaceMappingsOperationFailAfterGlobalPreLocal : ReplaceMappingsOperation
    {
        internal ReplaceMappingsOperationFailAfterGlobalPreLocal(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            Tuple<IStoreMapping, Guid>[] mappingsSource,
            Tuple<IStoreMapping, Guid>[] mappingsTarget)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mappingsSource,
            mappingsTarget
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class AddShardOperationFailAfterLocalSource : AddShardOperation
    {
        internal AddShardOperationFailAfterLocalSource(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
            : base(
            shardMapManager,
            shardMap,
            shard)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class RemoveShardOperationFailAfterLocalSource : RemoveShardOperation
    {
        internal RemoveShardOperationFailAfterLocalSource(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
            : base(
            shardMapManager,
            shardMap,
            shard)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class UpdateShardOperationFailAfterLocalSource : UpdateShardOperation
    {
        internal UpdateShardOperationFailAfterLocalSource(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shardOld,
            IStoreShard shardNew)
            : base(
            shardMapManager,
            shardMap,
            shardOld,
            shardNew)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class AddMappingOperationFailAfterLocalSource : AddMappingOperation
    {
        internal AddMappingOperationFailAfterLocalSource(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mapping
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class RemoveMappingOperationFailAfterLocalSource : RemoveMappingOperation
    {
        internal RemoveMappingOperationFailAfterLocalSource(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockOwnerId)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mapping,
            lockOwnerId
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class UpdateMappingOperationFailAfterLocalSource : UpdateMappingOperation
    {
        internal UpdateMappingOperationFailAfterLocalSource(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget,
            string patternForKill,
            Guid lockOwnerId,
            bool killConnection)
            : base(
                shardMapManager,
                operationCode,
                shardMap,
                mappingSource,
                mappingTarget,
                patternForKill,
                lockOwnerId,
                killConnection)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class ReplaceMappingsOperationFailAfterLocalSource : ReplaceMappingsOperation
    {
        internal ReplaceMappingsOperationFailAfterLocalSource(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            Tuple<IStoreMapping, Guid>[] mappingsSource,
            Tuple<IStoreMapping, Guid>[] mappingsTarget)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mappingsSource,
            mappingsTarget
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }

        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class AddShardOperationFailAfterLocalTarget : AddShardOperation
    {
        internal AddShardOperationFailAfterLocalTarget(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
            : base(
            shardMapManager,
            shardMap,
            shard)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class RemoveShardOperationFailAfterLocalTarget : RemoveShardOperation
    {
        internal RemoveShardOperationFailAfterLocalTarget(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shard)
            : base(
            shardMapManager,
            shardMap,
            shard)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class UpdateShardOperationFailAfterLocalTarget : UpdateShardOperation
    {
        internal UpdateShardOperationFailAfterLocalTarget(
            ShardMapManager shardMapManager,
            IStoreShardMap shardMap,
            IStoreShard shardOld,
            IStoreShard shardNew)
            : base(
            shardMapManager,
            shardMap,
            shardOld,
            shardNew)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class AddMappingOperationFailAfterLocalTarget : AddMappingOperation
    {
        internal AddMappingOperationFailAfterLocalTarget(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mapping
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class RemoveMappingOperationFailAfterLocalTarget : RemoveMappingOperation
    {
        internal RemoveMappingOperationFailAfterLocalTarget(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mapping,
            Guid lockOwnerId)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mapping,
            lockOwnerId
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class UpdateMappingOperationFailAfterLocalTarget : UpdateMappingOperation
    {
        internal UpdateMappingOperationFailAfterLocalTarget(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            IStoreMapping mappingSource,
            IStoreMapping mappingTarget,
            string patternForKill,
            Guid lockOwnerId,
            bool killConnection)
            : base(
                shardMapManager,
                operationCode,
                shardMap,
                mappingSource,
                mappingTarget,
                patternForKill,
                lockOwnerId,
                killConnection)
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    private class ReplaceMappingsOperationFailAfterLocalTarget : ReplaceMappingsOperation
    {
        internal ReplaceMappingsOperationFailAfterLocalTarget(
            ShardMapManager shardMapManager,
            StoreOperationCode operationCode,
            IStoreShardMap shardMap,
            Tuple<IStoreMapping, Guid>[] mappingsSource,
            Tuple<IStoreMapping, Guid>[] mappingsTarget)
            : base(
            shardMapManager,
            operationCode,
            shardMap,
            mappingsSource,
            mappingsTarget
            )
        {
        }

        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            var results = new SqlResults();

            return results;
        }
    }

    internal static SqlException TransientSqlException = ShardMapFaultHandlingTests.CreateSqlException();

    private static SqlException CreateSqlException()
    {
        var cisSqlError = typeof(SqlError)
            .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic);

        var ciSqlError = cisSqlError.Single(c => c.GetParameters().Length == 8);
        var se = (SqlError)ciSqlError.Invoke(new object[] { 10928, (byte)0, (byte)0, "", "", "", 0, null });

        var ciSqlErrorCollection = typeof(SqlErrorCollection)
            .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic).Single();

        var sec = (SqlErrorCollection)ciSqlErrorCollection.Invoke(new object[0]);

        var miSqlErrorCollectionAdd = typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.Instance | BindingFlags.NonPublic);

        _ = miSqlErrorCollectionAdd.Invoke(sec, new object[] { se });

        var miSqlExceptionCreate = typeof(SqlException)
            .GetMethod(
                "CreateException",
                BindingFlags.Static | BindingFlags.NonPublic,
                null,
                new Type[] { typeof(SqlErrorCollection), typeof(string) },
                null);

        var sqlException = (SqlException)miSqlExceptionCreate.Invoke(null, new object[] { sec, "" });

        return sqlException;
    }
}
