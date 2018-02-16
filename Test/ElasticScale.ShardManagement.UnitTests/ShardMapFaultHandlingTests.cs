// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Test related to ShardMap fault handling.
    /// </summary>
    [TestClass]
    public class ShardMapFaultHandlingTests
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
            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapFaultHandlingTests.s_listShardMapName);
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

            // Remove all existing mappings from the range shard map.
            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);
            Assert.IsNotNull(rsm);

            foreach (RangeMapping<int> rm in rsm.GetMappings())
            {
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

        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void ShardMapFaultHandlingTestsInitialize(TestContext testContext)
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
                for (int i = 0; i < ShardMapFaultHandlingTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ShardMapFaultHandlingTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ShardMapFaultHandlingTests.s_shardedDBs[i]),
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

            ListShardMap<int> lsm = smm.CreateListShardMap<int>(ShardMapFaultHandlingTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Assert.AreEqual(ShardMapFaultHandlingTests.s_listShardMapName, lsm.Name);

            // Create range shard map.
            RangeShardMap<int> rsm = smm.CreateRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

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

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();
                // Drop shard databases
                for (int i = 0; i < ShardMapFaultHandlingTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ShardMapFaultHandlingTests.s_shardedDBs[i]),
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
            ShardMapFaultHandlingTests.CleanShardMapsHelper();
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        [TestCleanup()]
        public void ShardMapperTestCleanup()
        {
            ShardMapFaultHandlingTests.CleanShardMapsHelper();
        }

        #endregion Common Methods

        private class NTimeFailingAddMappingOperation : AddMappingOperation
        {
            private int _failureCountMax;
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
            ShardMapManager smm = new ShardMapManager(
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

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapFaultHandlingTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Shard s = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            Assert.IsNotNull(s);

            bool failed = false;

            try
            {
                PointMapping<int> p1 = lsm.CreatePointMapping(2, s);
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
            StubStoreOperationFactory ssof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                    (_smm, _opcode, _ssm, _sm) => new NTimeFailingAddMappingOperation(2, _smm, _opcode, _ssm, _sm)
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                ssof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapFaultHandlingTests.s_listShardMapName);

            Assert.IsNotNull(lsm);

            Shard s = lsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            Assert.IsNotNull(s);

            bool failed = false;

            try
            {
                // Inject GSM transaction failure at GSM commit time.
                PointMapping<int> p1 = lsm.CreatePointMapping(2, s);
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
                PointMapping<int> p1 = lsm.CreatePointMapping(2, s);
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
            ShardMapManager smm = new ShardMapManager(
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

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            Assert.IsNotNull(s);

            bool failed = false;

            try
            {
                // Inject GSM transaction failure at GSM commit time.
                RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
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
            StubStoreOperationFactory ssof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping =
                    (_smm, _opcode, _ssm, _sm) => new NTimeFailingAddMappingOperation(2, _smm, _opcode, _ssm, _sm)
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                ssof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            Shard s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            Assert.IsNotNull(s);

            bool failed = false;

            try
            {
                // Inject GSM transaction failure at GSM commit time.
                RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
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
                RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s);
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
            StubStoreOperationFactory ssof = new StubStoreOperationFactory()
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

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                ssof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            // test undo operations on shard

            // global pre-local only create shard
            Shard stemp = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            // now creating shard with GSM and LSM operations
            ssof.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
            Shard s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            // global pre-local only update shard

            rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

            // now update shard with GSM and LSM operations
            ssof.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard = null;
            Shard sNew = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

            // global pre-local only remove shard
            rsm.DeleteShard(sNew);

            // now remove with GSM and LSM operations
            ssof.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
            rsm.DeleteShard(sNew);

            // test undo operations for shard mapings

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            Assert.IsNotNull(s1);

            Shard s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[1]));

            Assert.IsNotNull(s2);

            // first add mapping will just execute global pre-local and add operation into pending operations log
            RangeMapping<int> rtemp = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            ssof.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping = null;

            // now add mapping will succeed after undoing pending operation

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            Assert.IsNotNull(r1);

            RangeMappingUpdate ru = new RangeMappingUpdate { Status = MappingStatus.Offline };

            // below call will only execute global pre-local step to create operations log
            RangeMapping<int> r2 = rsm.UpdateMapping(r1, ru);

            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;

            // now update same mapping again, this will undo previous pending operation and then add this mapping

            RangeMapping<int> r3 = rsm.UpdateMapping(r1, ru);

            // try mapping update failures with change in shard location
            // first reset CreateUpdateMappingOperation to just perform global pre-local
            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                    => new UpdateMappingOperationFailAfterGlobalPreLocal(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection);

            RangeMapping<int> r4 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

            // now try with actual update mapping operation
            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;
            RangeMapping<int> r5 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

            // split mapping toperform gsm-only pre-local operation

            IReadOnlyList<RangeMapping<int>> rlisttemp = rsm.SplitMapping(r5, 5);

            // try actual operation which will undo previous pending op
            ssof.CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray = null;

            IReadOnlyList<RangeMapping<int>> rlist = rsm.SplitMapping(r5, 5);

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
            StubStoreOperationFactory ssof = new StubStoreOperationFactory()
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

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                ssof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            // test undo operations on shard

            // global pre-local only create shard
            Shard stemp = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            // now creating shard with GSM and LSM operations
            ssof.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
            Shard s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            // global pre-local only update shard

            rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

            // now update shard with GSM and LSM operations
            ssof.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard = null;
            Shard sNew = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

            // global pre-local only remove shard
            rsm.DeleteShard(sNew);

            // now remove with GSM and LSM operations
            ssof.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
            rsm.DeleteShard(sNew);

            // test undo operations for shard mapings

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            Assert.IsNotNull(s1);

            Shard s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[1]));

            Assert.IsNotNull(s2);

            // first add mapping will just execute global pre-local and add operation into pending operations log
            RangeMapping<int> rtemp = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            ssof.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping = null;

            // now add mapping will succeed after undoing pending operation

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            Assert.IsNotNull(r1);

            RangeMappingUpdate ru = new RangeMappingUpdate { Status = MappingStatus.Offline };

            // below call will only execute global pre-local step to create operations log
            RangeMapping<int> r2 = rsm.UpdateMapping(r1, ru);

            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;

            // now update same mapping again, this will undo previous pending operation and then add this mapping

            RangeMapping<int> r3 = rsm.UpdateMapping(r1, ru);

            // try mapping update failures with change in shard location
            // first reset CreateUpdateMappingOperation to just perform global pre-local
            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                    => new UpdateMappingOperationFailAfterLocalSource(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection);

            RangeMapping<int> r4 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

            // now try with actual update mapping operation
            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;
            RangeMapping<int> r5 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

            // split mapping toperform gsm-only pre-local operation

            IReadOnlyList<RangeMapping<int>> rlisttemp = rsm.SplitMapping(r5, 5);

            // try actual operation which will undo previous pending op
            ssof.CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray = null;

            IReadOnlyList<RangeMapping<int>> rlist = rsm.SplitMapping(r5, 5);

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
            StubStoreOperationFactory ssof = new StubStoreOperationFactory()
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

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                ssof,
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapFaultHandlingTests.s_rangeShardMapName);

            Assert.IsNotNull(rsm);

            // test undo operations on shard

            // global pre-local only create shard
            Shard stemp = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            // now creating shard with GSM and LSM operations
            ssof.CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
            Shard s = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            // global pre-local only update shard

            rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

            // now update shard with GSM and LSM operations
            ssof.CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard = null;
            Shard sNew = rsm.UpdateShard(s, new ShardUpdate { Status = ShardStatus.Offline });

            // global pre-local only remove shard
            rsm.DeleteShard(sNew);

            // now remove with GSM and LSM operations
            ssof.CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard = null;
            rsm.DeleteShard(sNew);

            // test undo operations for shard mapings

            Shard s1 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[0]));

            Assert.IsNotNull(s1);

            Shard s2 = rsm.CreateShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapFaultHandlingTests.s_shardedDBs[1]));

            Assert.IsNotNull(s2);

            // first add mapping will just execute global pre-local and add operation into pending operations log
            RangeMapping<int> rtemp = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            ssof.CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping = null;

            // now add mapping will succeed after undoing pending operation

            RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(1, 10), s1);

            Assert.IsNotNull(r1);

            RangeMappingUpdate ru = new RangeMappingUpdate { Status = MappingStatus.Offline };

            // below call will only execute global pre-local step to create operations log
            RangeMapping<int> r2 = rsm.UpdateMapping(r1, ru);

            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;

            // now update same mapping again, this will undo previous pending operation and then add this mapping

            RangeMapping<int> r3 = rsm.UpdateMapping(r1, ru);

            // try mapping update failures with change in shard location
            // first reset CreateUpdateMappingOperation to just perform global pre-local
            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool =
                (_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection)
                    => new UpdateMappingOperationFailAfterLocalTarget(_shardMapManager, _operationCode, _shardMap, _mappingSource, _mappingTarget, _patternForKill, _lockOwnerId, _killConnection);

            RangeMapping<int> r4 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

            // now try with actual update mapping operation
            ssof.CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuidBool = null;
            RangeMapping<int> r5 = rsm.UpdateMapping(r3, new RangeMappingUpdate { Shard = s2 });

            // split mapping toperform gsm-only pre-local operation

            IReadOnlyList<RangeMapping<int>> rlisttemp = rsm.SplitMapping(r5, 5);

            // try actual operation which will undo previous pending op
            ssof.CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray = null;

            IReadOnlyList<RangeMapping<int>> rlist = rsm.SplitMapping(r5, 5);

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }

            public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
            {
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

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
                SqlResults results = new SqlResults();

                return results;
            }
        }

        internal static SqlException TransientSqlException = ShardMapFaultHandlingTests.CreateSqlException();

        private static SqlException CreateSqlException()
        {
            ConstructorInfo[] cisSqlError = typeof(SqlError)
                .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic);

#if NETFRAMEWORK
            ConstructorInfo ciSqlError = cisSqlError.Single(c => c.GetParameters().Length == 7);
            SqlError se = (SqlError)ciSqlError.Invoke(new object[] { (int)10928, (byte)0, (byte)0, "", "", "", (int)0 });
#else
            ConstructorInfo ciSqlError = cisSqlError.Single(c => c.GetParameters().Length == 8);
            SqlError se = (SqlError)ciSqlError.Invoke(new object[] { (int)10928, (byte)0, (byte)0, "", "", "", (int)0, null });
#endif

            ConstructorInfo ciSqlErrorCollection = typeof(SqlErrorCollection)
                .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic).Single();

            SqlErrorCollection sec = (SqlErrorCollection)ciSqlErrorCollection.Invoke(new object[0]);

            MethodInfo miSqlErrorCollectionAdd = typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.Instance | BindingFlags.NonPublic);

            miSqlErrorCollectionAdd.Invoke(sec, new object[] { se });

            MethodInfo miSqlExceptionCreate = typeof(SqlException)
                .GetMethod(
                    "CreateException",
                    BindingFlags.Static | BindingFlags.NonPublic,
                    null,
                    new Type[] { typeof(SqlErrorCollection), typeof(string) },
                    null);

            SqlException sqlException = (SqlException)miSqlExceptionCreate.Invoke(null, new object[] { sec, "" });

            return sqlException;
        }
    }
}
