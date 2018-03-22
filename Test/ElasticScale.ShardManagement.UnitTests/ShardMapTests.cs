// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    using ClientTestCommon;

    /// <summary>
    /// Test related to ShardMap class and it's methods.
    /// </summary>
    [TestClass]
    public class ShardMapTests
    {
        /// <summary>
        /// Sharded databases to create for the test.
        /// </summary>
        private static string[] s_shardedDBs = new[]
        {
            "shard1" + Globals.TestDatabasePostfix, "shard2" + Globals.TestDatabasePostfix
        };

        /// <summary>
        /// Default shard map name.
        /// </summary>
        private static string s_defaultShardMapName = "Customers_default";

        #region Common Methods

        /// <summary>
        /// Helper function to clean default shard map.
        /// </summary>
        private static void CleanShardMapsHelper()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            // Remove all existing mappings from the list shard map.
            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);

            Assert.IsNotNull(sm);

            // Remove all shards from list shard map
            IEnumerable<Shard> s = sm.GetShards();

            using (IEnumerator<Shard> sEnum = s.GetEnumerator())
            {
                while (sEnum.MoveNext())
                {
                    sm.DeleteShard(sEnum.Current);
                }
            }
        }
        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void ShardMapTestsInitialize(TestContext testContext)
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
                for (int i = 0; i < ShardMapTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ShardMapTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ShardMapTests.s_shardedDBs[i]),
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

            // Create default shard map.
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.CreateListShardMap<int>(ShardMapTests.s_defaultShardMapName);

            Assert.IsNotNull(sm);

            Assert.AreEqual(ShardMapTests.s_defaultShardMapName, sm.Name);
        }

        /// <summary>
        /// Cleans up common state for the all tests in this class.
        /// </summary>
        [ClassCleanup()]
        public static void ShardMapTestsCleanup()
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();
                // Drop shard databases
                for (int i = 0; i < ShardMapTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ShardMapTests.s_shardedDBs[i]),
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
        public void ShardMapTestInitialize()
        {
            ShardMapTests.CleanShardMapsHelper();
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        [TestCleanup()]
        public void ShardMapTestCleanup()
        {
            ShardMapTests.CleanShardMapsHelper();
        }

        #endregion Common Methods

        /// <summary>
        /// Add a shard to shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void CreateShardDefault()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);

            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(
                Globals.ShardMapManagerTestsDatasourceName,
                ShardMapTests.s_shardedDBs[0],
                SqlProtocol.Tcp,
                1433);

            Shard sNew = sm.CreateShard(sl);

            Assert.IsNotNull(sNew);

            // Validate that the shard location is round-tripped correctly
            Assert.AreEqual(sl, sNew.Location);
            Assert.AreEqual(sl, sm.GetShard(sl).Location);

            // Validate that we can connect to the shard (using all the overloads OpenConnection available)
            using (sNew.OpenConnection(
                Globals.ShardUserConnectionString))
            {
            }

            // Validate that we can connect to the shard
            using (sNew.OpenConnection(
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
            {
            }

            var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

            if (sqlAuthLogin.Create())
            {
                // Validate that we can connect to the shard using Sql Auth
                using (sNew.OpenConnection(Globals.ShardUserConnectionStringForSqlAuth(sqlAuthLogin.UniquifiedUserName), ConnectionOptions.Validate))
                {
                }

                // Validate that we can connect to the shard using a secure Sql Auth Credential
                using (sNew.OpenConnection(string.Empty, Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName)))
                {
                }

                using (sNew.OpenConnection(
                    string.Empty,
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    ConnectionOptions.Validate))
                {
                }

                // Validate that we can connect to the shard using Sql Auth and a secure credential
                // This should fail with an ArgumentException (because you can't pass in both a SqlAuth connection string
                // and a secure credential.
                try
                {
                    using (sNew.OpenConnection(
                        Globals.ShardUserConnectionStringForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                        Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                        ConnectionOptions.Validate))
                    {
                    }

                    Assert.Fail("This should have thrown, ");
                }
                catch (ArgumentException)
                {
                    // Expected failure. you can't pass in both a SqlAuth connection string and a secure credential
                }

                // Drop test login
                sqlAuthLogin.Drop();
            }
            else
            {
                Assert.Inconclusive("Failed to create sql login, test skipped");
            }
        }

        /// <summary>
        /// Add a duplicate shard to shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void CreateShardDuplicate()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);

            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(sl);

            Assert.IsNotNull(sNew);

            bool addFailed = false;

            try
            {
                Shard sDuplicate = sm.CreateShard(sl);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardLocationAlreadyExists, sme.ErrorCode);
                addFailed = true;
            }

            Assert.IsTrue(addFailed);
        }

        /// <summary>
        /// Add a shard with null location to shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void CreateShardNullLocation()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            bool addFailed = false;

            try
            {
                ShardLocation sl = new ShardLocation("", "");
                Shard sNew = sm.CreateShard(sl);
            }
            catch (ArgumentException)
            {
                addFailed = true;
            }

            Assert.IsTrue(addFailed);
        }

        /// <summary>
        /// Remove existing shard from shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteShardDefault()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(sl);

            Assert.IsNotNull(sNew);

            sm.DeleteShard(sNew);
        }

        /// <summary>
        /// Remove an already removed shard from shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteShardDuplicate()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(sl);

            Assert.IsNotNull(sNew);

            sm.DeleteShard(sNew);

            Assert.IsNotNull(sNew);

            bool removeFailed = false;

            try
            {
                sm.DeleteShard(sNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardDoesNotExist, sme.ErrorCode);
                removeFailed = true;
            }

            Assert.IsTrue(removeFailed);
        }

        /// <summary>
        /// Remove a shard with shard version mismatch.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteShardVersionMismatch()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(sl);

            // Update shard to increment version

            ShardUpdate su = new ShardUpdate();
            su.Status = ShardStatus.Offline;

            Shard sUpdated = sm.UpdateShard(sNew, su);

            bool removeFailed = false;

            try
            {
                sm.DeleteShard(sNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardVersionMismatch, sme.ErrorCode);
                removeFailed = true;
            }

            Assert.IsTrue(removeFailed);
        }

        /// <summary>
        /// Update shard.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateShard()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(new ShardCreationInfo(sl, ShardStatus.Online));

            ShardUpdate su = new ShardUpdate();
            su.Status = ShardStatus.Offline;

            Shard sUpdated = sm.UpdateShard(sNew, su);

            Assert.IsNotNull(sNew);
        }

        /// <summary>
        /// Update shard with version mismatch.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateShardVersionMismatch()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(new ShardCreationInfo(sl, ShardStatus.Online));

            // update shard once to increment version

            ShardUpdate su = new ShardUpdate();
            su.Status = ShardStatus.Offline;

            Shard sUpdated = sm.UpdateShard(sNew, su);

            Assert.IsNotNull(sNew);

            // now try updating sNew shard again.

            bool updateFailed = false;

            try
            {
                Shard sUpdatedFail = sm.UpdateShard(sNew, su);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardVersionMismatch, sme.ErrorCode);
                updateFailed = true;
            }

            Assert.IsTrue(updateFailed);
        }

        /// <summary>
        /// Validate shard.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void ValidateShard()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(new ShardCreationInfo(sl, ShardStatus.Online));

            ShardUpdate su = new ShardUpdate();
            su.Status = ShardStatus.Offline;

            Shard sUpdated = sm.UpdateShard(sNew, su);
            Assert.IsNotNull(sUpdated);

            bool validationFailed = false;
            try
            {
                using (SqlConnection conn = sNew.OpenConnection(
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
            catch (ShardManagementException sme)
            {
                validationFailed = true;
                Assert.AreEqual(ShardManagementErrorCategory.Validation, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardVersionMismatch, sme.ErrorCode);
            }

            Assert.IsTrue(validationFailed);

            validationFailed = false;

            try
            {
                using (SqlConnection conn = sUpdated.OpenConnection(
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
            catch (ShardManagementException)
            {
                validationFailed = true;
            }

            Assert.IsFalse(validationFailed);
        }

        private class NTimeFailingAddShardOperation : AddShardOperation
        {
            private int _failureCountMax;
            private int _currentFailureCount;

            internal NTimeFailingAddShardOperation(
                int failureCountMax,
                ShardMapManager shardMapManager,
                IStoreShardMap shardMap,
                IStoreShard shard) :
                base(shardMapManager, shardMap, shard)
            {
                _failureCountMax = failureCountMax;
                _currentFailureCount = 0;
            }

            public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
            {
                if (_currentFailureCount < _failureCountMax)
                {
                    _currentFailureCount++;

                    throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                }
                else
                {
                    return base.DoGlobalPostLocalExecute(ts);
                }
            }
        }

        private class NTimeFailingRemoveShardOperation : RemoveShardOperation
        {
            private int _failureCountMax;
            private int _currentFailureCount;

            internal NTimeFailingRemoveShardOperation(
                int failureCountMax,
                ShardMapManager shardMapManager,
                IStoreShardMap shardMap,
                IStoreShard shard) :
                base(shardMapManager, shardMap, shard)
            {
                _failureCountMax = failureCountMax;
                _currentFailureCount = 0;
            }

            public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
            {
                if (_currentFailureCount < _failureCountMax)
                {
                    _currentFailureCount++;

                    throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                }
                else
                {
                    return base.DoGlobalPostLocalExecute(ts);
                }
            }
        }

        private class NTimeFailingUpdateShardOperation : UpdateShardOperation
        {
            private int _failureCountMax;
            private int _currentFailureCount;

            internal NTimeFailingUpdateShardOperation(
                int failureCountMax,
                ShardMapManager shardMapManager,
                IStoreShardMap shardMap,
                IStoreShard shardOld,
                IStoreShard shardNew) :
                base(shardMapManager, shardMap, shardOld, shardNew)
            {
                _failureCountMax = failureCountMax;
                _currentFailureCount = 0;
            }

            public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
            {
                if (_currentFailureCount < _failureCountMax)
                {
                    _currentFailureCount++;

                    throw new StoreException("", ShardMapFaultHandlingTests.TransientSqlException);
                }
                else
                {
                    return base.DoGlobalPostLocalExecute(ts);
                }
            }
        }

        #region GsmAbortTests

        /// <summary>
        /// Add a shard to shard map, abort transaction in GSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void CreateShardAbortGSM()
        {
            int retryCount = 0;

            EventHandler<RetryingEventArgs> eventHandler = (sender, arg) =>
            {
                retryCount++;
            };

            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StubStoreOperationFactory()
                {
                    CallBase = true,
                    CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
                        (_smm, _sm, _s) => new NTimeFailingAddShardOperation(10, _smm, _sm, _s)
                },
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(5, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);

            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            bool storeOperationFailed = false;

            smm.ShardMapManagerRetrying += eventHandler;

            try
            {
                Shard sNew = sm.CreateShard(sl);
                Assert.IsNotNull(sNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            smm.ShardMapManagerRetrying -= eventHandler;

            Assert.AreEqual(5, retryCount);

            Assert.IsTrue(storeOperationFailed);

            // verify that shard map does not have any shards.
            int count = 0;
            IEnumerable<Shard> sList = sm.GetShards();

            using (IEnumerator<Shard> sEnum = sList.GetEnumerator())
            {
                while (sEnum.MoveNext())
                    count++;
            }
            Assert.AreEqual(0, count);
        }

        /// <summary>
        /// Add a shard to shard map, abort transaction in GSM Do and GSM Undo.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void CreateShardAbortGSMDoAndLSMUndo()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard =
                (_smm, _sm, _s) =>
                {
                    StubAddShardOperation op = new StubAddShardOperation(_smm, _sm, _s);
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

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);

            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            bool storeOperationFailed = false;
            try
            {
                Shard sNew = sm.CreateShard(sl);
                Assert.IsNotNull(sNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // Obtain the pending operations.
            var pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 1);

            // verify that shard map does not have any shards.
            Assert.AreEqual(0, sm.GetShards().Count());

            shouldThrow = false;
            storeOperationFailed = false;
            try
            {
                Shard sNew = sm.CreateShard(sl);
                Assert.IsNotNull(sNew);
            }
            catch (ShardManagementException)
            {
                storeOperationFailed = true;
            }

            Assert.IsFalse(storeOperationFailed);
            Assert.AreEqual(1, sm.GetShards().Count());
        }

        /// <summary>
        /// Remove existing shard from shard map, do not commit transaction in GSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteShardAbortGSM()
        {
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StubStoreOperationFactory()
                {
                    CallBase = true,
                    CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
                        (_smm, _sm, _s) => new NTimeFailingRemoveShardOperation(10, _smm, _sm, _s)
                },
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero), RetryBehavior.DefaultRetryBehavior);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(sl);

            Assert.IsNotNull(sNew);

            bool storeOperationFailed = false;
            try
            {
                sm.DeleteShard(sNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // verify that the shard exists in store.
            Shard sValidate = sm.GetShard(sl);
            Assert.IsNotNull(sValidate);
        }

        /// <summary>
        /// Remove shard from shard map, abort transaction in GSM Do and LSM Undo.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void DeleteShardAbortGSMDoAndLSMUndo()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard =
                (_smm, _sm, _s) =>
                {
                    StubRemoveShardOperation op = new StubRemoveShardOperation(_smm, _sm, _s);
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
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);

            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(sl);

            Assert.IsNotNull(sNew);

            bool storeOperationFailed = false;
            try
            {
                sm.DeleteShard(sNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // verify that the shard exists in store.
            Shard sValidate = sm.GetShard(sl);
            Assert.IsNotNull(sValidate);

            // Obtain the pending operations.
            var pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 1);

            shouldThrow = false;
            storeOperationFailed = false;
            try
            {
                sm.DeleteShard(sNew);
            }
            catch (ShardManagementException)
            {
                storeOperationFailed = true;
            }

            Assert.IsFalse(storeOperationFailed);
            Assert.AreEqual(0, sm.GetShards().Count());
        }

        /// <summary>
        /// Update shard, do not commit transaction in GSM.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateShardAbortGSM()
        {
            ShardMapManager smm = new ShardMapManager(
                new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
                new SqlStoreConnectionFactory(),
                new StubStoreOperationFactory()
                {
                    CallBase = true,
                    CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard =
                        (_smm, _sm, _so, _sn) => new NTimeFailingUpdateShardOperation(10, _smm, _sm, _so, _sn)
                },
                new CacheStore(),
                ShardMapManagerLoadPolicy.Lazy,
                new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero),
                RetryBehavior.DefaultRetryBehavior);
            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(new ShardCreationInfo(sl, ShardStatus.Online));

            ShardUpdate su = new ShardUpdate();
            su.Status = ShardStatus.Offline;

            bool storeOperationFailed = false;
            try
            {
                Shard sUpdated = sm.UpdateShard(sNew, su);
                Assert.IsNotNull(sNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // verify that shard status is not changed.
            Shard sValidate = sm.GetShard(sl);
            Assert.AreEqual(sNew.Status, sValidate.Status);
        }

        /// <summary>
        /// Update shard in shard map, abort transaction in GSM Do and GSM Undo.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void UpdateShardAbortGSMDoAndLSMUndo()
        {
            bool shouldThrow = true;

            IStoreOperationFactory sof = new StubStoreOperationFactory()
            {
                CallBase = true,
                CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard =
                (_smm, _sm, _so, _sn) =>
                {
                    StubUpdateShardOperation op = new StubUpdateShardOperation(_smm, _sm, _so, _sn);
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



            ShardMap sm = smm.GetShardMap(ShardMapTests.s_defaultShardMapName);
            Assert.IsNotNull(sm);

            ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapTests.s_shardedDBs[0]);

            Shard sNew = sm.CreateShard(new ShardCreationInfo(sl, ShardStatus.Online));

            ShardUpdate su = new ShardUpdate();
            su.Status = ShardStatus.Offline;

            bool storeOperationFailed = false;
            try
            {
                Shard sUpdated = sm.UpdateShard(sNew, su);
                Assert.IsNotNull(sNew);
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.StorageOperationFailure, sme.ErrorCode);
                storeOperationFailed = true;
            }

            Assert.IsTrue(storeOperationFailed);

            // verify that shard status is not changed.
            Shard sValidate = sm.GetShard(sl);
            Assert.AreEqual(sNew.Status, sValidate.Status);

            // Obtain the pending operations.
            var pendingOperations = ShardMapperTests.GetPendingStoreOperations();
            Assert.AreEqual(pendingOperations.Count(), 1);

            shouldThrow = false;
            storeOperationFailed = false;
            try
            {
                sm.UpdateShard(sNew, su);
            }
            catch (ShardManagementException)
            {
                storeOperationFailed = true;
            }

            Assert.IsFalse(storeOperationFailed);
            sValidate = sm.GetShard(sl);
            Assert.AreEqual(su.Status, sValidate.Status);
        }

        #endregion GsmAbortTests
    }
}
