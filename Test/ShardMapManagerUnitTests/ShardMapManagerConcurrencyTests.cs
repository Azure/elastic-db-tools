// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    [TestClass]
    public class ShardMapManagerConcurrencyTests
    {
        /// <summary>
        /// Shard map name used in the tests.
        /// </summary>
        private static string s_shardMapName = "Customer";

        #region Common Methods

        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void ShardMapManagerConcurrencyTestsInitialize(TestContext testContext)
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
            }

            // Create the shard map manager.
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);
        }

        /// <summary>
        /// Cleans up common state for the all tests in this class.
        /// </summary>
        [ClassCleanup()]
        public static void ShardMapManagerConcurrencyTestsCleanup()
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();
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
        public void ShardMapManagerConcurrencyTestInitialize()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                                    Globals.ShardMapManagerConnectionString,
                                    ShardMapManagerLoadPolicy.Lazy);
            try
            {
                ShardMap sm = smm.GetShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);
                smm.DeleteShardMap(sm);
            }
            catch (ShardManagementException smme)
            {
                Assert.IsTrue(smme.ErrorCode == ShardManagementErrorCode.ShardMapLookupFailure);
            }
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        [TestCleanup()]
        public void ShardMapManagerConcurrencyTestCleanup()
        {
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);
            try
            {
                ShardMap sm = smm.GetShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);
                smm.DeleteShardMap(sm);
            }
            catch (ShardManagementException smme)
            {
                Assert.IsTrue(smme.ErrorCode == ShardManagementErrorCode.ShardMapLookupFailure);
            }
        }

        #endregion Common Methods

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void ConcurrencyScenarioListShardMap()
        {
            bool operationFailed; // variable to track status of negative test scenarios

            // Create 2 SMM objects representing management and client

            ShardMapManager smmMgmt = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            ShardMapManager smmClient = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            #region CreateShardMap
            // Add a shard map from management SMM.
            ShardMap smMgmt = smmMgmt.CreateListShardMap<int>(ShardMapManagerConcurrencyTests.s_shardMapName);

            Assert.AreEqual(ShardMapManagerConcurrencyTests.s_shardMapName, smMgmt.Name);

            // Lookup shard map from client SMM.
            ShardMap smClient = smmClient.GetShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);

            Assert.IsNotNull(smClient);

            #endregion CreateShardMap

            #region ConvertToListShardMap

            ListShardMap<int> lsmMgmt = smmMgmt.GetListShardMap<int>(ShardMapManagerConcurrencyTests.s_shardMapName);
            Assert.IsNotNull(lsmMgmt);

            // look up shard map again, it will 
            ListShardMap<int> lsmClient = smmClient.GetListShardMap<int>(ShardMapManagerConcurrencyTests.s_shardMapName);
            Assert.IsNotNull(lsmClient);

            #endregion ConvertToListShardMap

            #region DeleteShardMap

            // verify that smClient is accessible

            IEnumerable<Shard> shardClient = lsmClient.GetShards();

            smmMgmt.DeleteShardMap(lsmMgmt);

            operationFailed = false;

            try
            {
                // smClient does not exist, below call will fail.
                IEnumerable<Shard> sCNew = lsmClient.GetShards();
            }
            catch (ShardManagementException sme)
            {
                Assert.AreEqual(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.AreEqual(ShardManagementErrorCode.ShardMapDoesNotExist, sme.ErrorCode);
                operationFailed = true;
            }

            Assert.IsTrue(operationFailed);

            #endregion DeleteShardMap
        }
    }
}
