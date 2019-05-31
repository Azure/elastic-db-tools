// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    using ClientTestCommon;

    /// <summary>
    /// Tests related to ShardMapManagerFactory class and it's methods.
    /// </summary>
    [TestClass]
    public class ShardMapManagerFactoryTests
    {
        #region Common Methods

        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void ShardMapManagerFactoryTestsInitialize(TestContext testContext)
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

                // Testing TryGetSqlShardMapManager failure case here instead of in TryGetShardMapManager_Fail()
                // There is no method to cleanup GSM objects, so if some other test runs in lab before 
                // TryGetShardMapManager_Fail, then this call will actually suceed as it will find earlier SMM structures.
                // Calling it just after creating database makes sure that GSM does not exist.
                // Other options were to recreate SMM database in tests (this will increase test duration) or
                // delete storage structures (t-sql delete) in the test which is not very clean solution.

                ShardMapManager smm = null;

                bool lookupSmm = ShardMapManagerFactory.TryGetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Eager,
                        RetryBehavior.DefaultRetryBehavior,
                        out smm);

                Assert.IsFalse(lookupSmm);
            }
        }

        /// <summary>
        /// Cleans up common state for the all tests in this class.
        /// </summary>
        [ClassCleanup()]
        public static void ShardMapManagerFactoryTestsCleanup()
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
        public void ShardMapManagerFactoryTestInitialize()
        {
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        [TestCleanup()]
        public void ShardMapManagerFactoryTestCleanup()
        {
        }

        #endregion Common Methods

        /// <summary>
        /// Create shard map manager.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void CreateShardMapManager_Overwrite()
        {
            var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

            if (sqlAuthLogin.Create())
            {
                ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

                // Cover all the CreateSqlShardMapManager overloads
                ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionStringForSqlAuth,
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    ShardMapManagerCreateMode.ReplaceExisting);

                // This overload should fail with a ShardManagementException, because the manager already exists (but the Auth should work) 
                try
                {
                    ShardMapManagerFactory.CreateSqlShardMapManager(
                        Globals.ShardMapManagerConnectionStringForSqlAuth,
                        Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName));
                    Assert.Fail("This should have thrown, because the manager already exists");
                }
                catch (ShardManagementException)
                {
                }

                // This overload should fail with a ShardManagementException, because the manager already exists (but the Auth should work) 
                try
                {
                    ShardMapManagerFactory.CreateSqlShardMapManager(Globals.ShardMapManagerConnectionString);
                    Assert.Fail("This should have thrown, because the manager already exists");
                }
                catch (ShardManagementException)
                {
                }

                // This overload should fail with a ShardManagementException, because the manager already exists (but the Auth should work) 
                try
                {
                    ShardMapManagerFactory.CreateSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        RetryBehavior.DefaultRetryBehavior);
                    Assert.Fail("This should have thrown, because the manager already exists");
                }
                catch (ShardManagementException)
                {
                }

                ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerCreateMode.ReplaceExisting,
                    RetryBehavior.DefaultRetryBehavior);

                ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionStringForSqlAuth,
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    ShardMapManagerCreateMode.ReplaceExisting,
                    RetryBehavior.DefaultRetryBehavior);

                ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerCreateMode.ReplaceExisting,
                    RetryBehavior.DefaultRetryBehavior);

                ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionStringForSqlAuth,
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    ShardMapManagerCreateMode.ReplaceExisting,
                    RetryBehavior.DefaultRetryBehavior);

                // Drop test login
                sqlAuthLogin.Drop();
            }
            else
            {
                Assert.Inconclusive("Failed to create sql login, test skipped");
            }
        }

        /// <summary>
        /// Create shard map manager, disallowing over-write.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void CreateShardMapManager_NoOverwrite()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            ShardManagementException smme = AssertExtensions.AssertThrows<ShardManagementException>(() =>
                ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerCreateMode.KeepExisting));

            Assert.AreEqual(ShardManagementErrorCategory.ShardMapManagerFactory, smme.ErrorCategory);
            Assert.AreEqual(ShardManagementErrorCode.ShardMapManagerStoreAlreadyExists, smme.ErrorCode);
        }

        /// <summary>
        /// Get shard map manager, expects success.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void GetShardMapManager_Success()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            foreach (ShardMapManagerLoadPolicy loadPolicy in Enum.GetValues(typeof(ShardMapManagerLoadPolicy)))
            {
                ShardMapManager smm1 = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    loadPolicy);
                Assert.IsNotNull(smm1);

                ShardMapManager smm2 = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    loadPolicy,
                    RetryBehavior.DefaultRetryBehavior);
                Assert.IsNotNull(smm2);

                var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

                if (sqlAuthLogin.Create())
                {
                    // Cover all the GetSqlShardMapManager overloads
                    ShardMapManager smm3 = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionStringForSqlAuth,
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    loadPolicy,
                    RetryBehavior.DefaultRetryBehavior);
                    Assert.IsNotNull(smm3);

                    ShardMapManager smm4 = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionStringForSqlAuth,
                        Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                        loadPolicy,
                        RetryBehavior.DefaultRetryBehavior);
                    Assert.IsNotNull(smm4);

                    ShardMapManager smm5 = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionStringForSqlAuth,
                        Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                        loadPolicy);
                    Assert.IsNotNull(smm5);

                    // Drop test login
                    sqlAuthLogin.Drop();
                }
                else
                {
                    Assert.Inconclusive("Failed to create sql login, test skipped");
                }
            }
        }

        /// <summary>
        /// Tries to get shard map manager, expects success.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TryGetShardMapManager_Success()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            foreach (ShardMapManagerLoadPolicy loadPolicy in Enum.GetValues(typeof(ShardMapManagerLoadPolicy)))
            {
                ShardMapManager smm;
                bool success;

                success = ShardMapManagerFactory.TryGetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    loadPolicy,
                    out smm);
                Assert.IsTrue(success);
                Assert.IsNotNull(smm);

                success = ShardMapManagerFactory.TryGetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    loadPolicy,
                    RetryBehavior.DefaultRetryBehavior,
                    out smm);
                Assert.IsTrue(success);
                Assert.IsNotNull(smm);

                // Cover all the overloads
                var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

                if (sqlAuthLogin.Create())
                {
                    success = ShardMapManagerFactory.TryGetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionStringForSqlAuth,
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    loadPolicy,
                    RetryBehavior.DefaultRetryBehavior,
                    out smm);
                    Assert.IsTrue(success);
                    Assert.IsNotNull(smm);

                    success = ShardMapManagerFactory.TryGetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionStringForSqlAuth,
                        Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                        loadPolicy,
                        RetryBehavior.DefaultRetryBehavior,
                        out smm);
                    Assert.IsTrue(success);
                    Assert.IsNotNull(smm);
                }
                else
                {
                    Assert.Inconclusive("Failed to create sql login, test skipped");
                }
            }
        }

        /// <summary>
        /// Tries to get shard map manager, expects failure.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TryGetShardMapManager_Fail()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            ShardMapManager smm = null;
            bool success = false;

            // Null retry policy not allowed
            AssertExtensions.AssertThrows<ArgumentNullException>(() =>
                success = ShardMapManagerFactory.TryGetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Eager,
                    null,
                    out smm));
            Assert.IsFalse(success);
            Assert.IsNull(smm);
        }
    }
}
