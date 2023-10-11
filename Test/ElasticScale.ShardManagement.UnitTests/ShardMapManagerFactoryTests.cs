// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ClientTestCommon;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests;

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

        using var conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString);
        conn.Open();

        // Create ShardMapManager database
        using (var cmd = new SqlCommand(
            string.Format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName),
            conn))
        {
            _ = cmd.ExecuteNonQuery();
        }

        // Testing TryGetSqlShardMapManager failure case here instead of in TryGetShardMapManager_Fail()
        // There is no method to cleanup GSM objects, so if some other test runs in lab before 
        // TryGetShardMapManager_Fail, then this call will actually suceed as it will find earlier SMM structures.
        // Calling it just after creating database makes sure that GSM does not exist.
        // Other options were to recreate SMM database in tests (this will increase test duration) or
        // delete storage structures (t-sql delete) in the test which is not very clean solution.


        var lookupSmm = ShardMapManagerFactory.TryGetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Eager,
                RetryBehavior.DefaultRetryBehavior,
                out var smm);

        Assert.IsFalse(lookupSmm);
    }

    /// <summary>
    /// Cleans up common state for the all tests in this class.
    /// </summary>
    [ClassCleanup()]
    public static void ShardMapManagerFactoryTestsCleanup()
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
            _ = ShardMapManagerFactory.CreateSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerCreateMode.ReplaceExisting);

            // Cover all the CreateSqlShardMapManager overloads
            _ = ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionStringForSqlAuth,
                Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                ShardMapManagerCreateMode.ReplaceExisting);

            // This overload should fail with a ShardManagementException, because the manager already exists (but the Auth should work) 
            try
            {
                _ = ShardMapManagerFactory.CreateSqlShardMapManager(
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
                _ = ShardMapManagerFactory.CreateSqlShardMapManager(Globals.ShardMapManagerConnectionString);
                Assert.Fail("This should have thrown, because the manager already exists");
            }
            catch (ShardManagementException)
            {
            }

            // This overload should fail with a ShardManagementException, because the manager already exists (but the Auth should work) 
            try
            {
                _ = ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    RetryBehavior.DefaultRetryBehavior);
                Assert.Fail("This should have thrown, because the manager already exists");
            }
            catch (ShardManagementException)
            {
            }

            _ = ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting,
                RetryBehavior.DefaultRetryBehavior);

            _ = ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionStringForSqlAuth,
                Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                ShardMapManagerCreateMode.ReplaceExisting,
                RetryBehavior.DefaultRetryBehavior);

            _ = ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting,
                RetryBehavior.DefaultRetryBehavior);

            _ = ShardMapManagerFactory.CreateSqlShardMapManager(
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
        _ = ShardMapManagerFactory.CreateSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerCreateMode.ReplaceExisting);

        var smme = AssertExtensions.AssertThrows<ShardManagementException>(() =>
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
        _ = ShardMapManagerFactory.CreateSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerCreateMode.ReplaceExisting);

        foreach (ShardMapManagerLoadPolicy loadPolicy in Enum.GetValues(typeof(ShardMapManagerLoadPolicy)))
        {
            var smm1 = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                loadPolicy);
            Assert.IsNotNull(smm1);

            var smm2 = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                loadPolicy,
                RetryBehavior.DefaultRetryBehavior);
            Assert.IsNotNull(smm2);

            var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

            if (sqlAuthLogin.Create())
            {
                // Cover all the GetSqlShardMapManager overloads
                var smm3 = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionStringForSqlAuth,
                Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                loadPolicy,
                RetryBehavior.DefaultRetryBehavior);
                Assert.IsNotNull(smm3);

                var smm4 = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionStringForSqlAuth,
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    loadPolicy,
                    RetryBehavior.DefaultRetryBehavior);
                Assert.IsNotNull(smm4);

                var smm5 = ShardMapManagerFactory.GetSqlShardMapManager(
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
        _ = ShardMapManagerFactory.CreateSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerCreateMode.ReplaceExisting);

        foreach (ShardMapManagerLoadPolicy loadPolicy in Enum.GetValues(typeof(ShardMapManagerLoadPolicy)))
        {
            bool success;

            success = ShardMapManagerFactory.TryGetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                loadPolicy,
                out var smm);
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
        _ = ShardMapManagerFactory.CreateSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerCreateMode.ReplaceExisting);

        ShardMapManager smm = null;
        var success = false;

        // Null retry policy not allowed
        _ = AssertExtensions.AssertThrows<ArgumentNullException>(() =>
            success = ShardMapManagerFactory.TryGetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Eager,
                null,
                out smm));
        Assert.IsFalse(success);
        Assert.IsNull(smm);
    }
}
