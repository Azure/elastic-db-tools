﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Xunit;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
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

                Assert.False(lookupSmm);
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
        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
        public void CreateShardMapManager_Overwrite()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);
        }

        /// <summary>
        /// Create shard map manager, disallowing over-write.
        /// </summary>
        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
        public void CreateShardMapManager_NoOverwrite()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            ShardManagementException smme = AssertExtensions.AssertThrows<ShardManagementException>(() =>
                ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerCreateMode.KeepExisting));

            Assert.Equal(ShardManagementErrorCategory.ShardMapManagerFactory, smme.ErrorCategory);
            Assert.Equal(ShardManagementErrorCode.ShardMapManagerStoreAlreadyExists, smme.ErrorCode);
        }

        /// <summary>
        /// Get shard map manager, expects success.
        /// </summary>
        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
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
                Assert.NotNull(smm1);

                ShardMapManager smm2 = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    loadPolicy,
                    RetryBehavior.DefaultRetryBehavior);
                Assert.NotNull(smm2);
            }
        }

        /// <summary>
        /// Tries to get shard map manager, expects success.
        /// </summary>
        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
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
                Assert.True(success);
                Assert.NotNull(smm);

                success = ShardMapManagerFactory.TryGetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    loadPolicy,
                    RetryBehavior.DefaultRetryBehavior,
                    out smm);
                Assert.True(success);
                Assert.NotNull(smm);
            }
        }

        /// <summary>
        /// Tries to get shard map manager, expects failure.
        /// </summary>
        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
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
            Assert.False(success);
            Assert.Null(smm);
        }
    }
}
