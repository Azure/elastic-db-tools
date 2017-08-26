// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Fixtures;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Xunit;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Tests related to ShardMapManagerFactory class and it's methods.
    /// </summary>
    public class ShardMapManagerFactoryTests : IDisposable, IClassFixture<ShardMapManagerFactoryTestsFixture>
    {
        #region Common Methods

        /// <summary>
        /// Initializes common state per-test.
        /// </summary>
        public ShardMapManagerFactoryTests()
        {
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        public void Dispose()
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
