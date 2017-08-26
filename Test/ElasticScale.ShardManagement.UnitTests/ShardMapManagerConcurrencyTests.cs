// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Xunit;
using System.Collections.Generic;
using System;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Fixtures;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    public class ShardMapManagerConcurrencyTests : IDisposable, IClassFixture<ShardMapManagerConcurrencyTestsFixture>
    {
        /// <summary>
        /// Shard map name used in the tests.
        /// </summary>
        internal static string s_shardMapName = "Customer";

        #region Common Methods

        /// <summary>
        /// Initializes common state per-test.
        /// </summary>
        public ShardMapManagerConcurrencyTests()
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
                Assert.True(smme.ErrorCode == ShardManagementErrorCode.ShardMapLookupFailure);
            }
        }

        /// <summary>
        /// Cleans up common state per-test.
        /// </summary>
        public void Dispose()
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
                Assert.True(smme.ErrorCode == ShardManagementErrorCode.ShardMapLookupFailure);
            }
        }

        #endregion Common Methods

        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
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

            Assert.Equal(ShardMapManagerConcurrencyTests.s_shardMapName, smMgmt.Name);

            // Lookup shard map from client SMM.
            ShardMap smClient = smmClient.GetShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);

            Assert.NotNull(smClient);

            #endregion CreateShardMap

            #region ConvertToListShardMap

            ListShardMap<int> lsmMgmt = smmMgmt.GetListShardMap<int>(ShardMapManagerConcurrencyTests.s_shardMapName);
            Assert.NotNull(lsmMgmt);

            // look up shard map again, it will 
            ListShardMap<int> lsmClient = smmClient.GetListShardMap<int>(ShardMapManagerConcurrencyTests.s_shardMapName);
            Assert.NotNull(lsmClient);

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
                Assert.Equal(ShardManagementErrorCategory.ShardMap, sme.ErrorCategory);
                Assert.Equal(ShardManagementErrorCode.ShardMapDoesNotExist, sme.ErrorCode);
                operationFailed = true;
            }

            Assert.True(operationFailed);

            #endregion DeleteShardMap
        }
    }
}
