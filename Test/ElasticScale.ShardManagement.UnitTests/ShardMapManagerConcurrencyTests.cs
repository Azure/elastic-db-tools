// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests;

[TestClass]
public class ShardMapManagerConcurrencyTests
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
    public static void ShardMapManagerConcurrencyTestsInitialize(TestContext testContext)
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
    public static void ShardMapManagerConcurrencyTestsCleanup()
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
    public void ShardMapManagerConcurrencyTestInitialize()
    {
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
                                Globals.ShardMapManagerConnectionString,
                                ShardMapManagerLoadPolicy.Lazy);
        try
        {
            var sm = smm.GetShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);
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
        var smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);
        try
        {
            var sm = smm.GetShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);
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

        var smmMgmt = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

        var smmClient = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

        #region CreateShardMap
        // Add a shard map from management SMM.
        ShardMap smMgmt = smmMgmt.CreateListShardMap<int>(ShardMapManagerConcurrencyTests.s_shardMapName);

        Assert.AreEqual(ShardMapManagerConcurrencyTests.s_shardMapName, smMgmt.Name);

        // Lookup shard map from client SMM.
        var smClient = smmClient.GetShardMap(ShardMapManagerConcurrencyTests.s_shardMapName);

        Assert.IsNotNull(smClient);

        #endregion CreateShardMap

        #region ConvertToListShardMap

        var lsmMgmt = smmMgmt.GetListShardMap<int>(ShardMapManagerConcurrencyTests.s_shardMapName);
        Assert.IsNotNull(lsmMgmt);

        // look up shard map again, it will 
        var lsmClient = smmClient.GetListShardMap<int>(ShardMapManagerConcurrencyTests.s_shardMapName);
        Assert.IsNotNull(lsmClient);

        #endregion ConvertToListShardMap

        #region DeleteShardMap

        // verify that smClient is accessible

        _ = lsmClient.GetShards();

        smmMgmt.DeleteShardMap(lsmMgmt);

        operationFailed = false;

        try
        {
            // smClient does not exist, below call will fail.
            var sCNew = lsmClient.GetShards();
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
