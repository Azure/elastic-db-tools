// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ClientTestCommon;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests;

/// <summary>
/// Tests based on scenarios which cover various aspects of the
/// ShardMapManager library.
/// </summary>
[TestClass]
public class ScenarioTests
{
#if CODESAMPLE
    private static Lazy<ShardMapManager> smm = new Lazy<ShardMapManager>(
        () => ShardMapManagerFactory.GetSqlShardMapManager(ShardMapManagerLoadPolicy.Eager),
        true);

    internal static ShardMapManager ShardMapManagerInstance
    {
        get
        {
            return smm.Value;
        }
    }
#endif

    // Shards with single user per tenant model.
    private static readonly string[] s_perTenantDBs = new[]
    {
        "PerTenantDB1", "PerTenantDB2", "PerTenantDB3", "PerTenantDB4"
    };

    // Shards with multiple users per tenant model.
    private static readonly string[] s_multiTenantDBs = new[]
    {
        "MultiTenantDB1", "MultiTenantDB2", "MultiTenantDB3", "MultiTenantDB4", "MultiTenantDB5"
    };

    #region Common Methods

    /// <summary>
    /// Initializes common state for tests in this class.
    /// </summary>
    /// <param name="testContext">The TestContext we are running in.</param>
    [ClassInitialize()]
    public static void ScenarioTestsInitialize(TestContext testContext)
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

        // Create PerTenantDB databases
        for (var i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
        {
            using (var cmd = new SqlCommand(
                string.Format(Globals.DropDatabaseQuery, ScenarioTests.s_perTenantDBs[i]),
                conn))
            {
                _ = cmd.ExecuteNonQuery();
            }

            using (var cmd = new SqlCommand(
                string.Format(Globals.CreateDatabaseQuery, ScenarioTests.s_perTenantDBs[i]),
                conn))
            {
                _ = cmd.ExecuteNonQuery();
            }
        }

        // Create MultiTenantDB databases
        for (var i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++)
        {
            using (var cmd = new SqlCommand(
                string.Format(Globals.DropDatabaseQuery, ScenarioTests.s_multiTenantDBs[i]),
                conn))
            {
                _ = cmd.ExecuteNonQuery();
            }

            using (var cmd = new SqlCommand(
                string.Format(Globals.CreateDatabaseQuery, ScenarioTests.s_multiTenantDBs[i]),
                conn))
            {
                _ = cmd.ExecuteNonQuery();
            }
        }
    }

    /// <summary>
    /// Cleans up common state for the all tests in this class.
    /// </summary>
    [ClassCleanup()]
    public static void ScenarioTestsCleanup()
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
    public void ScenarioTestInitialize()
    {
    }

    /// <summary>
    /// Cleans up common state per-test.
    /// </summary>
    [TestCleanup()]
    public void ScenarioTestCleanup()
    {
    }

    #endregion Common Methods

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void BasicScenarioDefaultShardMaps()
    {
        var success = true;

        try
        {
            #region DeployShardMapManager

            // Deploy shard map manager.
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            #endregion DeployShardMapManager

            #region GetShardMapManager

            // Obtain shard map manager.
            var shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            #endregion GetShardMapManager

            #region CreateDefaultShardMap

            // Create a single user per-tenant shard map.
            ShardMap defaultShardMap = shardMapManager.CreateListShardMap<int>("DefaultShardMap");

            #endregion CreateDefaultShardMap

            #region CreateShard

            for (var i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
            {
                // Create the shard.
                defaultShardMap.CreateShard(
                    new ShardLocation(
                        Globals.ShardMapManagerTestsDatasourceName,
                        ScenarioTests.s_perTenantDBs[i]));
            }

            #endregion CreateShard

            #region UpdateShard

            // Find the shard by location.
            var shardToUpdate = defaultShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, "PerTenantDB1"));

            // Perform the actual update. Mark offline.
            var updatedShard = defaultShardMap.UpdateShard(
                shardToUpdate,
                new ShardUpdate
                {
                    Status = ShardStatus.Offline
                });

            // Verify that update succeeded.
            Assert.AreEqual(ShardStatus.Offline, updatedShard.Status);

            #endregion UpdateShard

            #region DeleteShard

            // Find the shard by location.
            var shardToDelete = defaultShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, "PerTenantDB4"));

            defaultShardMap.DeleteShard(shardToDelete);

            // Verify that delete succeeded.

            defaultShardMap.TryGetShard(shardToDelete.Location, out var deletedShard);

            Assert.IsNull(deletedShard);

            // Now add the shard back for further tests.
            // Create the shard.
            defaultShardMap.CreateShard(shardToDelete.Location);

            #endregion DeleteShard

            #region OpenConnection without Validation

            // Find the shard by location.
            var shardForConnection = defaultShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, "PerTenantDB1"));

            using (var conn = shardForConnection.OpenConnection(
                Globals.ShardUserConnectionString,
                ConnectionOptions.None)) // validate = false
            {
            }

            #endregion OpenConnection without Validation

            #region OpenConnection with Validation

            // Use the stale state of "shardToUpdate" shard & see if validation works.
            var validationFailed = false;
            try
            {
                using var conn = shardToDelete.OpenConnection(
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate); // validate = true
            }
            catch (ShardManagementException smme)
            {
                validationFailed = true;
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.ShardDoesNotExist);
            }

            Assert.AreEqual(validationFailed, true);

            #endregion OpenConnection with Validation

            #region OpenConnectionAsync without Validation

            // Find the shard by location.
            shardForConnection = defaultShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, "PerTenantDB1"));

            using (var conn = shardForConnection.OpenConnectionAsync(
                Globals.ShardUserConnectionString,
                ConnectionOptions.None).Result) // validate = false
            {
            }

            #endregion OpenConnectionAsync without Validation

            #region OpenConnectionAsync with Validation

            // Use the stale state of "shardToUpdate" shard & see if validation works.
            validationFailed = false;
            try
            {
                using var conn = shardToDelete.OpenConnectionAsync(
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate).Result; // validate = true
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException is ShardManagementException smme)
                {
                    validationFailed = true;
                    Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.ShardDoesNotExist);
                }
            }

            Assert.AreEqual(validationFailed, true);

            var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

            if (sqlAuthLogin.Create())
            {
                // Also verify we can connect to the shard with Sql Auth, and Sql Auth using a secure credential
                using (shardForConnection.OpenConnectionAsync(
                    "TrustServerCertificate=true",
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    ConnectionOptions.None).Result)
                {
                }

                using (shardForConnection.OpenConnectionAsync(
                    "TrustServerCertificate=true",
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName)).Result)
                {
                }

                // Drop test login
                sqlAuthLogin.Drop();
            }
            else
            {
                Assert.Inconclusive("Failed to create sql login, test skipped");
            }

            // Ensure code coverage of other overloads
            using (shardForConnection.OpenConnectionAsync(
                Globals.ShardUserConnectionString).Result)
            {
            }

            #endregion OpenConnectionAsync with Validation

#if FUTUREWORK
            #region GetAllOnlineShards

            // Get all online shards.
            foreach (Shard s in defaultShardMap.GetShards(Int32.MaxValue, 1))
            {
                Trace.WriteLine(s.Location);
            }

            #endregion GetAllOnlineShards
#endif
        }
        catch (ShardManagementException smme)
        {
            success = false;

            Trace.WriteLine(string.Format("Error Category: {0}", smme.ErrorCategory));
            Trace.WriteLine(string.Format("Error Code    : {0}", smme.ErrorCode));
            Trace.WriteLine(string.Format("Error Message : {0}", smme.Message));

            if (smme.InnerException != null)
            {
                Trace.WriteLine(string.Format("Storage Error Message : {0}", smme.InnerException.Message));

                if (smme.InnerException.InnerException != null)
                {
                    Trace.WriteLine(string.Format("SqlClient Error Message : {0}", smme.InnerException.InnerException.Message));
                }
            }
        }

        Assert.IsTrue(success);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void BasicScenarioListShardMapsWithIntegratedSecurity() => BasicScenarioListShardMapsInternal(Globals.ShardMapManagerConnectionString, Globals.ShardUserConnectionString);

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void BasicScenarioListShardMapsWithSqlAuthentication()
    {
        // Try to create a test login
        var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

        if (sqlAuthLogin.Create())
        {
            var gsmSb = new SqlConnectionStringBuilder(Globals.ShardMapManagerConnectionString)
            {
                IntegratedSecurity = false,
                UserID = sqlAuthLogin.UniquifiedUserName,
                Password = Globals.SqlLoginTestPassword,
            };

            var lsmSb = new SqlConnectionStringBuilder(Globals.ShardUserConnectionString)
            {
                IntegratedSecurity = false,
                UserID = sqlAuthLogin.UniquifiedUserName,
                Password = Globals.SqlLoginTestPassword,
            };

            BasicScenarioListShardMapsInternal(gsmSb.ConnectionString, lsmSb.ConnectionString);

            // Drop test login
            sqlAuthLogin.Drop();
        }
        else
        {
            Assert.Inconclusive("Failed to create sql login, test skipped");
        }
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void BasicScenarioListShardMapsWithSecureSqlAuthentication()
    {
        // Try to create a test login
        var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

        if (sqlAuthLogin.Create())
        {
            var gsmSb = new SqlConnectionStringBuilder(Globals.ShardMapManagerConnectionString)
            {
                IntegratedSecurity = false,
            };

            var lsmSb = new SqlConnectionStringBuilder(Globals.ShardUserConnectionString)
            {
                IntegratedSecurity = false,
            };

            BasicScenarioListShardMapsInternal(
                gsmSb.ConnectionString,
                lsmSb.ConnectionString,
                Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName));

            // Drop test login
            sqlAuthLogin.Drop();
        }
        else
        {
            Assert.Inconclusive("Failed to create sql login, test skipped");
        }
    }

    private void BasicScenarioListShardMapsInternal(
        string shardMapManagerConnectionString,
        string shardUserConnectionString,
        SqlCredential shardMapManagerSqlCredential = null,
        SqlCredential shardUserSqlCredential = null)
    {
        var success = true;
        var shardMapName = "PerTenantShardMap";

        try
        {
            #region DeployShardMapManager

            // Deploy shard map manager.
            _ = shardMapManagerSqlCredential == null
                ? ShardMapManagerFactory.CreateSqlShardMapManager(
                    shardMapManagerConnectionString,
                    ShardMapManagerCreateMode.ReplaceExisting)
                : ShardMapManagerFactory.CreateSqlShardMapManager(
                    shardMapManagerConnectionString,
                    shardMapManagerSqlCredential,
                    ShardMapManagerCreateMode.ReplaceExisting);

            #endregion DeployShardMapManager

            #region GetShardMapManager

            // Obtain shard map manager.
            var shardMapManager = (shardMapManagerSqlCredential == null) ?
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy) :
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    shardMapManagerSqlCredential,
                    ShardMapManagerLoadPolicy.Lazy);


            #endregion GetShardMapManager

            #region CreateListShardMap

            // Create a single user per-tenant shard map.
            var perTenantShardMap = shardMapManager.CreateListShardMap<int>(shardMapName);

            #endregion CreateListShardMap

            #region CreateShardAndPointMapping

            for (var i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
            {
                // Create the shard.
                var s = perTenantShardMap.CreateShard(
                    new ShardLocation(
                        Globals.ShardMapManagerTestsDatasourceName,
                        ScenarioTests.s_perTenantDBs[i]));

                // Create the mapping.
                var p = perTenantShardMap.CreatePointMapping(
                    i + 1,
                    s);
            }

            #endregion CreateShardAndPointMapping

            #region UpdatePointMapping

            // Let's add another point 5 and map it to same shard as 1.

            var mappingForOne = perTenantShardMap.GetMappingForKey(1);

            var mappingForFive = perTenantShardMap.CreatePointMapping(5, mappingForOne.Shard);

            Assert.IsTrue(mappingForOne.Shard.Location.Equals(mappingForFive.Shard.Location));

            // Move 3 from PerTenantDB3 to PerTenantDB for 5.
            var mappingToUpdate = perTenantShardMap.GetMappingForKey(3);
            var updateFailed = false;

            // Try updating that shard in the mapping without taking it offline first.
            try
            {
                _ = perTenantShardMap.UpdateMapping(
                    mappingToUpdate,
                    new PointMappingUpdate
                    {
                        Shard = mappingForFive.Shard
                    });
            }
            catch (ShardManagementException smme)
            {
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingIsNotOffline);
                updateFailed = true;
            }

            Assert.IsTrue(updateFailed);

            // Perform the actual update.
            var newMappingFor3 = MarkMappingOfflineAndUpdateShard<int>(
                perTenantShardMap, mappingToUpdate, mappingForFive.Shard);

            // Verify that update succeeded.
            Assert.IsTrue(newMappingFor3.Shard.Location.Equals(mappingForFive.Shard.Location));
            Assert.IsTrue(newMappingFor3.Status == MappingStatus.Offline);

            // Update custom field for the updated mapping.
            //PointMapping<int> veryNewMappingFor3 = perTenantShardMap.UpdatePointMapping(
            //    newMappingFor3,
            //    new PointMappingUpdate
            //    {
            //        Custom = new byte[] { 0x12, 0x34 }
            //    });

            #endregion UpdatePointMapping

            #region DeleteMapping

            // Find the shard by location.
            var mappingToDelete = perTenantShardMap.GetMappingForKey(5);
            var operationFailed = false;

            // Try to delete mapping while it is online, the delete should fail.
            try
            {
                perTenantShardMap.DeleteMapping(mappingToDelete);
            }
            catch (ShardManagementException smme)
            {
                operationFailed = true;
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingIsNotOffline);
            }

            Trace.Assert(operationFailed);

            // The mapping must be taken offline first before it can be deleted.
            mappingToDelete = perTenantShardMap.UpdateMapping(
                mappingToDelete,
                new PointMappingUpdate
                {
                    Status = MappingStatus.Offline,
                });

            perTenantShardMap.DeleteMapping(mappingToDelete);

            // Verify that delete succeeded.
            try
            {
                var deletedMapping = perTenantShardMap.GetMappingForKey(5);
            }
            catch (ShardManagementException smme)
            {
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingNotFoundForKey);
            }

            #endregion DeleteMapping

            #region OpenConnection without Validation

            using (var conn = (shardUserSqlCredential == null) ?
                perTenantShardMap.OpenConnectionForKey(
                    2,
                    shardUserConnectionString,
                    ConnectionOptions.None) :
                perTenantShardMap.OpenConnectionForKey(
                    2,
                    shardUserConnectionString,
                    shardUserSqlCredential,
                    ConnectionOptions.None))
            {
            }

            #endregion OpenConnection without Validation

            #region OpenConnection with Validation

            // Use the stale state of "shardToUpdate" shard & see if validation works.
            var validationFailed = false;
            try
            {
                using var conn = (shardUserSqlCredential == null) ?
                    perTenantShardMap.OpenConnection(
                        mappingToDelete,
                        shardUserConnectionString,
                        ConnectionOptions.Validate) :
                    perTenantShardMap.OpenConnection(
                        mappingToDelete,
                        new SqlConnectionInfo(
                            shardUserConnectionString,
                            shardUserSqlCredential),
                        ConnectionOptions.Validate);
            }
            catch (ShardManagementException smme)
            {
                validationFailed = true;
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
            }

            Assert.AreEqual(validationFailed, true);

            #endregion OpenConnection with Validation

            #region OpenConnection without Validation and Empty Cache

            // Obtain a new ShardMapManager instance
            var newShardMapManager = (shardMapManagerSqlCredential == null) ?
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy) :
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    shardMapManagerSqlCredential,
                    ShardMapManagerLoadPolicy.Lazy);

            // Get the ShardMap
            var newPerTenantShardMap = newShardMapManager.GetListShardMap<int>(shardMapName);

            using (var conn = (shardUserSqlCredential == null) ?
                newPerTenantShardMap.OpenConnectionForKey(
                    2,
                    shardUserConnectionString,
                    ConnectionOptions.None) :
                newPerTenantShardMap.OpenConnectionForKey(
                    2,
                    shardUserConnectionString,
                    shardUserSqlCredential,
                    ConnectionOptions.None))
            {
            }

            #endregion

            #region OpenConnection with Validation and Empty Cache

            // Use the stale state of "shardToUpdate" shard & see if validation works.
            validationFailed = false;

            // Obtain a new ShardMapManager instance
            newShardMapManager = (shardMapManagerSqlCredential == null) ?
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy) :
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    shardMapManagerSqlCredential,
                    ShardMapManagerLoadPolicy.Lazy);

            // Get the ShardMap
            newPerTenantShardMap = newShardMapManager.GetListShardMap<int>(shardMapName);

            // Create a new mapping
            var newMappingToDelete = newPerTenantShardMap.CreatePointMapping(6,
                newPerTenantShardMap.GetMappingForKey(1).Shard);

            // Delete the mapping
            newMappingToDelete = newPerTenantShardMap.UpdateMapping(
                newMappingToDelete,
                new PointMappingUpdate
                {
                    Status = MappingStatus.Offline,
                });

            newPerTenantShardMap.DeleteMapping(newMappingToDelete);

            try
            {
                using var conn = (shardUserSqlCredential == null) ?
                    newPerTenantShardMap.OpenConnection(
                        newMappingToDelete,
                        shardUserConnectionString,
                        ConnectionOptions.Validate) :
                    newPerTenantShardMap.OpenConnection(
                        newMappingToDelete,
                        new SqlConnectionInfo(
                            shardUserConnectionString,
                            shardUserSqlCredential),
                        ConnectionOptions.Validate);
            }
            catch (ShardManagementException smme)
            {
                validationFailed = true;
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
            }

            Assert.AreEqual(validationFailed, true);

            #endregion

            #region OpenConnectionAsync without Validation

            using (var conn = (shardUserSqlCredential == null) ?
                perTenantShardMap.OpenConnectionForKeyAsync(
                    2,
                    shardUserConnectionString,
                    ConnectionOptions.None).Result :
                perTenantShardMap.OpenConnectionForKeyAsync(
                    2,
                    shardUserConnectionString,
                    shardUserSqlCredential,
                    ConnectionOptions.None).Result)
            {
            }

            #endregion

            #region OpenConnectionAsync with Validation

            // Use the stale state of "shardToUpdate" shard & see if validation works.
            validationFailed = false;
            try
            {
                using var conn = (shardUserSqlCredential == null) ?
                    perTenantShardMap.OpenConnectionAsync(
                        mappingToDelete,
                        shardUserConnectionString,
                        ConnectionOptions.Validate).Result :
                    perTenantShardMap.OpenConnectionAsync(
                        mappingToDelete,
                        new SqlConnectionInfo(
                            shardUserConnectionString,
                            shardUserSqlCredential),
                        ConnectionOptions.Validate).Result;
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException is ShardManagementException smme)
                {
                    validationFailed = true;
                    Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                }
            }

            Assert.AreEqual(validationFailed, true);

            #endregion

            #region OpenConnectionAsync without Validation and Empty Cache

            // Obtain a new ShardMapManager instance
            newShardMapManager = (shardMapManagerSqlCredential == null) ?
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy) :
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    shardMapManagerSqlCredential,
                    ShardMapManagerLoadPolicy.Lazy);

            // Get the ShardMap
            newPerTenantShardMap = newShardMapManager.GetListShardMap<int>(shardMapName);
            using (var conn = (shardUserSqlCredential == null) ?
                newPerTenantShardMap.OpenConnectionForKeyAsync(
                    2,
                    shardUserConnectionString,
                    ConnectionOptions.None).Result :
                newPerTenantShardMap.OpenConnectionForKeyAsync(
                    2,
                    shardUserConnectionString,
                    shardUserSqlCredential,
                    ConnectionOptions.None).Result)
            {
            }

            #endregion

            #region OpenConnectionAsync with Validation and Empty Cache

            // Use the stale state of "shardToUpdate" shard & see if validation works.
            validationFailed = false;

            // Obtain a new ShardMapManager instance
            newShardMapManager = (shardMapManagerSqlCredential == null) ?
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy) :
                ShardMapManagerFactory.GetSqlShardMapManager(
                    shardMapManagerConnectionString,
                    shardMapManagerSqlCredential,
                    ShardMapManagerLoadPolicy.Lazy);

            // Get the ShardMap
            newPerTenantShardMap = newShardMapManager.GetListShardMap<int>(shardMapName);

            // Create a new mapping
            newMappingToDelete = newPerTenantShardMap.CreatePointMapping(6,
                newPerTenantShardMap.GetMappingForKey(1).Shard);

            // Delete the mapping
            newMappingToDelete = newPerTenantShardMap.UpdateMapping(
                newMappingToDelete,
                new PointMappingUpdate
                {
                    Status = MappingStatus.Offline,
                });

            newPerTenantShardMap.DeleteMapping(newMappingToDelete);

            try
            {
                using var conn = (shardUserSqlCredential == null) ?
                    newPerTenantShardMap.OpenConnectionAsync(
                        newMappingToDelete,
                        shardUserConnectionString,
                        ConnectionOptions.Validate).Result :
                    newPerTenantShardMap.OpenConnectionAsync(
                        newMappingToDelete,
                        new SqlConnectionInfo(
                            shardUserConnectionString,
                            shardUserSqlCredential),
                        ConnectionOptions.Validate).Result;
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException is ShardManagementException smme)
                {
                    validationFailed = true;
                    Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                }
            }

            Assert.AreEqual(validationFailed, true);

            #endregion

            #region LookupPointMapping

            // Perform tenant lookup. This will populate the cache.
            for (var i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
            {
                var result = shardMapManager
                    .GetListShardMap<int>("PerTenantShardMap")
                    .GetMappingForKey(i + 1);

                Trace.WriteLine(result.Shard.Location);

                // Since we moved 3 to database 1 earlier.
                Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_perTenantDBs[i != 2 ? i : 0]);
            }

            // Perform tenant lookup. This will read from the cache.
            for (var i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
            {
                var result = shardMapManager
                    .GetListShardMap<int>("PerTenantShardMap")
                    .GetMappingForKey(i + 1);

                Trace.WriteLine(result.Shard.Location);

                // Since we moved 3 to database 1 earlier.
                Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_perTenantDBs[i != 2 ? i : 0]);
            }

            #endregion LookupPointMapping
        }
        catch (ShardManagementException smme)
        {
            success = false;

            Trace.WriteLine(string.Format("Error Category: {0}", smme.ErrorCategory));
            Trace.WriteLine(string.Format("Error Code    : {0}", smme.ErrorCode));
            Trace.WriteLine(string.Format("Error Message : {0}", smme.Message));

            if (smme.InnerException != null)
            {
                Trace.WriteLine(string.Format("Storage Error Message : {0}", smme.InnerException.Message));

                if (smme.InnerException.InnerException != null)
                {
                    Trace.WriteLine(string.Format("SqlClient Error Message : {0}", smme.InnerException.InnerException.Message));
                }
            }
        }

        Assert.IsTrue(success);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void BasicScenarioRangeShardMaps()
    {
        var success = true;
        var rangeShardMapName = "MultiTenantShardMap";

        try
        {
            #region DeployShardMapManager

            // Deploy shard map manager.
            _ = ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            #endregion DeployShardMapManager

            #region GetShardMapManager

            // Obtain shard map manager.
            var shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

            #endregion GetShardMapManager

            #region CreateRangeShardMap

            // Create a single user per-tenant shard map.
            var multiTenantShardMap = shardMapManager.CreateRangeShardMap<int>(rangeShardMapName);

            #endregion CreateRangeShardMap

            #region CreateShardAndRangeMapping

            for (var i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++)
            {
                // Create the shard.
                var s = multiTenantShardMap.CreateShard(
                    new ShardLocation(
                        Globals.ShardMapManagerTestsDatasourceName,
                        ScenarioTests.s_multiTenantDBs[i]));

                // Create the mapping.
                var r = multiTenantShardMap.CreateRangeMapping(
                    new Range<int>(i * 10, (i + 1) * 10),
                    s);
            }

            #endregion CreateShardAndRangeMapping

            #region UpdateMapping

            // Let's add [50, 60) and map it to same shard as 23 i.e. MultiTenantDB3.

            var mappingFor23 = multiTenantShardMap.GetMappingForKey(23);

            var mappingFor50To60 = multiTenantShardMap.CreateRangeMapping(
                new Range<int>(50, 60),
                mappingFor23.Shard);

            Assert.IsTrue(mappingFor23.Shard.Location.Equals(mappingFor50To60.Shard.Location));

            // Move [10, 20) from MultiTenantDB2 to MultiTenantDB1
            var mappingToUpdate = multiTenantShardMap.GetMappingForKey(10);
            var mappingFor5 = multiTenantShardMap.GetMappingForKey(5);
            var updateFailed = false;

            // Try updating that shard in the mapping without taking it offline first.
            try
            {
                _ = multiTenantShardMap.UpdateMapping(
                    mappingToUpdate,
                    new RangeMappingUpdate
                    {
                        Shard = mappingFor5.Shard
                    });
            }
            catch (ShardManagementException smme)
            {
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingIsNotOffline);
                updateFailed = true;
            }

            Trace.Assert(updateFailed);

            // Mark mapping offline, update shard location.
            var newMappingFor10To20Offline = MarkMappingOfflineAndUpdateShard<int>(
                multiTenantShardMap, mappingToUpdate, mappingFor5.Shard);

            // Verify that update succeeded.
            Assert.IsTrue(newMappingFor10To20Offline.Shard.Location.Equals(mappingFor5.Shard.Location));
            Assert.IsTrue(newMappingFor10To20Offline.Status == MappingStatus.Offline);

            // Bring the mapping back online.
            var newMappingFor10To20Online = multiTenantShardMap.UpdateMapping(
                newMappingFor10To20Offline,
                new RangeMappingUpdate
                {
                    Status = MappingStatus.Online,
                });

            // Verify that update succeeded.
            Assert.IsTrue(newMappingFor10To20Online.Status == MappingStatus.Online);

            #endregion UpdateMapping

            #region DeleteMapping

            // Find mapping for [0, 10).
            var mappingToDelete = multiTenantShardMap.GetMappingForKey(5);
            var operationFailed = false;

            // Try to delete mapping while it is online, the delete should fail.
            try
            {
                multiTenantShardMap.DeleteMapping(mappingToDelete);
            }
            catch (ShardManagementException smme)
            {
                operationFailed = true;
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingIsNotOffline);
            }

            Trace.Assert(operationFailed);

            // The mapping must be made offline first before it can be deleted.
            var ru = new RangeMappingUpdate
            {
                Status = MappingStatus.Offline
            };

            mappingToDelete = multiTenantShardMap.UpdateMapping(mappingToDelete, ru);
            Trace.Assert(mappingToDelete.Status == MappingStatus.Offline);

            multiTenantShardMap.DeleteMapping(mappingToDelete);

            // Verify that delete succeeded.
            try
            {
                var deletedMapping = multiTenantShardMap.GetMappingForKey(5);
            }
            catch (ShardManagementException smme)
            {
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingNotFoundForKey);
            }

            #endregion DeleteMapping

            #region OpenConnection without Validation

            using (var conn = multiTenantShardMap.OpenConnectionForKey(
                20,
                Globals.ShardUserConnectionString,
                ConnectionOptions.None))
            {
            }

            #endregion OpenConnection without Validation

            #region OpenConnection with Validation

            // Use the stale state of "shardToUpdate" shard & see if validation works.
            var validationFailed = false;
            try
            {
                using var conn = multiTenantShardMap.OpenConnection(
                    mappingToDelete,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate);
            }
            catch (ShardManagementException smme)
            {
                validationFailed = true;
                Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
            }

            Assert.AreEqual(validationFailed, true);

            #endregion OpenConnection with Validation

            #region OpenConnection without Validation and Empty Cache

            // Obtain new shard map manager instance
            var newShardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

            // Get the Range Shard Map
            var newMultiTenantShardMap = newShardMapManager.GetRangeShardMap<int>(rangeShardMapName);

            using (var conn = newMultiTenantShardMap.OpenConnectionForKey(
                20,
                Globals.ShardUserConnectionString,
                ConnectionOptions.None))
            {
            }

            // Try to create a test login
            var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

            if (sqlAuthLogin.Create())
            {
                // Cover the OpenConnectionForKey overloads
                using (var conn = newMultiTenantShardMap.OpenConnectionForKey(
                    20,
                    "TrustServerCertificate=true",
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName)))
                {
                }

                #endregion

                #region OpenConnection with Validation and Empty Cache

                // Obtain new shard map manager instance
                newShardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                // Get the Range Shard Map
                newMultiTenantShardMap = newShardMapManager.GetRangeShardMap<int>(rangeShardMapName);

                // Create a new mapping
                var newMappingToDelete = newMultiTenantShardMap.CreateRangeMapping(
                    new Range<int>(70, 80),
                    newMultiTenantShardMap.GetMappingForKey(23).Shard);

                // Delete the mapping
                newMappingToDelete = newMultiTenantShardMap.UpdateMapping(
                    newMappingToDelete,
                    new RangeMappingUpdate
                    {
                        Status = MappingStatus.Offline,
                    });

                newMultiTenantShardMap.DeleteMapping(newMappingToDelete);

                // Use the stale state of "shardToUpdate" shard & see if validation works.
                validationFailed = false;

                try
                {
                    using var conn = newMultiTenantShardMap.OpenConnection(
                        newMappingToDelete,
                        Globals.ShardUserConnectionString,
                        ConnectionOptions.Validate);
                }
                catch (ShardManagementException smme)
                {
                    validationFailed = true;
                    Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                }

                Assert.AreEqual(validationFailed, true);

                #endregion

                #region OpenConnectionAsync without Validation

                using (var conn = multiTenantShardMap.OpenConnectionForKeyAsync(
                    20,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.None).Result)
                {
                }

                #endregion

                #region OpenConnectionAsync with Validation

                // Use the stale state of "shardToUpdate" shard & see if validation works.
                validationFailed = false;
                try
                {
                    using var conn = multiTenantShardMap.OpenConnectionAsync(
                        mappingToDelete,
                        Globals.ShardUserConnectionString,
                        ConnectionOptions.Validate).Result;
                }
                catch (AggregateException ex)
                {
                    if (ex.InnerException is ShardManagementException smme)
                    {
                        validationFailed = true;
                        Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                    }
                }

                Assert.AreEqual(validationFailed, true);

                #endregion

                // Cover the OpenConnectionForKeyAsync overloads
                using (var conn = multiTenantShardMap.OpenConnectionForKeyAsync(
                    20,
                    "TrustServerCertificate=true",
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                    ConnectionOptions.None).Result)
                {
                }

                using (var conn = multiTenantShardMap.OpenConnectionForKeyAsync(
                    20,
                    "TrustServerCertificate=true",
                    Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName)).Result)
                {
                }


                #region OpenConnectionAsync without Validation and Empty Cache

                // Obtain new shard map manager instance
                newShardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                // Get the Range Shard Map
                newMultiTenantShardMap = newShardMapManager.GetRangeShardMap<int>(rangeShardMapName);

                using (var conn = newMultiTenantShardMap.OpenConnectionForKeyAsync(
                    20,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.None).Result)
                {
                }

                #endregion

                #region OpenConnectionAsync with Validation and Empty Cache

                // Obtain new shard map manager instance
                newShardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                // Get the Range Shard Map
                newMultiTenantShardMap = newShardMapManager.GetRangeShardMap<int>(rangeShardMapName);

                // Create a new mapping
                newMappingToDelete = newMultiTenantShardMap.CreateRangeMapping(
                    new Range<int>(70, 80),
                    newMultiTenantShardMap.GetMappingForKey(23).Shard);

                // Delete the mapping
                newMappingToDelete = newMultiTenantShardMap.UpdateMapping(
                    newMappingToDelete,
                    new RangeMappingUpdate
                    {
                        Status = MappingStatus.Offline,
                    });

                newMultiTenantShardMap.DeleteMapping(newMappingToDelete);

                // Use the stale state of "shardToUpdate" shard & see if validation works.
                validationFailed = false;
                try
                {
                    using var conn = newMultiTenantShardMap.OpenConnectionAsync(
                        newMappingToDelete,
                        Globals.ShardUserConnectionString,
                        ConnectionOptions.Validate).Result;
                }
                catch (AggregateException ex)
                {
                    if (ex.InnerException is ShardManagementException smme)
                    {
                        validationFailed = true;
                        Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                    }
                }

                Assert.AreEqual(validationFailed, true);
            }

            #endregion

            #region GetMapping

            // Perform tenant lookup. This will populate the cache.
            for (var i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++)
            {
                var result = shardMapManager
                    .GetRangeShardMap<int>("MultiTenantShardMap")
                    .GetMappingForKey((i + 1) * 10);

                Trace.WriteLine(result.Shard.Location);

                if (i == 0)
                {
                    // Since we moved [10,20) to database 1 earlier.
                    Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_multiTenantDBs[0]);
                }
                else
                    if (i < 4)
                {
                    Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_multiTenantDBs[i + 1]);
                }
                else
                {
                    Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_multiTenantDBs[2]);
                }
            }

            // Perform tenant lookup. This will read from the cache.
            for (var i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++)
            {
                var result = shardMapManager
                    .GetRangeShardMap<int>("MultiTenantShardMap")
                    .GetMappingForKey((i + 1) * 10);

                Trace.WriteLine(result.Shard.Location);

                if (i == 0)
                {
                    // Since we moved [10,20) to database 1 earlier.
                    Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_multiTenantDBs[0]);
                }
                else
                    if (i < 4)
                {
                    Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_multiTenantDBs[i + 1]);
                }
                else
                {
                    Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_multiTenantDBs[2]);
                }
            }

            #endregion GetMapping

            #region Split/Merge

            var splitPoint = 55;

            // Split [50, 60) into [50, 55) and [55, 60)
            var mappingToSplit = multiTenantShardMap.GetMappingForKey(splitPoint);

            var rangesAfterSplit = multiTenantShardMap.SplitMapping(mappingToSplit, splitPoint);

            rangesAfterSplit = rangesAfterSplit.OrderBy(nr => nr.Value.Low).ToArray();

            // We should get 2 ranges back.
            Assert.AreEqual(2, rangesAfterSplit.Count);

            Assert.AreEqual(rangesAfterSplit[0].Value.Low, new Range<int>(50, 55).Low);
            Assert.AreEqual(rangesAfterSplit[0].Value.High, new Range<int>(50, 55).High);
            Assert.AreEqual(rangesAfterSplit[1].Value.Low, new Range<int>(55, 60).Low);
            Assert.AreEqual(rangesAfterSplit[1].Value.High, new Range<int>(55, 60).High);

            // Split [50, 55) into [50, 52) and [52, 55)
            var newRangesAfterAdd = multiTenantShardMap.SplitMapping(rangesAfterSplit[0], 52);

            newRangesAfterAdd = newRangesAfterAdd.OrderBy(nr => nr.Value.Low).ToArray();

            // We should get 2 ranges back.
            Assert.AreEqual(2, newRangesAfterAdd.Count);

            Assert.AreEqual(newRangesAfterAdd[0].Value.Low, new Range<int>(50, 52).Low);
            Assert.AreEqual(newRangesAfterAdd[0].Value.High, new Range<int>(50, 52).High);
            Assert.AreEqual(newRangesAfterAdd[1].Value.Low, new Range<int>(52, 55).Low);
            Assert.AreEqual(newRangesAfterAdd[1].Value.High, new Range<int>(52, 55).High);

            // Move [50, 52) to MultiTenantDB1

            var targetShard = multiTenantShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ScenarioTests.s_multiTenantDBs[0]));

            // Mark mapping offline, update shard location.
            var movedMapping1 = MarkMappingOfflineAndUpdateShard<int>(
                multiTenantShardMap, newRangesAfterAdd[0], targetShard);

            // Bring the mapping back online.
            movedMapping1 = multiTenantShardMap.UpdateMapping(
                movedMapping1,
                new RangeMappingUpdate
                {
                    Status = MappingStatus.Online,
                });


            // Mark mapping offline, update shard location.
            var movedMapping2 = MarkMappingOfflineAndUpdateShard<int>(
                multiTenantShardMap, newRangesAfterAdd[1], targetShard);

            // Bring the mapping back online.
            movedMapping2 = multiTenantShardMap.UpdateMapping(
                movedMapping2,
                new RangeMappingUpdate
                {
                    Status = MappingStatus.Online,
                });

            // Obtain the final moved mapping.
            var finalMovedMapping = multiTenantShardMap.MergeMappings(movedMapping1, movedMapping2);

            Assert.AreEqual(finalMovedMapping.Value.Low, new Range<int>(50, 55).Low);
            Assert.AreEqual(finalMovedMapping.Value.High, new Range<int>(50, 55).High);

            #endregion Split/Merge
        }
        catch (ShardManagementException smme)
        {
            success = false;

            Trace.WriteLine(string.Format("Error Category: {0}", smme.ErrorCategory));
            Trace.WriteLine(string.Format("Error Code    : {0}", smme.ErrorCode));
            Trace.WriteLine(string.Format("Error Message : {0}", smme.Message));

            if (smme.InnerException != null)
            {
                Trace.WriteLine(string.Format("Storage Error Message : {0}", smme.InnerException.Message));

                if (smme.InnerException.InnerException != null)
                {
                    Trace.WriteLine(string.Format("SqlClient Error Message : {0}", smme.InnerException.InnerException.Message));
                }
            }
        }

        Assert.IsTrue(success);
    }

    [TestMethod()]
    [TestCategory("ExcludeFromGatedCheckin")]
    public void ListShardMapPerformanceCounterValidation()
    {
    }

    private RangeMapping<T> MarkMappingOfflineAndUpdateShard<T>(RangeShardMap<T> map, RangeMapping<T> mapping, Shard newShard)
    {
        var mappingOffline = map.UpdateMapping(
                mapping,
                new RangeMappingUpdate
                {
                    Status = MappingStatus.Offline
                });
        Assert.IsNotNull(mappingOffline);

        return map.UpdateMapping(
                mappingOffline,
                new RangeMappingUpdate
                {
                    Shard = newShard
                });
    }

    private PointMapping<T> MarkMappingOfflineAndUpdateShard<T>(ListShardMap<T> map, PointMapping<T> mapping, Shard newShard)
    {
        var mappingOffline = map.UpdateMapping(
                mapping,
                new PointMappingUpdate
                {
                    Status = MappingStatus.Offline
                });
        Assert.IsNotNull(mappingOffline);

        return map.UpdateMapping(
                mappingOffline,
                new PointMappingUpdate
                {
                    Shard = newShard
                });
    }
}
