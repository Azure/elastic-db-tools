// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    using ClientTestCommon;

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
        private static string[] s_perTenantDBs = new[]
        {
            "PerTenantDB1", "PerTenantDB2", "PerTenantDB3", "PerTenantDB4"
        };

        // Shards with multiple users per tenant model.
        private static string[] s_multiTenantDBs = new[]
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

                // Create PerTenantDB databases
                for (int i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ScenarioTests.s_perTenantDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ScenarioTests.s_perTenantDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                // Create MultiTenantDB databases
                for (int i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ScenarioTests.s_multiTenantDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ScenarioTests.s_multiTenantDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
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
            bool success = true;

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
                ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                #endregion GetShardMapManager

                #region CreateDefaultShardMap

                // Create a single user per-tenant shard map.
                ShardMap defaultShardMap = shardMapManager.CreateListShardMap<int>("DefaultShardMap");

                #endregion CreateDefaultShardMap

                #region CreateShard

                for (int i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
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
                Shard shardToUpdate = defaultShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, "PerTenantDB1"));

                // Perform the actual update. Mark offline.
                Shard updatedShard = defaultShardMap.UpdateShard(
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
                Shard shardToDelete = defaultShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, "PerTenantDB4"));

                defaultShardMap.DeleteShard(shardToDelete);

                // Verify that delete succeeded.
                Shard deletedShard;

                defaultShardMap.TryGetShard(shardToDelete.Location, out deletedShard);

                Assert.IsNull(deletedShard);

                // Now add the shard back for further tests.
                // Create the shard.
                defaultShardMap.CreateShard(shardToDelete.Location);

                #endregion DeleteShard

                #region OpenConnection without Validation

                // Find the shard by location.
                Shard shardForConnection = defaultShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, "PerTenantDB1"));

                using (SqlConnection conn = shardForConnection.OpenConnection(
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.None)) // validate = false
                {
                }

                #endregion OpenConnection without Validation

                #region OpenConnection with Validation

                // Use the stale state of "shardToUpdate" shard & see if validation works.
                bool validationFailed = false;
                try
                {
                    using (SqlConnection conn = shardToDelete.OpenConnection(
                        Globals.ShardUserConnectionString,
                        ConnectionOptions.Validate)) // validate = true
                    {
                    }
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

                using (SqlConnection conn = shardForConnection.OpenConnectionAsync(
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
                    using (SqlConnection conn = shardToDelete.OpenConnectionAsync(
                        Globals.ShardUserConnectionString,
                        ConnectionOptions.Validate).Result) // validate = true
                    {
                    }
                }
                catch (AggregateException ex)
                {
                    ShardManagementException smme = ex.InnerException as ShardManagementException;
                    if (smme != null)
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
                        string.Empty,
                        Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                        ConnectionOptions.None).Result)
                    {
                    }

                    using (shardForConnection.OpenConnectionAsync(
                        string.Empty,
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

                Trace.WriteLine(String.Format("Error Category: {0}", smme.ErrorCategory));
                Trace.WriteLine(String.Format("Error Code    : {0}", smme.ErrorCode));
                Trace.WriteLine(String.Format("Error Message : {0}", smme.Message));

                if (smme.InnerException != null)
                {
                    Trace.WriteLine(String.Format("Storage Error Message : {0}", smme.InnerException.Message));

                    if (smme.InnerException.InnerException != null)
                    {
                        Trace.WriteLine(String.Format("SqlClient Error Message : {0}", smme.InnerException.InnerException.Message));
                    }
                }
            }

            Assert.IsTrue(success);
        }

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void BasicScenarioListShardMapsWithIntegratedSecurity()
        {
            BasicScenarioListShardMapsInternal(Globals.ShardMapManagerConnectionString, Globals.ShardUserConnectionString);
        }

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void BasicScenarioListShardMapsWithSqlAuthentication()
        {
            // Try to create a test login
            var sqlAuthLogin = new SqlAuthenticationLogin(Globals.ShardMapManagerConnectionString, Globals.SqlLoginTestUser, Globals.SqlLoginTestPassword);

            if (sqlAuthLogin.Create())
            {
                SqlConnectionStringBuilder gsmSb = new SqlConnectionStringBuilder(Globals.ShardMapManagerConnectionString)
                {
                    IntegratedSecurity = false,
                    UserID = sqlAuthLogin.UniquifiedUserName,
                    Password = Globals.SqlLoginTestPassword,
                };

                SqlConnectionStringBuilder lsmSb = new SqlConnectionStringBuilder(Globals.ShardUserConnectionString)
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
                SqlConnectionStringBuilder gsmSb = new SqlConnectionStringBuilder(Globals.ShardMapManagerConnectionString)
                                                       {
                                                           IntegratedSecurity = false,
                                                       };

                SqlConnectionStringBuilder lsmSb = new SqlConnectionStringBuilder(Globals.ShardUserConnectionString)
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
            bool success = true;
            string shardMapName = "PerTenantShardMap";

            try
            {
                #region DeployShardMapManager

                // Deploy shard map manager.
                if (shardMapManagerSqlCredential == null)
                {
                    ShardMapManagerFactory.CreateSqlShardMapManager(
                        shardMapManagerConnectionString,
                        ShardMapManagerCreateMode.ReplaceExisting);
                }
                else
                {
                    ShardMapManagerFactory.CreateSqlShardMapManager(
                        shardMapManagerConnectionString,
                        shardMapManagerSqlCredential,
                        ShardMapManagerCreateMode.ReplaceExisting);
                }

                #endregion DeployShardMapManager

                #region GetShardMapManager

                // Obtain shard map manager.
                ShardMapManager shardMapManager = (shardMapManagerSqlCredential == null) ? 
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
                ListShardMap<int> perTenantShardMap = shardMapManager.CreateListShardMap<int>(shardMapName);

                #endregion CreateListShardMap

                #region CreateShardAndPointMapping

                for (int i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
                {
                    // Create the shard.
                    Shard s = perTenantShardMap.CreateShard(
                        new ShardLocation(
                            Globals.ShardMapManagerTestsDatasourceName,
                            ScenarioTests.s_perTenantDBs[i]));

                    // Create the mapping.
                    PointMapping<int> p = perTenantShardMap.CreatePointMapping(
                        i + 1,
                        s);
                }

                #endregion CreateShardAndPointMapping

                #region UpdatePointMapping

                // Let's add another point 5 and map it to same shard as 1.

                PointMapping<int> mappingForOne = perTenantShardMap.GetMappingForKey(1);

                PointMapping<int> mappingForFive = perTenantShardMap.CreatePointMapping(5, mappingForOne.Shard);

                Assert.IsTrue(mappingForOne.Shard.Location.Equals(mappingForFive.Shard.Location));

                // Move 3 from PerTenantDB3 to PerTenantDB for 5.
                PointMapping<int> mappingToUpdate = perTenantShardMap.GetMappingForKey(3);
                bool updateFailed = false;

                // Try updating that shard in the mapping without taking it offline first.
                try
                {
                    perTenantShardMap.UpdateMapping(
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
                PointMapping<int> newMappingFor3 = MarkMappingOfflineAndUpdateShard<int>(
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
                PointMapping<int> mappingToDelete = perTenantShardMap.GetMappingForKey(5);
                bool operationFailed = false;

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
                    PointMapping<int> deletedMapping = perTenantShardMap.GetMappingForKey(5);
                }
                catch (ShardManagementException smme)
                {
                    Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingNotFoundForKey);
                }

                #endregion DeleteMapping

                #region OpenConnection without Validation

                using (SqlConnection conn = (shardUserSqlCredential == null) ?
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
                bool validationFailed = false;
                try
                {
                    using (SqlConnection conn = (shardUserSqlCredential == null) ?
                        perTenantShardMap.OpenConnection(
                            mappingToDelete,
                            shardUserConnectionString,
                            ConnectionOptions.Validate) :
                        perTenantShardMap.OpenConnection(
                            mappingToDelete,
                            new SqlConnectionInfo(
                                shardUserConnectionString,
                                shardUserSqlCredential),
                            ConnectionOptions.Validate))
                    {
                    }
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
                ShardMapManager newShardMapManager = (shardMapManagerSqlCredential == null) ? 
                    ShardMapManagerFactory.GetSqlShardMapManager(
                        shardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy) :
                    ShardMapManagerFactory.GetSqlShardMapManager(
                        shardMapManagerConnectionString,
                        shardMapManagerSqlCredential,
                        ShardMapManagerLoadPolicy.Lazy);

                // Get the ShardMap
                ListShardMap<int> newPerTenantShardMap = newShardMapManager.GetListShardMap<int>(shardMapName);

                using (SqlConnection conn = (shardUserSqlCredential == null) ?
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
                PointMapping<int> newMappingToDelete = newPerTenantShardMap.CreatePointMapping(6,
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
                    using (SqlConnection conn = (shardUserSqlCredential == null) ?
                        newPerTenantShardMap.OpenConnection(
                            newMappingToDelete,
                            shardUserConnectionString,
                            ConnectionOptions.Validate) :
                        newPerTenantShardMap.OpenConnection(
                            newMappingToDelete,
                            new SqlConnectionInfo(
                                shardUserConnectionString,
                                shardUserSqlCredential),
                            ConnectionOptions.Validate))
                    {
                    }
                }
                catch (ShardManagementException smme)
                {
                    validationFailed = true;
                    Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                }

                Assert.AreEqual(validationFailed, true);

                #endregion

                #region OpenConnectionAsync without Validation

                using (SqlConnection conn = (shardUserSqlCredential == null) ?
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
                    using (SqlConnection conn = (shardUserSqlCredential == null) ?
                        perTenantShardMap.OpenConnectionAsync(
                            mappingToDelete,
                            shardUserConnectionString,
                            ConnectionOptions.Validate).Result :
                        perTenantShardMap.OpenConnectionAsync(
                            mappingToDelete,
                            new SqlConnectionInfo(
                                shardUserConnectionString,
                                shardUserSqlCredential),
                            ConnectionOptions.Validate).Result)
                    {
                    }
                }
                catch (AggregateException ex)
                {
                    ShardManagementException smme = ex.InnerException as ShardManagementException;
                    if (smme != null)
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
                using (SqlConnection conn = (shardUserSqlCredential == null) ?
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
                    using (SqlConnection conn = (shardUserSqlCredential == null) ?
                        newPerTenantShardMap.OpenConnectionAsync(
                            newMappingToDelete,
                            shardUserConnectionString,
                            ConnectionOptions.Validate).Result :
                        newPerTenantShardMap.OpenConnectionAsync(
                            newMappingToDelete,
                            new SqlConnectionInfo(
                                shardUserConnectionString,
                                shardUserSqlCredential),
                            ConnectionOptions.Validate).Result)
                    {
                    }
                }
                catch (AggregateException ex)
                {
                    ShardManagementException smme = ex.InnerException as ShardManagementException;
                    if (smme != null)
                    {
                        validationFailed = true;
                        Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                    }
                }

                Assert.AreEqual(validationFailed, true);

                #endregion

                #region LookupPointMapping

                // Perform tenant lookup. This will populate the cache.
                for (int i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
                {
                    PointMapping<int> result = shardMapManager
                        .GetListShardMap<int>("PerTenantShardMap")
                        .GetMappingForKey(i + 1);

                    Trace.WriteLine(result.Shard.Location);

                    // Since we moved 3 to database 1 earlier.
                    Assert.IsTrue(result.Shard.Location.Database == ScenarioTests.s_perTenantDBs[i != 2 ? i : 0]);
                }

                // Perform tenant lookup. This will read from the cache.
                for (int i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++)
                {
                    PointMapping<int> result = shardMapManager
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

                Trace.WriteLine(String.Format("Error Category: {0}", smme.ErrorCategory));
                Trace.WriteLine(String.Format("Error Code    : {0}", smme.ErrorCode));
                Trace.WriteLine(String.Format("Error Message : {0}", smme.Message));

                if (smme.InnerException != null)
                {
                    Trace.WriteLine(String.Format("Storage Error Message : {0}", smme.InnerException.Message));

                    if (smme.InnerException.InnerException != null)
                    {
                        Trace.WriteLine(String.Format("SqlClient Error Message : {0}", smme.InnerException.InnerException.Message));
                    }
                }
            }

            Assert.IsTrue(success);
        }

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void BasicScenarioRangeShardMaps()
        {
            bool success = true;
            string rangeShardMapName = "MultiTenantShardMap";

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
                ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

                #endregion GetShardMapManager

                #region CreateRangeShardMap

                // Create a single user per-tenant shard map.
                RangeShardMap<int> multiTenantShardMap = shardMapManager.CreateRangeShardMap<int>(rangeShardMapName);

                #endregion CreateRangeShardMap

                #region CreateShardAndRangeMapping

                for (int i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++)
                {
                    // Create the shard.
                    Shard s = multiTenantShardMap.CreateShard(
                        new ShardLocation(
                            Globals.ShardMapManagerTestsDatasourceName,
                            ScenarioTests.s_multiTenantDBs[i]));

                    // Create the mapping.
                    RangeMapping<int> r = multiTenantShardMap.CreateRangeMapping(
                        new Range<int>(i * 10, (i + 1) * 10),
                        s);
                }

                #endregion CreateShardAndRangeMapping

                #region UpdateMapping

                // Let's add [50, 60) and map it to same shard as 23 i.e. MultiTenantDB3.

                RangeMapping<int> mappingFor23 = multiTenantShardMap.GetMappingForKey(23);

                RangeMapping<int> mappingFor50To60 = multiTenantShardMap.CreateRangeMapping(
                    new Range<int>(50, 60),
                    mappingFor23.Shard);

                Assert.IsTrue(mappingFor23.Shard.Location.Equals(mappingFor50To60.Shard.Location));

                // Move [10, 20) from MultiTenantDB2 to MultiTenantDB1
                RangeMapping<int> mappingToUpdate = multiTenantShardMap.GetMappingForKey(10);
                RangeMapping<int> mappingFor5 = multiTenantShardMap.GetMappingForKey(5);
                bool updateFailed = false;

                // Try updating that shard in the mapping without taking it offline first.
                try
                {
                    multiTenantShardMap.UpdateMapping(
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
                RangeMapping<int> newMappingFor10To20Offline = MarkMappingOfflineAndUpdateShard<int>(
                    multiTenantShardMap, mappingToUpdate, mappingFor5.Shard);

                // Verify that update succeeded.
                Assert.IsTrue(newMappingFor10To20Offline.Shard.Location.Equals(mappingFor5.Shard.Location));
                Assert.IsTrue(newMappingFor10To20Offline.Status == MappingStatus.Offline);

                // Bring the mapping back online.
                RangeMapping<int> newMappingFor10To20Online = multiTenantShardMap.UpdateMapping(
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
                RangeMapping<int> mappingToDelete = multiTenantShardMap.GetMappingForKey(5);
                bool operationFailed = false;

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
                RangeMappingUpdate ru = new RangeMappingUpdate();
                ru.Status = MappingStatus.Offline;

                mappingToDelete = multiTenantShardMap.UpdateMapping(mappingToDelete, ru);
                Trace.Assert(mappingToDelete.Status == MappingStatus.Offline);

                multiTenantShardMap.DeleteMapping(mappingToDelete);

                // Verify that delete succeeded.
                try
                {
                    RangeMapping<int> deletedMapping = multiTenantShardMap.GetMappingForKey(5);
                }
                catch (ShardManagementException smme)
                {
                    Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingNotFoundForKey);
                }

                #endregion DeleteMapping

                #region OpenConnection without Validation

                using (SqlConnection conn = multiTenantShardMap.OpenConnectionForKey(
                    20,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.None))
                {
                }

                #endregion OpenConnection without Validation

                #region OpenConnection with Validation

                // Use the stale state of "shardToUpdate" shard & see if validation works.
                bool validationFailed = false;
                try
                {
                    using (SqlConnection conn = multiTenantShardMap.OpenConnection(
                        mappingToDelete,
                        Globals.ShardUserConnectionString,
                        ConnectionOptions.Validate))
                    {
                    }
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
                ShardMapManager newShardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

                // Get the Range Shard Map
                RangeShardMap<int> newMultiTenantShardMap = newShardMapManager.GetRangeShardMap<int>(rangeShardMapName);

                using (SqlConnection conn = newMultiTenantShardMap.OpenConnectionForKey(
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
                    using (SqlConnection conn = newMultiTenantShardMap.OpenConnectionForKey(
                        20,
                        string.Empty,
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
                    RangeMapping<int> newMappingToDelete = newMultiTenantShardMap.CreateRangeMapping(
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
                        using (SqlConnection conn = newMultiTenantShardMap.OpenConnection(
                            newMappingToDelete,
                            Globals.ShardUserConnectionString,
                            ConnectionOptions.Validate))
                        {
                        }
                    }
                    catch (ShardManagementException smme)
                    {
                        validationFailed = true;
                        Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                    }

                    Assert.AreEqual(validationFailed, true);

                    #endregion

                    #region OpenConnectionAsync without Validation

                    using (SqlConnection conn = multiTenantShardMap.OpenConnectionForKeyAsync(
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
                        using (SqlConnection conn = multiTenantShardMap.OpenConnectionAsync(
                            mappingToDelete,
                            Globals.ShardUserConnectionString,
                            ConnectionOptions.Validate).Result)
                        {
                        }
                    }
                    catch (AggregateException ex)
                    {
                        ShardManagementException smme = ex.InnerException as ShardManagementException;
                        if (smme != null)
                        {
                            validationFailed = true;
                            Assert.AreEqual(smme.ErrorCode, ShardManagementErrorCode.MappingDoesNotExist);
                        }
                    }

                    Assert.AreEqual(validationFailed, true);

                    #endregion

                    // Cover the OpenConnectionForKeyAsync overloads
                    using (SqlConnection conn = multiTenantShardMap.OpenConnectionForKeyAsync(
                        20,
                        string.Empty,
                        Globals.ShardUserCredentialForSqlAuth(sqlAuthLogin.UniquifiedUserName),
                        ConnectionOptions.None).Result)
                    {
                    }

                    using (SqlConnection conn = multiTenantShardMap.OpenConnectionForKeyAsync(
                        20,
                        string.Empty,
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

                    using (SqlConnection conn = newMultiTenantShardMap.OpenConnectionForKeyAsync(
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
                        using (SqlConnection conn = newMultiTenantShardMap.OpenConnectionAsync(
                            newMappingToDelete,
                            Globals.ShardUserConnectionString,
                            ConnectionOptions.Validate).Result)
                        {
                        }
                    }
                    catch (AggregateException ex)
                    {
                        ShardManagementException smme = ex.InnerException as ShardManagementException;
                        if (smme != null)
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
                for (int i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++)
                {
                    RangeMapping<int> result = shardMapManager
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
                for (int i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++)
                {
                    RangeMapping<int> result = shardMapManager
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

                int splitPoint = 55;

                // Split [50, 60) into [50, 55) and [55, 60)
                RangeMapping<int> mappingToSplit = multiTenantShardMap.GetMappingForKey(splitPoint);

                IReadOnlyList<RangeMapping<int>> rangesAfterSplit = multiTenantShardMap.SplitMapping(mappingToSplit, splitPoint);

                rangesAfterSplit = rangesAfterSplit.OrderBy(nr => nr.Value.Low).ToArray();

                // We should get 2 ranges back.
                Assert.AreEqual(2, rangesAfterSplit.Count);

                Assert.AreEqual(rangesAfterSplit[0].Value.Low, new Range<int>(50, 55).Low);
                Assert.AreEqual(rangesAfterSplit[0].Value.High, new Range<int>(50, 55).High);
                Assert.AreEqual(rangesAfterSplit[1].Value.Low, new Range<int>(55, 60).Low);
                Assert.AreEqual(rangesAfterSplit[1].Value.High, new Range<int>(55, 60).High);

                // Split [50, 55) into [50, 52) and [52, 55)
                IReadOnlyList<RangeMapping<int>> newRangesAfterAdd = multiTenantShardMap.SplitMapping(rangesAfterSplit[0], 52);

                newRangesAfterAdd = newRangesAfterAdd.OrderBy(nr => nr.Value.Low).ToArray();

                // We should get 2 ranges back.
                Assert.AreEqual(2, newRangesAfterAdd.Count);

                Assert.AreEqual(newRangesAfterAdd[0].Value.Low, new Range<int>(50, 52).Low);
                Assert.AreEqual(newRangesAfterAdd[0].Value.High, new Range<int>(50, 52).High);
                Assert.AreEqual(newRangesAfterAdd[1].Value.Low, new Range<int>(52, 55).Low);
                Assert.AreEqual(newRangesAfterAdd[1].Value.High, new Range<int>(52, 55).High);

                // Move [50, 52) to MultiTenantDB1

                Shard targetShard = multiTenantShardMap.GetShard(new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ScenarioTests.s_multiTenantDBs[0]));

                // Mark mapping offline, update shard location.
                RangeMapping<int> movedMapping1 = MarkMappingOfflineAndUpdateShard<int>(
                    multiTenantShardMap, newRangesAfterAdd[0], targetShard);

                // Bring the mapping back online.
                movedMapping1 = multiTenantShardMap.UpdateMapping(
                    movedMapping1,
                    new RangeMappingUpdate
                    {
                        Status = MappingStatus.Online,
                    });


                // Mark mapping offline, update shard location.
                RangeMapping<int> movedMapping2 = MarkMappingOfflineAndUpdateShard<int>(
                    multiTenantShardMap, newRangesAfterAdd[1], targetShard);

                // Bring the mapping back online.
                movedMapping2 = multiTenantShardMap.UpdateMapping(
                    movedMapping2,
                    new RangeMappingUpdate
                    {
                        Status = MappingStatus.Online,
                    });

                // Obtain the final moved mapping.
                RangeMapping<int> finalMovedMapping = multiTenantShardMap.MergeMappings(movedMapping1, movedMapping2);

                Assert.AreEqual(finalMovedMapping.Value.Low, new Range<int>(50, 55).Low);
                Assert.AreEqual(finalMovedMapping.Value.High, new Range<int>(50, 55).High);

                #endregion Split/Merge
            }
            catch (ShardManagementException smme)
            {
                success = false;

                Trace.WriteLine(String.Format("Error Category: {0}", smme.ErrorCategory));
                Trace.WriteLine(String.Format("Error Code    : {0}", smme.ErrorCode));
                Trace.WriteLine(String.Format("Error Message : {0}", smme.Message));

                if (smme.InnerException != null)
                {
                    Trace.WriteLine(String.Format("Storage Error Message : {0}", smme.InnerException.Message));

                    if (smme.InnerException.InnerException != null)
                    {
                        Trace.WriteLine(String.Format("SqlClient Error Message : {0}", smme.InnerException.InnerException.Message));
                    }
                }
            }

            Assert.IsTrue(success);
        }

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void ListShardMapPerformanceCounterValidation()
        {
#if NETFRAMEWORK
            if (PerfCounterInstance.HasCreatePerformanceCategoryPermissions())
            {
                string shardMapName = "PerTenantShardMap";

                #region Setup

                // Deploy shard map manager.
                ShardMapManager shardMapManager = ShardMapManagerFactory.CreateSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerCreateMode.ReplaceExisting);

                // Create a single user per-tenant shard map.
                ListShardMap<int> perTenantShardMap = shardMapManager.CreateListShardMap<int>(shardMapName);

                ShardLocation sl1 = new ShardLocation(
                    Globals.ShardMapManagerTestsDatasourceName,
                    ScenarioTests.s_perTenantDBs[0]);

                // Create first shard and add 1 point mapping.
                Shard s = perTenantShardMap.CreateShard(sl1);

                // Create the mapping.
                PointMapping<int> p1 = perTenantShardMap.CreatePointMapping(1, s);

                #endregion Setup

                // Delete and recreate perf counter catagory.
                ShardMapManagerFactory.CreatePerformanceCategoryAndCounters();

                // Eager loading of shard map manager
                ShardMapManager smm =
                    ShardMapManagerFactory.GetSqlShardMapManager(Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Eager);

                // check if perf counter instance exists, instance name logic is from PerfCounterInstance.cs
                string instanceName = string.Concat(Process.GetCurrentProcess().Id.ToString(), "-", shardMapName);

                Assert.IsTrue(ValidateInstanceExists(instanceName));

                // verify # of mappings.
                Assert.IsTrue(ValidateCounterValue(instanceName, PerformanceCounterName.MappingsCount, 1));

                ListShardMap<int> lsm = smm.GetListShardMap<int>(shardMapName);

                // Add a new shard and mapping and verify updated counters
                ShardLocation sl2 = new ShardLocation(
                    Globals.ShardMapManagerTestsDatasourceName,
                    ScenarioTests.s_perTenantDBs[1]);

                Shard s2 = lsm.CreateShard(sl2);

                PointMapping<int> p2 = lsm.CreatePointMapping(2, s2);
                Assert.IsTrue(ValidateCounterValue(instanceName, PerformanceCounterName.MappingsCount, 2));

                // Create few more mappings and validate MappingsAddOrUpdatePerSec counter
                s2 = lsm.GetShard(sl2);
                for (int i = 3; i < 11; i++)
                {
                    lsm.CreatePointMapping(i, s2);
                    s2 = lsm.GetShard(sl2);
                }

                Assert.IsTrue(ValidateNonZeroCounterValue(instanceName,
                    PerformanceCounterName.MappingsAddOrUpdatePerSec));

                // try to lookup non-existing mapping and verify MappingsLookupFailedPerSec
                for (int i = 0; i < 10; i++)
                {
                    ShardManagementException exception = AssertExtensions.AssertThrows<ShardManagementException>(
                        () => lsm.OpenConnectionForKey(20, Globals.ShardUserConnectionString));
                }

                Assert.IsTrue(ValidateNonZeroCounterValue(instanceName,
                    PerformanceCounterName.MappingsLookupFailedPerSec));

                // perform DDR operation few times and validate non-zero counter values
                for (int i = 0; i < 10; i++)
                {
                    using (SqlConnection conn = lsm.OpenConnectionForKey(1, Globals.ShardUserConnectionString))
                    {
                    }
                }

                Assert.IsTrue(ValidateNonZeroCounterValue(instanceName, PerformanceCounterName.DdrOperationsPerSec));
                Assert.IsTrue(ValidateNonZeroCounterValue(instanceName,
                    PerformanceCounterName.MappingsLookupSucceededPerSec));

                // Remove shard map after removing mappings and shard
                for (int i = 1; i < 11; i++)
                {
                    lsm.DeleteMapping(lsm.MarkMappingOffline(lsm.GetMappingForKey(i)));
                }

                Assert.IsTrue(ValidateNonZeroCounterValue(instanceName,
                    PerformanceCounterName.MappingsRemovePerSec));

                lsm.DeleteShard(lsm.GetShard(sl1));
                lsm.DeleteShard(lsm.GetShard(sl2));

                Assert.IsTrue(ValidateCounterValue(instanceName, PerformanceCounterName.MappingsCount, 0));

                smm.DeleteShardMap(lsm);

                // make sure that perf counter instance is removed
                Assert.IsFalse(ValidateInstanceExists(instanceName));
            }
            else
            {
                Assert.Inconclusive("Do not have permissions to create performance counter category, test skipped");
            }
#endif
        }

#if NETFRAMEWORK
        private bool ValidateNonZeroCounterValue(string instanceName, PerformanceCounterName counterName)
        {
            string counterdisplayName = (from c in PerfCounterInstance.counterList
                                         where c.CounterName == counterName
                                         select c.CounterDisplayName).First();

            using (PerformanceCounter pc =
                new PerformanceCounter(PerformanceCounters.ShardManagementPerformanceCounterCategory, counterdisplayName, instanceName))
            {
                return pc.RawValue != 0;
            }
        }

        private bool ValidateCounterValue(string instanceName, PerformanceCounterName counterName, long value)
        {
            string counterdisplayName = (from c in PerfCounterInstance.counterList
                                         where c.CounterName == counterName
                                         select c.CounterDisplayName).First();

            using (PerformanceCounter pc =
                new PerformanceCounter(PerformanceCounters.ShardManagementPerformanceCounterCategory, counterdisplayName, instanceName))
            {
                return pc.RawValue.Equals(value);
            }
        }

        private bool ValidateInstanceExists(string instanceName)
        {
            return PerformanceCounterCategory.InstanceExists(instanceName, PerformanceCounters.ShardManagementPerformanceCounterCategory);
        }
#endif

        private RangeMapping<T> MarkMappingOfflineAndUpdateShard<T>(RangeShardMap<T> map, RangeMapping<T> mapping, Shard newShard)
        {
            RangeMapping<T> mappingOffline = map.UpdateMapping(
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
            PointMapping<T> mappingOffline = map.UpdateMapping(
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
}
