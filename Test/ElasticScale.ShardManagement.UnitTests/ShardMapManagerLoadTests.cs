// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    [TestClass]
    public class ShardMapManagerLoadTests
    {
        /// <summary>
        /// Sharded databases to create for the test.
        /// </summary>
        private static string[] s_shardedDBs = new[]
        {
            "shard1" + Globals.TestDatabasePostfix,
            "shard2" + Globals.TestDatabasePostfix,
            "shard3" + Globals.TestDatabasePostfix,
            "shard4" + Globals.TestDatabasePostfix,
            "shard5" + Globals.TestDatabasePostfix,
            "shard6" + Globals.TestDatabasePostfix,
            "shard7" + Globals.TestDatabasePostfix,
            "shard8" + Globals.TestDatabasePostfix,
            "shard9" + Globals.TestDatabasePostfix,
            "shard10" + Globals.TestDatabasePostfix,
        };

        /// <summary>
        /// List shard map name.
        /// </summary>
        private static string s_listShardMapName = "Customers_list";

        /// <summary>
        /// Range shard map name.
        /// </summary>
        private static string s_rangeShardMapName = "Customers_range";

        /// <summary>
        /// Query to kill connections for a particular database
        /// </summary>
        internal const string KillConnectionsForDatabaseQuery =
            @"declare @stmt varchar(max)
                set @stmt = ''
                select @stmt = @stmt + 'Kill ' + Convert(varchar, spid) + ';'
                from master..sysprocesses
                where spid > 50 and dbid = DB_ID('{0}')
                exec(@stmt)";

        /// <summary>
        /// Queries to cleanup objects used for deadlock detection.
        /// These will not work against Azure SQL DB, code just catches and ignores SqlException for these queries.
        /// </summary>
        private static string[] s_deadlockDetectionCleanupQueries = new[]
        {
            "use msdb",
            "drop event notification CaptureDeadlocks on server",
            "drop service DeadlockService",
            "drop queue DeadlockQueue",
            "use master",
        };

        /// <summary>
        /// Queries to create objects for deadlock detection.
        /// These will not work against Azure SQL DB, code just catches and ignores SqlException for these queries.
        /// </summary>
        private static string[] s_deadlockDetectionSetupQueries = new[]
        {
            "use msdb",
            "create queue DeadlockQueue",
            "create service DeadlockService on queue DeadlockQueue ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification])",
            "create event notification CaptureDeadlocks on server with FAN_IN for DEADLOCK_GRAPH to service 'DeadlockService', 'current database'",
            "use master",
        };

        /// <summary>
        /// Query to collect deadlock graphs
        /// This will not work against Azure SQL DB, code just catches and ignores SqlException.
        /// </summary>
        private static string s_deadlockDetectionQuery = "select CAST(message_body AS XML) from msdb..DeadlockQueue";

        /// <summary>
        /// Number of shards added to both list and range shard maps.
        /// </summary>
        private const int InitialShardCount = 6;

        /// <summary>
        /// Lowest point on Integer range that can be mapped by unit tests.
        /// </summary>
        private const int MinMappingPoint = -2000;

        /// <summary>
        /// Highest point on Integer range that can be mapped by unit tests.
        /// </summary>
        private const int MaxMappingPoint = 2000;

        /// <summary>
        /// Maximum size of a single range mapping.
        /// </summary>
        private const int MaxRangeMappingSize = 10;

        /// <summary>
        /// Random number generator used to generate keys in unit test.
        /// </summary>
        private Random _r = new Random();

        /// <summary>
        /// Retry policy used for DDR in unit tests.
        /// </summary>
        private static TransientFaultHandling.RetryPolicy<TransientFaultHandling.SqlDatabaseTransientErrorDetectionStrategy> s_retryPolicy;

        #region CommonMethods

        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void ShardMapManagerLoadTestsInitialize(TestContext testContext)
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
                for (int i = 0; i < ShardMapManagerLoadTests.s_shardedDBs.Length; i++)
                {
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ShardMapManagerLoadTests.s_shardedDBs[i]),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                // cleanup for deadlock monitoring
                foreach (string q in s_deadlockDetectionCleanupQueries)
                {
                    using (SqlCommand cmd = new SqlCommand(q, conn))
                    {
                        try
                        {
                            cmd.ExecuteNonQuery();
                        }
                        catch (SqlException)
                        {
                        }
                    }
                }

                // setup for deadlock monitoring
                foreach (string q in s_deadlockDetectionSetupQueries)
                {
                    using (SqlCommand cmd = new SqlCommand(q, conn))
                    {
                        try
                        {
                            cmd.ExecuteNonQuery();
                        }
                        catch (SqlException)
                        {
                        }
                    }
                }
            }

            // Create shard map manager.
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            // Create list shard map.
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Lazy);

            ListShardMap<int> lsm = smm.CreateListShardMap<int>(ShardMapManagerLoadTests.s_listShardMapName);
            Assert.IsNotNull(lsm);

            // Create range shard map.
            RangeShardMap<int> rsm = smm.CreateRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
            Assert.IsNotNull(rsm);

            // Add 'InitialShardCount' shards to list and range shard map.

            for (int i = 0; i < ShardMapManagerLoadTests.InitialShardCount; i++)
            {
                ShardCreationInfo si = new ShardCreationInfo(
                    new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapManagerLoadTests.s_shardedDBs[i]),
                    ShardStatus.Online);

                Shard sList = lsm.CreateShard(si);
                Assert.IsNotNull(sList);

                Shard sRange = rsm.CreateShard(si);
                Assert.IsNotNull(sRange);
            }

            // Initialize retry policy
            s_retryPolicy = new TransientFaultHandling.RetryPolicy<TransientFaultHandling.SqlDatabaseTransientErrorDetectionStrategy>(
                new TransientFaultHandling.ExponentialBackoff(5, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(100)));
        }

        /// <summary>
        /// Cleans up common state for the all tests in this class.
        /// </summary>
        [ClassCleanup()]
        public static void ShardMapManagerLoadTestsCleanup()
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            // Detect inconsistencies for all shard locations in a shard map.
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RecoveryManager rm = new RecoveryManager(smm);

            bool inconsistencyDetected = false;

            foreach (ShardLocation sl in smm.GetDistinctShardLocations())
            {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

                foreach (RecoveryToken g in gs)
                {
                    var kvps = rm.GetMappingDifferences(g);
                    if (kvps.Keys.Count > 0)
                    {
                        inconsistencyDetected = true;
                        Debug.WriteLine("LSM at location {0} is not consistent with GSM", sl);
                    }
                }
            }

            bool deadlocksDetected = false;

            // Check for deadlocks during the run and cleanup database and deadlock objects on successful run
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                // check for any deadlocks occured during the run and cleanup deadlock monitoring objects
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = s_deadlockDetectionQuery;
                    cmd.CommandType = System.Data.CommandType.Text;

                    try
                    {
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                // some deadlocks occured during the test, collect xml plan for these deadlocks
                                deadlocksDetected = true;

                                while (reader.Read())
                                {
                                    Debug.WriteLine("Deadlock information");
                                    Debug.WriteLine(reader.GetSqlXml(0).Value);
                                }
                            }
                        }
                    }
                    catch (SqlException)
                    {
                    }
                }

                // cleanup only if there are no inconsistencies and deadlocks during the run.
                if (!deadlocksDetected && !inconsistencyDetected)
                {
                    foreach (string q in s_deadlockDetectionCleanupQueries)
                    {
                        using (SqlCommand cmd = new SqlCommand(q, conn))
                        {
                            try
                            {
                                cmd.ExecuteNonQuery();
                            }
                            catch (SqlException)
                            {
                            }
                        }
                    }

                    // Drop shard databases
                    for (int i = 0; i < ShardMapManagerLoadTests.s_shardedDBs.Length; i++)
                    {
                        using (SqlCommand cmd = new SqlCommand(
                            string.Format(Globals.DropDatabaseQuery, ShardMapManagerLoadTests.s_shardedDBs[i]),
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
        }

        /// <summary>
        /// Helper function to do validation,this will be called using TFH retry policy.
        /// </summary>
        /// <param name="sm">Shard map for a mapping to validate</param>
        /// <param name="key">Key to lookup and validate</param>
        private static void ValidateImpl(ShardMap sm, int key)
        {
            try
            {
                using (SqlConnection conn = sm.OpenConnectionForKey(key,
                    Globals.ShardUserConnectionString,
                    ConnectionOptions.Validate))
                {
                }
            }
            catch (SqlException e)
            {
                // Error Number 3980: The request failed to run because the batch is aborted, this can be caused by abort signal sent from client, or another request is running in the same session, which makes the session busy.
                // Error Number = 0, Class = 20, Message = The connection is broken and recovery is not possible.  The connection is marked by the server as unrecoverable.  No attempt was made to restore the connection.
                // Error Number = 0, Class = 11, Message = A severe error occurred on the current command.  The results, if any, should be discarded.
                switch (e.Number)
                {
                    case 0:
                        if (e.Class != 20 && e.Class != 11)
                            throw;
                        break;

                    case 3980:
                        break;

                    default:
                        throw;
                }
            }
        }

        /// <summary>
        /// Get existing point mapping from a list shard map
        /// </summary>
        /// <param name="lsm">List shard map</param>
        /// <returns>Valid existing point mapping, null if no point mappings found in given shard map</returns>
        private PointMapping<int> GetRandomPointMapping(ListShardMap<int> lsm)
        {
            // Get all point mappings without storing the results in the cache so that OpenConnection will fetch mapping again.
            IReadOnlyList<PointMapping<int>> allMappings = lsm.GetMappings();

            if (allMappings.Count == 0)
                return null;

            int index = _r.Next(allMappings.Count);
            return allMappings[index];
        }

        /// <summary>
        /// Get existing range mapping from a range shard map
        /// </summary>
        /// <param name="lsm">Range shard map</param>
        /// <param name="minimumRangeSize">Minimum size of range mapping, this can be used for AddRangeWithinRange and RemoveRangeFromRange tests</param>
        /// <returns>Valid existing range mapping, null if no range mappings found in given shard map</returns>
        private RangeMapping<int> GetRandomRangeMapping(RangeShardMap<int> rsm, int minimumRangeSize = 1)
        {
            IReadOnlyList<RangeMapping<int>> allMappings = rsm.GetMappings();

            List<RangeMapping<int>> filteredList = new List<RangeMapping<int>>(
                from m in allMappings
                where ((int)m.Range.High.Value - (int)m.Range.Low.Value) >= minimumRangeSize
                select m);

            if (filteredList.Count == 0)
                return null;

            return filteredList[_r.Next(filteredList.Count)];
        }

        /// <summary>
        /// Helper function to select a random shard for specified shard map.
        /// </summary>
        /// <param name="sm">Shard map to get shard.</param>
        /// <returns>Shard from specified shard map.</returns>
        private Shard GetRandomOnlineShardFromShardMap(ShardMap sm)
        {
            List<Shard> shardList = sm.GetShards().Where(s => s.Status == ShardStatus.Online).ToList();

            if (shardList.Count > 0)
                return shardList[_r.Next(shardList.Count)];
            else
                return null;
        }

        /// <summary>
        /// Helper function to add a new shard to given shard map.
        /// </summary>
        /// <param name="sm"></param>
        private void AddShardToShardMap(ShardMap sm)
        {
            IEnumerable<Shard> existingShards = sm.GetShards();

            // get list of shard locations that are not already added to this shard map.
            List<string> availableLocationList = (from dbName in ShardMapManagerLoadTests.s_shardedDBs
                                                  where !existingShards.Select(s => s.Location.Database).ToList().Contains(dbName)
                                                  select dbName).ToList();

            if (availableLocationList.Count > 0)
            {
                ShardLocation sl = new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, availableLocationList[_r.Next(availableLocationList.Count)]);

                Debug.WriteLine("Trying to add shard at location {0} to shard map {1}", sl, sm);

                ShardCreationInfo si = new ShardCreationInfo(sl, ShardStatus.Online);

                Shard newShard = sm.CreateShard(si);
            }
        }

        #endregion CommonMethods

        #region ListShardMapTests

        /// <summary>
        /// Add point mapping
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestAddPointMapping()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

                ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapManagerLoadTests.s_listShardMapName);

                Assert.IsNotNull(lsm);
                do
                {
                    // Chose a random shard to add mapping.
                    Shard s = GetRandomOnlineShardFromShardMap((ShardMap)lsm);
                    if (s == null)
                        continue;

                    // Create a random integer key for a new mapping and verify that its not already present in this shard map.
                    int key = _r.Next(MinMappingPoint, MaxMappingPoint);
                    PointMapping<int> pExisting = null;

                    // choose different mapping if this one already exists.
                    if (lsm.TryGetMappingForKey(key, out pExisting))
                        continue;

                    Debug.WriteLine("Trying to add point mapping for key {0} to shard location {1}", key, s.Location);

                    PointMapping<int> p1 = lsm.CreatePointMapping(key, s);

                    Assert.IsNotNull(p1);

                    // Validate mapping by trying to connect
                    s_retryPolicy.ExecuteAction(
                        () => ValidateImpl(
                            (ShardMap)lsm,
                            key));
                }
                while (false);
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Delete point mapping
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestDeletePointMapping()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapManagerLoadTests.s_listShardMapName);
                Assert.IsNotNull(lsm);

                PointMapping<int> p1 = this.GetRandomPointMapping(lsm);

                if (p1 != null)
                {
                    Debug.WriteLine("Trying to delete point mapping for key {0}", p1.Key);

                    PointMappingUpdate pu = new PointMappingUpdate();
                    pu.Status = MappingStatus.Offline;

                    PointMapping<int> mappingToDelete = lsm.UpdateMapping(p1, pu);

                    lsm.DeleteMapping(mappingToDelete);
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// DDR for a list shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestPointMappingDDR()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy);

                ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapManagerLoadTests.s_listShardMapName);
                Assert.IsNotNull(lsm);

                PointMapping<int> p1 = this.GetRandomPointMapping(lsm);

                if (p1 != null)
                {
                    Debug.WriteLine("Trying to validate point mapping for key {0}", p1.Key);

                    // Validate mapping by trying to connect
                    s_retryPolicy.ExecuteAction(
                        () => ValidateImpl(
                            (ShardMap)lsm,
                            (int)(p1.Key.Value)));
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Add a shard to list shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestAddShardToListShardMap()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapManagerLoadTests.s_listShardMapName);
                Assert.IsNotNull(lsm);

                AddShardToShardMap((ShardMap)lsm);
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Mark all shards as online in list shard map.
        /// <remarks>If remove shard operation fails, it will leave shards in offline state, this function will mark all such shards as online.</remarks>
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestMarkAllShardsAsOnlineInListShardMap()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapManagerLoadTests.s_listShardMapName);
                Assert.IsNotNull(lsm);

                foreach (Shard s in lsm.GetShards())
                {
                    if (s.Status == ShardStatus.Offline)
                    {
                        lsm.UpdateShard(s,
                                        new ShardUpdate
                                        {
                                            Status = ShardStatus.Online
                                        });
                    }
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Remove a random shard from list shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestRemoveShardFromListShardMap()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                ListShardMap<int> lsm = smm.GetListShardMap<int>(ShardMapManagerLoadTests.s_listShardMapName);
                Assert.IsNotNull(lsm);

                List<Shard> existingShards = lsm.GetShards().ToList();

                if (existingShards.Count == 0)
                    return;

                // If there is already a shard marked as offline, chose that one to delete.
                // This can happend if earlier remove operation was terminated for some reason - ex. killing connections.
                Shard offlineShard = existingShards.Find(e => e.Status == ShardStatus.Offline);

                if (offlineShard == null)
                {
                    offlineShard = existingShards[_r.Next(existingShards.Count)];

                    // First mark shard as offline so that other test threads will not add new mappings to it.
                    offlineShard = lsm.UpdateShard(offlineShard,
                                                    new ShardUpdate
                                                    {
                                                        Status = ShardStatus.Offline
                                                    });
                }

                Debug.WriteLine("Trying to remove shard at location {0}", offlineShard.Location);

                PointMappingUpdate pu = new PointMappingUpdate();
                pu.Status = MappingStatus.Offline;

                // Remove all mappings from this shard for given shard map.
                foreach (PointMapping<int> p in lsm.GetMappings(offlineShard))
                {
                    PointMapping<int> mappingToDelete = lsm.UpdateMapping(p, pu);
                    lsm.DeleteMapping(mappingToDelete);
                }

                // Shard object is changed as mappings are removed, get it again.
                Shard deleteShard = lsm.GetShard(offlineShard.Location);

                // now remove shard.
                lsm.DeleteShard(deleteShard);

                Debug.WriteLine("Removed shard at location {0} from shard map {1}", deleteShard.Location, lsm);
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        #endregion ListShardMapTests

        #region RangeShardMapTests

        /// <summary>
        /// Add range mapping
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestAddRangeMapping()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);

                Assert.IsNotNull(rsm);
                do
                {
                    // Chose a random shard to add mapping.
                    Shard s = GetRandomOnlineShardFromShardMap((ShardMap)rsm);
                    if (s == null)
                        continue;

                    // generate a random range to add a new range mapping and verify that its not already mapped.
                    int minKey = _r.Next(MinMappingPoint, MaxMappingPoint);
                    int maxKey = minKey + _r.Next(1, MaxRangeMappingSize);
                    maxKey = (maxKey <= MaxMappingPoint) ? maxKey : MaxMappingPoint;

                    IReadOnlyList<RangeMapping<int>> existingMapping = rsm.GetMappings(new Range<int>(minKey, maxKey));
                    if (existingMapping.Count > 0)
                        continue;

                    Debug.WriteLine("Trying to add range mapping for key range ({0} - {1}) to shard location {2}", minKey, maxKey, s.Location);

                    RangeMapping<int> r1 = rsm.CreateRangeMapping(new Range<int>(minKey, maxKey), s);

                    Assert.IsNotNull(r1);

                    // Validate mapping by trying to connect
                    s_retryPolicy.ExecuteAction(
                        () => ValidateImpl(
                            (ShardMap)rsm,
                            minKey));
                }
                while (false);
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Delete range mapping
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestDeleteRangeMapping()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                                Globals.ShardMapManagerConnectionString,
                                ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                RangeMapping<int> r1 = this.GetRandomRangeMapping(rsm);

                if (r1 != null)
                {
                    Debug.WriteLine("Trying to delete mapping for range with low value = {0}", r1.Range.Low);

                    RangeMappingUpdate ru = new RangeMappingUpdate();
                    ru.Status = MappingStatus.Offline;

                    RangeMapping<int> mappingToDelete = rsm.UpdateMapping(r1, ru);

                    rsm.DeleteMapping(mappingToDelete);
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// DDR for a range shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestRangeMappingDDR()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                RangeMapping<int> r1 = this.GetRandomRangeMapping(rsm);

                if (r1 != null)
                {
                    int keyToValidate = _r.Next((int)(r1.Range.Low.Value), (int)(r1.Range.High.Value));

                    Debug.WriteLine("Trying to validate mapping for key {0}", keyToValidate);

                    // Validate mapping by trying to connect
                    s_retryPolicy.ExecuteAction(
                        () => ValidateImpl(
                            (ShardMap)rsm,
                            keyToValidate));
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Split range with locking
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestSplitRangeWithLock()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                RangeMapping<int> r1 = this.GetRandomRangeMapping(rsm, 2);

                if (r1 != null)
                {
                    int splitPoint = _r.Next((int)(r1.Range.Low.Value)+1, (int)(r1.Range.High.Value)-1);

                    Debug.WriteLine("Trying to split range mapping for key range ({0} - {1}) at {2}", r1.Range.Low.Value, r1.Range.High.Value, splitPoint);

                    // Lock the mapping
                    MappingLockToken mappingLockToken = MappingLockToken.Create();
                    rsm.LockMapping(r1, mappingLockToken);

                    IReadOnlyList<RangeMapping<int>> rList = rsm.SplitMapping(r1, splitPoint, mappingLockToken);

                    Assert.AreEqual(2, rList.Count);

                    foreach (RangeMapping<int> r2 in rList)
                    {
                        Assert.IsNotNull(r2);
                        Assert.AreEqual(mappingLockToken, rsm.GetMappingLockOwner(r2),
                            String.Format("LockOwnerId of mapping: {0} does not match id in store!", r2));

                        // Unlock each mapping and verify
                        rsm.UnlockMapping(r2, mappingLockToken);
                        Assert.AreEqual(MappingLockToken.NoLock, rsm.GetMappingLockOwner(r2),
                            String.Format("Mapping: {0} not unlocked as expected!", r2));
                    }
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Split range without locking
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestSplitRangeNoLock()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                RangeMapping<int> r1 = this.GetRandomRangeMapping(rsm, 2);

                if (r1 != null)
                {
                    int splitPoint = _r.Next((int)(r1.Range.Low.Value)+1, (int)(r1.Range.High.Value)-1);

                    Debug.WriteLine("Trying to split range mapping for key range ({0} - {1}) at {2}", r1.Range.Low.Value, r1.Range.High.Value, splitPoint);

                    IReadOnlyList<RangeMapping<int>> rList = rsm.SplitMapping(r1, splitPoint);
                    Assert.AreEqual(2, rList.Count);
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Merge ranges with locking
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestMergeRangesWithLock()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
            Globals.ShardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                IEnumerable<RangeMapping<int>> existingMappings = rsm.GetMappings(new Range<int>(MinMappingPoint, MaxMappingPoint));

                IQueryable<RangeMapping<int>> qr = Queryable.AsQueryable(existingMappings);

                // Find pair of adjacent mappings.
                var test = from a in qr
                           join b in qr on
                            new { a.Range.High.Value, a.StoreMapping.StoreShard.Id, a.StoreMapping.Status } equals
                            new { b.Range.Low.Value, b.StoreMapping.StoreShard.Id, b.StoreMapping.Status }
                           select new { a, b };

                if (test.Count() > 0)
                {
                    var t = test.First();

                    Debug.WriteLine("Trying to merge range mapping for key range ({0} - {1}) and ({2} - {3})", t.a.Range.Low.Value, t.a.Range.High.Value, t.b.Range.Low.Value, t.b.Range.High.Value);


                    MappingLockToken mappingLockTokenLeft = MappingLockToken.Create();
                    rsm.LockMapping(t.a, mappingLockTokenLeft);

                    MappingLockToken mappingLockTokenRight = MappingLockToken.Create();
                    rsm.LockMapping(t.b, mappingLockTokenLeft);

                    RangeMapping<int> rMerged = rsm.MergeMappings(t.a, t.b, mappingLockTokenLeft, mappingLockTokenRight);

                    Assert.IsNotNull(rMerged);

                    MappingLockToken storeMappingLockToken = rsm.GetMappingLockOwner(rMerged);
                    Assert.AreEqual(storeMappingLockToken, mappingLockTokenLeft, "Expected merged mapping lock id to equal left mapping id!");
                    rsm.UnlockMapping(rMerged, storeMappingLockToken);

                    storeMappingLockToken = rsm.GetMappingLockOwner(rMerged);
                    Assert.AreEqual(storeMappingLockToken, MappingLockToken.NoLock, "Expected merged mapping lock id to equal default mapping id after unlock!");
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Merge ranges without locking
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestMergeRangesNoLock()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                IEnumerable<RangeMapping<int>> existingMappings = rsm.GetMappings(new Range<int>(MinMappingPoint, MaxMappingPoint));

                IQueryable<RangeMapping<int>> qr = Queryable.AsQueryable(existingMappings);

                // find pair of adjacent mappings.
                var test = from a in qr
                           join b in qr on
                            new { a.Range.High.Value, a.StoreMapping.StoreShard.Id, a.StoreMapping.Status } equals
                            new { b.Range.Low.Value, b.StoreMapping.StoreShard.Id, b.StoreMapping.Status }
                           select new { a, b };

                if (test.Count() > 0)
                {
                    var t = test.First();

                    Debug.WriteLine("Trying to merge range mapping for key range ({0} - {1}) and ({2} - {3})", t.a.Range.Low.Value, t.a.Range.High.Value, t.b.Range.Low.Value, t.b.Range.High.Value);

                    RangeMapping<int> rMerged = rsm.MergeMappings(t.a, t.b);
                    Assert.IsNotNull(rMerged);
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Add a shard to range shard map
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestAddShardToRangeShardMap()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                AddShardToShardMap((ShardMap)rsm);
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Mark all shards as online in range shard map.
        /// <remarks>If remove shard operation fails, it will leave shards in offline state, this function will mark all such shards as online.</remarks>
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestMarkAllShardsAsOnlineInRangeShardMap()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                foreach (Shard s in rsm.GetShards())
                {
                    if (s.Status == ShardStatus.Offline)
                    {
                        rsm.UpdateShard(s,
                                        new ShardUpdate
                                        {
                                            Status = ShardStatus.Online
                                        });
                    }
                }
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }

        /// <summary>
        /// Remove a random shard from range shard map.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestRemoveShardFromRangeShardMap()
        {
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);

                RangeShardMap<int> rsm = smm.GetRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
                Assert.IsNotNull(rsm);

                List<Shard> existingShards = rsm.GetShards().ToList();

                if (existingShards.Count == 0)
                    return;

                // If there is already a shard marked as offline, chose that one to delete.
                // This can happend if earlier remove operation was terminated for some reason - ex. killing connections.
                Shard offlineShard = existingShards.Find(e => e.Status == ShardStatus.Offline);

                if (offlineShard == null)
                {
                    offlineShard = existingShards[_r.Next(existingShards.Count)];

                    // First mark shard as offline so that other test threads will not add new mappings to it.
                    offlineShard = rsm.UpdateShard(offlineShard,
                                                    new ShardUpdate
                                                    {
                                                        Status = ShardStatus.Offline
                                                    });
                }

                Debug.WriteLine("Trying to remove shard at location {0}", offlineShard.Location);

                RangeMappingUpdate ru = new RangeMappingUpdate();
                ru.Status = MappingStatus.Offline;

                // Remove all mappings from this shard for given shard map.
                foreach (RangeMapping<int> rm in rsm.GetMappings(offlineShard))
                {
                    RangeMapping<int> mappingToDelete = rsm.UpdateMapping(rm, ru);
                    rsm.DeleteMapping(mappingToDelete);
                }

                // get shard object again.
                Shard deleteShard = rsm.GetShard(offlineShard.Location);

                // now remove shard.
                rsm.DeleteShard(deleteShard);

                Debug.WriteLine("Removed shard at location {0} from shard map {1}", deleteShard.Location, rsm);
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }
        }


        #endregion RangeShardMapTests

        /// <summary>
        /// Kill all connections for a random shard
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestKillLSMConnections()
        {
            String databaseName = null;
            try
            {
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    Globals.ShardMapManagerConnectionString,
                    ShardMapManagerLoadPolicy.Lazy);
                do
                {
                    List<ShardLocation> sl = smm.GetDistinctShardLocations().ToList();
                    if (sl.Count == 0)
                        continue;

                    // Select a random database(shard) to kill connections
                    databaseName = sl[_r.Next(sl.Count)].Database;
                }
                while (false);
            }
            catch (ShardManagementException sme)
            {
                Debug.WriteLine("Exception caught: {0}", sme.Message);
            }

            if (databaseName != null)
            {
                using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
                {
                    conn.Open();

                    // kill all connections for given shard location
                    try
                    {
                        using (SqlCommand cmd = new SqlCommand(
                            string.Format(KillConnectionsForDatabaseQuery, databaseName),
                            conn))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                    catch (SqlException e)
                    {
                        //  233: A transport-level error has occurred when receiving results from the server. (provider: Shared Memory Provider, error: 0 - No process is on the other end of the pipe.)
                        // 6106: Process ID %d is not an active process ID.
                        // 6107: Only user processes can be killed
                        if ((e.Number != 233) && (e.Number != 6106) && (e.Number != 6107))
                        {
                            Assert.Fail("error number {0} with message {1}", e.Number, e.Message);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Kill all connections to Shard Map Manager database
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void LoadTestKillGSMConnections()
        {
            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                try
                {
                    // kill all connections for ShardMapManager database
                    using (SqlCommand cmd = new SqlCommand(
                        string.Format(KillConnectionsForDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                        conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (SqlException e)
                {
                    //  233: A transport-level error has occurred when receiving results from the server. (provider: Shared Memory Provider, error: 0 - No process is on the other end of the pipe.)
                    // 6106: Process ID %d is not an active process ID.
                    // 6107: Only user processes can be killed
                    if ((e.Number != 233) && (e.Number != 6106) && (e.Number != 6107))
                    {
                        Assert.Fail("error number {0} with message {1}", e.Number, e.Message);
                    }
                }
            }
        }
    }
}
