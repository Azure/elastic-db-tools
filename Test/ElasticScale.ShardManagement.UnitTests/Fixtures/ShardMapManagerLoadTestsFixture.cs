using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using Xunit;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Fixtures {
    public class ShardMapManagerLoadTestsFixture : IDisposable {

        public ShardMapManagerLoadTestsFixture() {

            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using(SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString)) {
                conn.Open();

                // Create ShardMapManager database
                using(SqlCommand cmd = new SqlCommand(
                    string.Format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                    conn)) {
                    cmd.ExecuteNonQuery();
                }

                // Create shard databases
                for(int i = 0; i < ShardMapManagerLoadTests.s_shardedDBs.Length; i++) {
                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ShardMapManagerLoadTests.s_shardedDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }
                }

                // cleanup for deadlock monitoring
                foreach(string q in ShardMapManagerLoadTests.s_deadlockDetectionCleanupQueries) {
                    using(SqlCommand cmd = new SqlCommand(q, conn)) {
                        try {
                            cmd.ExecuteNonQuery();
                        } catch(SqlException) {
                        }
                    }
                }

                // setup for deadlock monitoring
                foreach(string q in ShardMapManagerLoadTests.s_deadlockDetectionSetupQueries) {
                    using(SqlCommand cmd = new SqlCommand(q, conn)) {
                        try {
                            cmd.ExecuteNonQuery();
                        } catch(SqlException) {
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
            Assert.NotNull(lsm);

            // Create range shard map.
            RangeShardMap<int> rsm = smm.CreateRangeShardMap<int>(ShardMapManagerLoadTests.s_rangeShardMapName);
            Assert.NotNull(rsm);

            // Add 'InitialShardCount' shards to list and range shard map.

            for(int i = 0; i < ShardMapManagerLoadTests.InitialShardCount; i++) {
                ShardCreationInfo si = new ShardCreationInfo(
                    new ShardLocation(Globals.ShardMapManagerTestsDatasourceName, ShardMapManagerLoadTests.s_shardedDBs[i]),
                    ShardStatus.Online);

                Shard sList = lsm.CreateShard(si);
                Assert.NotNull(sList);

                Shard sRange = rsm.CreateShard(si);
                Assert.NotNull(sRange);
            }

            // Initialize retry policy
            ShardMapManagerLoadTests.s_retryPolicy = new RetryPolicy<SqlDatabaseTransientErrorDetectionStrategy>(
                new ExponentialBackoff(5, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(100)));
        }

        public void Dispose() {

            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            // Detect inconsistencies for all shard locations in a shard map.
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            RecoveryManager rm = new RecoveryManager(smm);

            bool inconsistencyDetected = false;

            foreach(ShardLocation sl in smm.GetDistinctShardLocations()) {
                IEnumerable<RecoveryToken> gs = rm.DetectMappingDifferences(sl);

                foreach(RecoveryToken g in gs) {
                    var kvps = rm.GetMappingDifferences(g);
                    if(kvps.Keys.Count > 0) {
                        inconsistencyDetected = true;
                        Debug.WriteLine("LSM at location {0} is not consistent with GSM", sl);
                    }
                }
            }

            bool deadlocksDetected = false;

            // Check for deadlocks during the run and cleanup database and deadlock objects on successful run
            using(SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString)) {
                conn.Open();

                // check for any deadlocks occured during the run and cleanup deadlock monitoring objects
                using(SqlCommand cmd = conn.CreateCommand()) {
                    cmd.CommandText = ShardMapManagerLoadTests.s_deadlockDetectionQuery;
                    cmd.CommandType = System.Data.CommandType.Text;

                    try {
                        using(SqlDataReader reader = cmd.ExecuteReader()) {
                            if(reader.HasRows) {
                                // some deadlocks occured during the test, collect xml plan for these deadlocks
                                deadlocksDetected = true;

                                while(reader.Read()) {
                                    Debug.WriteLine("Deadlock information");
                                    Debug.WriteLine(reader.GetSqlXml(0).Value);
                                }
                            }
                        }
                    } catch(SqlException) {
                    }
                }

                // cleanup only if there are no inconsistencies and deadlocks during the run.
                if(!deadlocksDetected && !inconsistencyDetected) {
                    foreach(string q in ShardMapManagerLoadTests.s_deadlockDetectionCleanupQueries) {
                        using(SqlCommand cmd = new SqlCommand(q, conn)) {
                            try {
                                cmd.ExecuteNonQuery();
                            } catch(SqlException) {
                            }
                        }
                    }

                    // Drop shard databases
                    for(int i = 0; i < ShardMapManagerLoadTests.s_shardedDBs.Length; i++) {
                        using(SqlCommand cmd = new SqlCommand(
                            string.Format(Globals.DropDatabaseQuery, ShardMapManagerLoadTests.s_shardedDBs[i]),
                            conn)) {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // Drop shard map manager database
                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

    }
}
