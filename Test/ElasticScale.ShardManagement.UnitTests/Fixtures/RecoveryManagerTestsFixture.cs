using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Fixtures {
    public class RecoveryManagerTestsFixture : IDisposable {

        public RecoveryManagerTestsFixture() {
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
                for(int i = 0; i < RecoveryManagerTests.s_shardedDBs.Length; i++) {
                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, RecoveryManagerTests.s_shardedDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }

                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, RecoveryManagerTests.s_shardedDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            // Create shard map manager.
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);
        }

        public void Dispose() {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using(SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString)) {
                conn.Open();
                // Drop shard databases
                for(int i = 0; i < RecoveryManagerTests.s_shardedDBs.Length; i++) {
                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, RecoveryManagerTests.s_shardedDBs[i]),
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
