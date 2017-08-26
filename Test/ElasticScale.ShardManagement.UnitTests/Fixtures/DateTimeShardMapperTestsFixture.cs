using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Fixtures {
    public class DateTimeShardMapperTestsFixture : IDisposable {

        public DateTimeShardMapperTestsFixture() {
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
                for(int i = 0; i < DateTimeShardMapperTests.s_shardedDBs.Length; i++) {
                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }

                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
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

            ListShardMap<DateTime> lsm = smm.CreateListShardMap<DateTime>(DateTimeShardMapperTests.s_listShardMapName);

            Assert.NotNull(lsm);

            Assert.Equal(DateTimeShardMapperTests.s_listShardMapName, lsm.Name);

            // Create range shard map.
            RangeShardMap<DateTime> rsm = smm.CreateRangeShardMap<DateTime>(DateTimeShardMapperTests.s_rangeShardMapName);

            Assert.NotNull(rsm);

            Assert.Equal(DateTimeShardMapperTests.s_rangeShardMapName, rsm.Name);

        }

        public void Dispose() {

            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using(SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString)) {
                conn.Open();
                // Drop shard databases
                for(int i = 0; i < DateTimeShardMapperTests.s_shardedDBs.Length; i++) {
                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, DateTimeShardMapperTests.s_shardedDBs[i]),
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
