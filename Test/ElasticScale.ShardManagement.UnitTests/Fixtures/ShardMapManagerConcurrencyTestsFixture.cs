using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Fixtures {
    public class ShardMapManagerConcurrencyTestsFixture : IDisposable {

        public ShardMapManagerConcurrencyTestsFixture() {

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
            }

            // Create the shard map manager.
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);
        }

        public void Dispose() {

            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using(SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString)) {
                conn.Open();
                using(SqlCommand cmd = new SqlCommand(
                    string.Format(Globals.DropDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                    conn)) {
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
