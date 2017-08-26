using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Fixtures {
    public class ShardMapManagerFactoryTestsFixture : IDisposable {

        public ShardMapManagerFactoryTestsFixture() {

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

                // Testing TryGetSqlShardMapManager failure case here instead of in TryGetShardMapManager_Fail()
                // There is no method to cleanup GSM objects, so if some other test runs in lab before 
                // TryGetShardMapManager_Fail, then this call will actually suceed as it will find earlier SMM structures.
                // Calling it just after creating database makes sure that GSM does not exist.
                // Other options were to recreate SMM database in tests (this will increase test duration) or
                // delete storage structures (t-sql delete) in the test which is not very clean solution.

                ShardMapManager smm = null;

                bool lookupSmm = ShardMapManagerFactory.TryGetSqlShardMapManager(
                        Globals.ShardMapManagerConnectionString,
                        ShardMapManagerLoadPolicy.Eager,
                        RetryBehavior.DefaultRetryBehavior,
                        out smm);

                Assert.False(lookupSmm);
            }
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
