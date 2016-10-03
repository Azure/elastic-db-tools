using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Fixtures {
    public class ScenarioTestsFixture : IDisposable {
        public ScenarioTestsFixture() {
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

                // Create PerTenantDB databases
                for(int i = 0; i < ScenarioTests.s_perTenantDBs.Length; i++) {
                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ScenarioTests.s_perTenantDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }

                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ScenarioTests.s_perTenantDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }
                }

                // Create MultiTenantDB databases
                for(int i = 0; i < ScenarioTests.s_multiTenantDBs.Length; i++) {
                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.DropDatabaseQuery, ScenarioTests.s_multiTenantDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }

                    using(SqlCommand cmd = new SqlCommand(
                        string.Format(Globals.CreateDatabaseQuery, ScenarioTests.s_multiTenantDBs[i]),
                        conn)) {
                        cmd.ExecuteNonQuery();
                    }
                }
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
