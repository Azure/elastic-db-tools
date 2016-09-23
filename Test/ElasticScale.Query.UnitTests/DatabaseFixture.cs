using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query {
    class DatabaseFixture : IDisposable {

        public DatabaseFixture() {
            // Drop and recreate the test databases, tables, and data that we will use to verify
            // the functionality.
            // For now I have hardcoded the server location and database names.  A better approach would be
            // to make the server location configurable and the database names be guids.
            // Not the top priority right now, though.
            //
            SqlConnection.ClearAllPools();
            MultiShardTestUtils.DropAndCreateDatabases();
            MultiShardTestUtils.CreateAndPopulateTables();
        }

        public void Dispose() {
            // We need to clear the connection pools so that we don't get a database still in use error
            // resulting from our attenpt to drop the databases below.
            //
            SqlConnection.ClearAllPools();
            MultiShardTestUtils.DropDatabases();
        }
    }
}
