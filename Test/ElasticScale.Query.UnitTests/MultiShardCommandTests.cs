using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Azure.SqlDatabase.ElasticScale.Query;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    [TestClass]
    public class MultiShardCommandTests
    {
        private const string dummyConnectionString = "User ID=x;Password=x";

        /// <summary>
        /// Verifies that <see cref="MultiShardCommand.ExecuteNonQuery()"/> and related methods throw not supported.
        /// </summary>
        [TestMethod]
        public void ExecuteNonQueryNonSupported()
        {
            List<ShardLocation> shardLocations = new List<ShardLocation>
            {
                new ShardLocation("server1", "db1"),
                new ShardLocation("server2", "db2"),
                new ShardLocation("server3", "db3")
            };

            MultiShardConnection conn = new MultiShardConnection(shardLocations, dummyConnectionString);
            MultiShardCommand cmd = conn.CreateCommand();

            AssertExtensions.AssertThrows<NotSupportedException>(() => cmd.ExecuteNonQuery());
            AssertExtensions.WaitAndAssertThrows<NotSupportedException>(cmd.ExecuteNonQueryAsync());
            AssertExtensions.WaitAndAssertThrows<NotSupportedException>(cmd.ExecuteNonQueryAsync(CancellationToken.None));
        }

        /// <summary>
        /// Verifies that <see cref="MultiShardCommand.ExecuteScalar()"/> and related methods throw not supported.
        /// </summary>
        [TestMethod]
        public void ExecuteScalarNonSupported()
        {
            List<ShardLocation> shardLocations = new List<ShardLocation>
            {
                new ShardLocation("server1", "db1"),
                new ShardLocation("server2", "db2"),
                new ShardLocation("server3", "db3")
            };

            MultiShardConnection conn = new MultiShardConnection(shardLocations, dummyConnectionString);
            MultiShardCommand cmd = conn.CreateCommand();

            AssertExtensions.AssertThrows<NotSupportedException>(() => cmd.ExecuteScalar());
            AssertExtensions.WaitAndAssertThrows<NotSupportedException>(cmd.ExecuteScalarAsync());
            AssertExtensions.WaitAndAssertThrows<NotSupportedException>(cmd.ExecuteScalarAsync(CancellationToken.None));
        }
    }
}
