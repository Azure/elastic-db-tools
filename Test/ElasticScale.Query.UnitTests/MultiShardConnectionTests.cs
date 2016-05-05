using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    [TestClass]
    public class MultiShardConnectionTests
    {
        private const string dummyConnectionString = "User ID=x;Password=x";

        /// <summary>
        /// Verifies that <see cref="MultiShardConnection.MultiShardConnection(IEnumerable{ShardLocation}, string)"/>
        /// throws when the input shardlocations are null
        /// </summary>
        [TestMethod]
        public void TestMultiShardConnectionConstructorThrowsNullShardLocations()
        {
            AssertExtensions.AssertThrows<ArgumentNullException>(
                () => new MultiShardConnection((IEnumerable<ShardLocation>)null, dummyConnectionString));
        }

        /// <summary>
        /// Verifies that <see cref="MultiShardConnection.MultiShardConnection(IEnumerable{ShardLocation}, string)"/>
        /// throws when the input shards are null
        /// </summary>
        [TestMethod]
        public void TestMultiShardConnectionConstructorThrowsNullShards()
        {
            AssertExtensions.AssertThrows<ArgumentNullException>(
                () => new MultiShardConnection((IEnumerable<Shard>)null, dummyConnectionString));
        }

        /// <summary>
        /// Verifies that <see cref="MultiShardConnection.MultiShardConnection(IEnumerable{ShardLocation}, string)"/>
        /// does not multiply evaluate the input enumerable
        /// </summary>
        [TestMethod]
        public void TestMultiShardConnectionConstructorEvaluatesShardLocations()
        {
            List<ShardLocation> shardLocations = new List<ShardLocation>
            {
                new ShardLocation("server1", "db1"),
                new ShardLocation("server2", "db2"),
                new ShardLocation("server3", "db3")
            };

            MultiShardConnection conn = new MultiShardConnection(shardLocations.ToConsumable(), dummyConnectionString);
            AssertExtensions.AssertSequenceEqual(shardLocations, conn.ShardLocations);
        }

        /// <summary>
        /// Verifies that <see cref="MultiShardConnection.MultiShardConnection(IEnumerable{Shard}, string)"/>
        /// does not multiply evaluate the input enumerable
        /// </summary>
        [TestMethod]
        public void TestMultiShardConnectionConstructorEvaluatesShards()
        {
            MultiShardTestUtils.DropAndCreateDatabases();
            ShardMap shardMap = MultiShardTestUtils.CreateAndGetTestShardMap();

            List<Shard> shards = shardMap.GetShards().ToList();

            MultiShardConnection conn = new MultiShardConnection(shards.ToConsumable(), dummyConnectionString);
            AssertExtensions.AssertSequenceEqual(shards, conn.Shards);
        }
    }
}
