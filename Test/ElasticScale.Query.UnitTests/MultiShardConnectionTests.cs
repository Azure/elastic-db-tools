using Microsoft.Azure.SqlDatabase.ElasticScale.ClientTestCommon;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests;

[TestClass]
public class MultiShardConnectionTests
{
    private const string dummyConnectionString = "User ID=x;Password=x";

    /// <summary>
    /// Verifies that <see cref="MultiShardConnection.MultiShardConnection(IEnumerable{ShardLocation}, string)"/>
    /// throws when the input shardlocations are null
    /// </summary>
    [TestMethod]
    public void TestMultiShardConnectionConstructorThrowsNullShardLocations() => AssertExtensions.AssertThrows<ArgumentNullException>(
            () => new MultiShardConnection((IEnumerable<ShardLocation>)null, dummyConnectionString));

    /// <summary>
    /// Verifies that <see cref="MultiShardConnection.MultiShardConnection(IEnumerable{ShardLocation}, string)"/>
    /// throws when the input shards are null
    /// </summary>
    [TestMethod]
    public void TestMultiShardConnectionConstructorThrowsNullShards() => AssertExtensions.AssertThrows<ArgumentNullException>(
            () => new MultiShardConnection((IEnumerable<Shard>)null, dummyConnectionString));

    /// <summary>
    /// Verifies that <see cref="MultiShardConnection.MultiShardConnection(IEnumerable{ShardLocation}, string)"/>
    /// does not multiply evaluate the input enumerable
    /// </summary>
    [TestMethod]
    public void TestMultiShardConnectionConstructorEvaluatesShardLocations()
    {
        var shardLocations = new List<ShardLocation>
        {
            new ShardLocation("server1", "db1"),
            new ShardLocation("server2", "db2"),
            new ShardLocation("server3", "db3")
        };

        var conn = new MultiShardConnection(shardLocations.ToConsumable(), dummyConnectionString);
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
        var shardMap = MultiShardTestUtils.CreateAndGetTestShardMap();

        var shards = shardMap.GetShards().ToList();

        var conn = new MultiShardConnection(shards.ToConsumable(), dummyConnectionString);
        AssertExtensions.AssertSequenceEqual(shards, conn.Shards);
    }
}
