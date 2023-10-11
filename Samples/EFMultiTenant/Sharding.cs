// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System.Data.SqlClient;
using System.Linq;

namespace EntityFrameworkMultiTenant;

internal class Sharding
{
    public ShardMapManager ShardMapManager { get; private set; }

    public ListShardMap<int> ShardMap { get; private set; }

    // Bootstrap Elastic Scale by creating a new shard map manager and a shard map on 
    // the shard map manager database if necessary.
    public Sharding(string smmserver, string smmdatabase, string smmconnstr)
    {
        // Connection string with administrative credentials for the root database
        var connStrBldr = new SqlConnectionStringBuilder(smmconnstr)
        {
            DataSource = smmserver,
            InitialCatalog = smmdatabase
        };

        // Deploy shard map manager.
        ShardMapManager = !ShardMapManagerFactory.TryGetSqlShardMapManager(connStrBldr.ConnectionString, ShardMapManagerLoadPolicy.Lazy, out var smm)
            ? ShardMapManagerFactory.CreateSqlShardMapManager(connStrBldr.ConnectionString)
            : smm;

        ShardMap = !ShardMapManager.TryGetListShardMap("ElasticScaleWithEF", out
        ListShardMap<int> sm)
            ? ShardMapManager.CreateListShardMap<int>("ElasticScaleWithEF")
            : sm;
    }

    // Enter a new shard - i.e. an empty database - to the shard map, allocate a first tenant to it 
    // and kick off EF intialization of the database to deploy schema
    // public void RegisterNewShard(string server, string database, string user, string pwd, string appname, int key)
    public void RegisterNewShard(string server, string database, string connstr, int key)
    {
        var shardLocation = new ShardLocation(server, database);

        if (!ShardMap.TryGetShard(shardLocation, out var shard))
            shard = ShardMap.CreateShard(shardLocation);

        var connStrBldr = new SqlConnectionStringBuilder(connstr)
        {
            DataSource = server,
            InitialCatalog = database
        };

        // Go into a DbContext to trigger migrations and schema deployment for the new shard.
        // This requires an un-opened connection.
        using (var db = new ElasticScaleContext<int>(connStrBldr.ConnectionString))
            // Run a query to engage EF migrations
            _ = (from b in db.Blogs
                 select b).Count();

        // Register the mapping of the tenant to the shard in the shard map.
        // After this step, DDR on the shard map can be used
        if (!ShardMap.TryGetMappingForKey(key, out var mapping))
            _ = ShardMap.CreatePointMapping(key, shard);
    }
}
