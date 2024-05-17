// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Data.SqlClient;

namespace ElasticDapper
{
    internal class Sharding
    {
        public ShardMapManager ShardMapManager { get; private set; }

        public ListShardMap<int> ShardMap { get; private set; }

        // Bootstrap Elastic Scale by creating a new shard map manager and a shard map on 
        // the shard map manager database if necessary.
        public Sharding(string smmserver, string smmdatabase, string smmconnstr)
        {
            // Connection string with administrative credentials for the root database
            SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(smmconnstr);
            connStrBldr.DataSource = smmserver;
            connStrBldr.InitialCatalog = smmdatabase;

            // Deploy shard map manager.
            ShardMapManager smm;
            if (!ShardMapManagerFactory.TryGetSqlShardMapManager(connStrBldr.ConnectionString, ShardMapManagerLoadPolicy.Lazy, out smm))
            {
                ShardMapManager = ShardMapManagerFactory.CreateSqlShardMapManager(connStrBldr.ConnectionString);
            }
            else
            {
                ShardMapManager = smm;
            }

            ListShardMap<int> sm;
            if (!ShardMapManager.TryGetListShardMap<int>("ElasticScaleWithDapper", out sm))
            {
                ShardMap = ShardMapManager.CreateListShardMap<int>("ElasticScaleWithDapper");
            }
            else
            {
                ShardMap = sm;
            }
        }

        // Enter a new shard - i.e. an empty database - to the shard map, allocate a first tenant to it 
        public void RegisterNewShard(string server, string database, int key)
        {
            Shard shard;
            ShardLocation shardLocation = new ShardLocation(server, database);

            if (!ShardMap.TryGetShard(shardLocation, out shard))
            {
                shard = ShardMap.CreateShard(shardLocation);
            }

            // Register the mapping of the tenant to the shard in the shard map.
            // After this step, DDR on the shard map can be used
            PointMapping<int> mapping;
            if (!ShardMap.TryGetMappingForKey(key, out mapping))
            {
                ShardMap.CreatePointMapping(key, shard);
            }
        }
    }
}
