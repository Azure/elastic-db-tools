// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Linq;

////////////////////////////////////////////////////////////////////////////////////////
// This sample follows the CodeFirstNewDatabase Blogging tutorial for EF.
// It illustrates the adjustments that need to be made to use EF in combination
// with the Entity Framewor to scale out your data tier across many databases and
// benefit from Elastic Scale capabilities for Data Dependent Routing and 
// Shard Map Management.
////////////////////////////////////////////////////////////////////////////////////////

namespace EFCodeFirstElasticScale
{
    // This sample requires three pre-created empty SQL Server databases. 
    // The first database serves as the shard map manager database to store the Elastic Scale shard map.
    // The remaining two databases serve as shards to hold the data for the sample.
    internal class Program
    {
        // You need to adjust the following settings to your database server and database names in Azure Db
        private static string s_server = "[YourSQLServerName]";
        private static string s_shardmapmgrdb = "[YourShardMapManagerDatabaseName]";
        private static string s_shard1 = "[YourShard01DatabaseName]";
        private static string s_shard2 = "[YourShard02DatabaseName]";
        private static string s_userName = "YourUserName";
        private static string s_password = "YourPassword";
        private static string s_applicationName = "ESC_EFv1.0";

        // Just two tenants for now.
        // Those we will allocate to shards.
        private static int s_tenantId1 = 42;
        private static int s_tenantId2 = 12;

        public static void Main()
        {
            SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder
            {
                UserID = s_userName,
                Password = s_password,
                ApplicationName = s_applicationName
            };

            // Bootstrap the shard map manager, register shards, and store mappings of tenants to shards
            // Note that you can keep working with existing shard maps. There is no need to 
            // re-create and populate the shard map from scratch every time.
            Console.WriteLine("Checking for existing shard map and creating new shard map if necessary.");

            Sharding sharding = new Sharding(s_server, s_shardmapmgrdb, connStrBldr.ConnectionString);
            sharding.RegisterNewShard(s_server, s_shard1, connStrBldr.ConnectionString, s_tenantId1);
            sharding.RegisterNewShard(s_server, s_shard2, connStrBldr.ConnectionString, s_tenantId2);

            // Do work for tenant 1 :-)

            // Create and save a new Blog 
            Console.Write("Enter a name for a new Blog: ");
            var name = Console.ReadLine();

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId1, connStrBldr.ConnectionString))
                {
                    var blog = new Blog { Name = name };
                    db.Blogs.Add(blog);
                    db.SaveChanges();
                }
            });

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId1, connStrBldr.ConnectionString))
                {
                    // Display all Blogs for tenant 1
                    var query = from b in db.Blogs
                                orderby b.Name
                                select b;

                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId1);
                    foreach (var item in query)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });

            // Do work for tenant 2 :-)
            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId2, connStrBldr.ConnectionString))
                {
                    // Display all Blogs from the database 
                    var query = from b in db.Blogs
                                orderby b.Name
                                select b;

                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId2);
                    foreach (var item in query)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });

            // Create and save a new Blog 
            Console.Write("Enter a name for a new Blog: ");
            var name2 = Console.ReadLine();

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId2, connStrBldr.ConnectionString))
                {
                    var blog = new Blog { Name = name2 };
                    db.Blogs.Add(blog);
                    db.SaveChanges();
                }
            });

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId2, connStrBldr.ConnectionString))
                {
                    // Display all Blogs from the database 
                    var query = from b in db.Blogs
                                orderby b.Name
                                select b;

                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId2);
                    foreach (var item in query)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}
