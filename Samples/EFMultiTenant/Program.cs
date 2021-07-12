// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity.Infrastructure;
using System.Data.SqlClient;
using System.Linq;

////////////////////////////////////////////////////////////////////////////////////////
// This sample follows the CodeFirstNewDatabase Blogging tutorial for EF.
// It illustrates the adjustments that need to be made to use EF in combination
// with the Entity Framework to scale out your data tier across many databases and
// benefit from Elastic Database Tools capabilities for Data Dependent Routing and 
// Shard Map Management.
//
// In particular, this sample shows how to configure multi-tenant shards, using 
// Row-Level Security (RLS) to ensure that tenants can only access their own rows.
////////////////////////////////////////////////////////////////////////////////////////

namespace EFMultiTenantElasticScale
{
    // This sample requires three pre-created empty SQL Server databases. 
    // The first database serves as the shard map manager database to store the Elastic Database shard map.
    // The remaining two databases serve as multi-tenant shards to hold the data for the sample.
    internal class Program
    {
        // You need to adjust the following settings to your database server and database names in Azure Db
        private static string s_server = "[YourSQLServerName]";
        private static string s_shardmapmgrdb = "[YourShardMapManagerDatabaseName]";
        private static string s_shard1 = "[YourShard01DatabaseName]";
        private static string s_shard2 = "[YourShard02DatabaseName]";
        private static string s_userName = "[YourUserName]";
        private static string s_password = "[YourPassword]";
        private static string s_applicationName = "ESC_EFMTv1.0";

        // Four tenants
        private static int s_tenantId1 = 1;
        private static int s_tenantId2 = 2;
        private static int s_tenantId3 = 3;
        private static int s_tenantId4 = 4;

        public static void Main()
        {
            SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder
            {
                UserID = s_userName,
                Password = s_password,
                ApplicationName = s_applicationName
            };

            // Bootstrap the shard map manager, register shards, and store mappings of tenants to shards.
            // Note that you can keep working with existing shard maps. There is no need to 
            // re-create and populate the shard map from scratch every time.
            Console.WriteLine("Checking for existing shard map and creating new shard map if necessary.");

            Sharding sharding = new Sharding(s_server, s_shardmapmgrdb, connStrBldr.ConnectionString);
            sharding.RegisterNewShard(s_server, s_shard1, connStrBldr.ConnectionString, s_tenantId1);
            sharding.RegisterNewShard(s_server, s_shard2, connStrBldr.ConnectionString, s_tenantId2);
            sharding.RegisterNewShard(s_server, s_shard1, connStrBldr.ConnectionString, s_tenantId3);
            sharding.RegisterNewShard(s_server, s_shard2, connStrBldr.ConnectionString, s_tenantId4);

            // Using Entity Framework and LINQ, create a new blog and then display all blogs for each tenant
            Console.WriteLine("\n--\n\nCreate a new blog for each tenant, then list the blogs belonging to that tenant.");
            Console.WriteLine("If row-level security has not been enabled, then the blogs for all tenants on the shard database will be listed.");

            int[] tenants = new int[] { s_tenantId1, s_tenantId2, s_tenantId3, s_tenantId4 };
            foreach (int tenantId in tenants)
            {
                Console.Write("\nEnter a name for a new Blog for TenantId {0}: ", tenantId);
                var name = Console.ReadLine();

                SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
                {
                    using (var db = new ElasticScaleContext<int>(sharding.ShardMap, tenantId, connStrBldr.ConnectionString))
                    {
                        var blog = new Blog { Name = name, TenantId = tenantId }; // must specify TenantId unless using default constraints to auto-populate
                        db.Blogs.Add(blog);
                        db.SaveChanges();

                        // If Row-Level Security is enabled, tenants will only display their own blogs
                        // Otherwise, tenants will see blogs for all tenants on the shard db
                        var query = from b in db.Blogs
                                    orderby b.Name
                                    select b;

                        Console.WriteLine("All blogs for TenantId {0}:", tenantId);
                        foreach (var item in query)
                        {
                            Console.WriteLine(item.Name);
                        }
                    }
                });
            }

            // Example query via ADO.NET SqlClient
            // If Row-Level Security is enabled, only Tenant 4's blogs will be listed
            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                // Note: We are using a wrapper function OpenDDRConnection that automatically set SESSION_CONTEXT with the specified TenantId. 
                // This is a best practice to ensure that SESSION_CONTEXT is always set before executing a query.
                using (SqlConnection conn = ElasticScaleContext<int>.OpenDDRConnection(sharding.ShardMap, s_tenantId4, connStrBldr.ConnectionString))
                {
                    SqlCommand cmd = conn.CreateCommand();
                    cmd.CommandText = @"SELECT * FROM Blogs";

                    Console.WriteLine("\n--\n\nAll blogs for TenantId {0} (using ADO.NET SqlClient):", s_tenantId4);
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine("{0}", reader["Name"]);
                    }
                }
            });

            // Because of the RLS block predicate, attempting to insert a row for the wrong tenant will throw an error.
            Console.WriteLine("\n--\n\nTrying to create a new Blog for TenantId {0} while connected as TenantId {1}: ", s_tenantId2, s_tenantId3);
            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId3, connStrBldr.ConnectionString))
                {
                    // Verify that block predicate prevents Tenant 3 from inserting rows for Tenant 2
                    try
                    {
                        var bad_blog = new Blog { Name = "BAD BLOG", TenantId = s_tenantId2 };
                        db.Blogs.Add(bad_blog);
                        db.SaveChanges();
                        Console.WriteLine("No error thrown - make sure your security policy has a block predicate on this table in each shard database.");
                    }
                    catch (DbUpdateException)
                    {
                        Console.WriteLine("Can't insert blog for incorrect tenant.");
                    }
                }
            });

            Console.WriteLine("\n--\n\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
