// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Dapper;
using DapperExtensions;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

////////////////////////////////////////////////////////////////////////////////////////
// This sample illustrates the adjustments that need to be made to use Dapper 
// and DapperExtensions in combination with Azure SQL DB Elastic Scale 
// to scale out your data tier across many databases and
// benefit from Elastic Scale capabilities for Data Dependent Routing and 
// Shard Map Management.
////////////////////////////////////////////////////////////////////////////////////////

namespace ElasticDapper
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
        private static string s_applicationName = "ESC_Dapv1.0";

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
            Sharding shardingLayer = new Sharding(s_server, s_shardmapmgrdb, connStrBldr.ConnectionString);
            shardingLayer.RegisterNewShard(s_server, s_shard1, connStrBldr.ConnectionString, s_tenantId1);
            shardingLayer.RegisterNewShard(s_server, s_shard2, connStrBldr.ConnectionString, s_tenantId2);

            // Create schema on each shard.
            foreach (string shard in new[] {s_shard1, s_shard2})
            {
                CreateSchema(shard);
            }

            // Do work for tenant 1 :-)
            // For tenant 1, let's stay with plain vanilla Dapper
            // and spell out the T-SQL we use to map into objects.

            // Create and save a new Blog 
            Console.Write("Enter a name for a new Blog: ");
            var name = Console.ReadLine();

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (SqlConnection sqlconn = shardingLayer.ShardMap.OpenConnectionForKey(
                    key: s_tenantId1,
                    connectionString: connStrBldr.ConnectionString,
                    options: ConnectionOptions.Validate))
                {
                    var blog = new Blog { Name = name };
                    sqlconn.Insert(blog);
                }
            });

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (SqlConnection sqlconn = shardingLayer.ShardMap.OpenConnectionForKey(
                    key: s_tenantId1,
                    connectionString: connStrBldr.ConnectionString,
                    options: ConnectionOptions.Validate))
                {
                    // Display all Blogs for tenant 1
                    IEnumerable<Blog> result = sqlconn.Query<Blog>(@"
                        SELECT * 
                        FROM Blog
                        ORDER BY Name");

                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId1);
                    foreach (var item in result)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });

            // Do work for tenant 2 :-)
            // Here I am going to illustrate how to integrate
            // with DapperExtensions which saves us the T-SQL 
            //
            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (SqlConnection sqlconn = shardingLayer.ShardMap.OpenConnectionForKey(
                    key: s_tenantId2,
                    connectionString: connStrBldr.ConnectionString,
                    options: ConnectionOptions.Validate))
                {
                    // Display all Blogs for tenant 2
                    IEnumerable<Blog> result = sqlconn.GetList<Blog>();
                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId2);
                    foreach (var item in result)
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
                using (SqlConnection sqlconn = shardingLayer.ShardMap.OpenConnectionForKey(
                    key: s_tenantId2,
                    connectionString: connStrBldr.ConnectionString,
                    options: ConnectionOptions.Validate))
                {
                    var blog = new Blog { Name = name2 };
                    sqlconn.Insert(blog);
                }
            });

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (SqlConnection sqlconn = shardingLayer.ShardMap.OpenConnectionForKey(s_tenantId2, connStrBldr.ConnectionString, ConnectionOptions.Validate))
                {
                    // Display all Blogs for tenant 2
                    IEnumerable<Blog> result = sqlconn.GetList<Blog>();
                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId2);
                    foreach (var item in result)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static void CreateSchema(string shardName)
        {
            SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder
            {
                UserID = s_userName,
                Password = s_password,
                ApplicationName = s_applicationName,
                DataSource = s_server,
                InitialCatalog = shardName
            };

            using (SqlConnection conn = new SqlConnection(connStrBldr.ToString()))
            {
                conn.Open();
                conn.Execute(@"
                    IF (OBJECT_ID('[dbo].[Blog]', 'U') IS NULL)
                    CREATE TABLE [dbo].[Blog](
	                    [BlogId] [int] IDENTITY(1,1) PRIMARY KEY,
	                    [Name] [nvarchar](max) NULL,
	                    [Url] [nvarchar](max) NULL,
                    )");
            }
        }
    }
}
