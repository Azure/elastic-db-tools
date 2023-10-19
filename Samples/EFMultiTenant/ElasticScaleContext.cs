// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity;
using Microsoft.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace EFMultiTenantElasticScale
{
    public class ElasticScaleContext<T> : DbContext
    {
        // Let's use the standard DbSets from the EF tutorial
        public DbSet<Blog> Blogs { get; set; }

        public DbSet<Post> Posts { get; set; }

        public DbSet<User> Users { get; set; }

        // Regular constructor calls should not happen.
        // 1.) Use the protected c'tor with the connection string parameter
        // to intialize a new shard. 
        // 2.) Use the public c'tor with the shard map parameter in
        // the regular application calls with a tenant id.
        private ElasticScaleContext()
        {
        }

        // C'tor to deploy schema and migrations to a new shard
        protected internal ElasticScaleContext(string connectionString)
            : base(SetInitializerForConnection(connectionString))
        {
        }

        // Only static methods are allowed in calls into base class c'tors
        private static string SetInitializerForConnection(string connnectionString)
        {
            // We want existence checks so that the schema can get deployed
            Database.SetInitializer<ElasticScaleContext<T>>(new CreateDatabaseIfNotExists<ElasticScaleContext<T>>());
            return connnectionString;
        }

        // C'tor for data dependent routing. This call will open a validated connection routed to the proper
        // shard by the shard map manager. Note that the base class c'tor call will fail for an open connection
        // if migrations need to be done and SQL credentials are used. This is the reason for the 
        // separation of c'tors into the DDR case (this c'tor) and the internal c'tor for new shards.
        public ElasticScaleContext(ShardMap shardMap, T shardingKey, string connectionStr)
            : base(OpenDDRConnection(shardMap, shardingKey, connectionStr), true /* contextOwnsConnection */)
        {
        }

        /// <summary>
        /// Wrapper function for ShardMap.OpenConnectionForKey() that automatically sets CONTEXT_INFO to the correct
        /// tenantId before returning a connection. As a best practice, you should only open connections using this 
        /// method to ensure that CONTEXT_INFO is always set before executing a query.
        /// </summary>
        public static SqlConnection OpenDDRConnection(ShardMap shardMap, T shardingKey, string connectionStr)
        {
            // No initialization
            Database.SetInitializer<ElasticScaleContext<T>>(null);

            // Ask shard map to broker a validated connection for the given key
            SqlConnection conn = null;
            try
            {
                conn = shardMap.OpenConnectionForKey(shardingKey, connectionStr, ConnectionOptions.Validate);

                // Set TenantId in SESSION_CONTEXT to shardingKey to enable Row-Level Security filtering
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = @"exec sp_set_session_context @key=N'TenantId', @value=@shardingKey";
                cmd.Parameters.AddWithValue("@shardingKey", shardingKey);
                cmd.ExecuteNonQuery();

                return conn;
            }
            catch (Exception)
            {
                if (conn != null)
                {
                    conn.Dispose();
                }

                throw;
            }
        }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            modelBuilder.Entity<User>()
                .Property(u => u.DisplayName)
                .HasColumnName("display_name");
        }
    }
}