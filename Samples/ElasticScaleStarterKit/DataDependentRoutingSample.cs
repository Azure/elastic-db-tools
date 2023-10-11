// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Linq;

namespace ElasticScaleStarterKit;

internal static class DataDependentRoutingSample
{
    private static readonly string[] s_customerNames = new[]
    {
        "AdventureWorks Cycles",
        "Contoso Ltd.",
        "Microsoft Corp.",
        "Northwind Traders",
        "ProseWare, Inc.",
        "Lucerne Publishing",
        "Fabrikam, Inc.",
        "Coho Winery",
        "Alpine Ski House",
        "Humongous Insurance"
    };

    private static readonly Random s_r = new();

    public static void ExecuteDataDependentRoutingQuery(RangeShardMap<int> shardMap, string credentialsConnectionString)
    {
        // A real application handling a request would need to determine the request's customer ID before connecting to the database.
        // Since this is a demo app, we just choose a random key out of the range that is mapped. Here we assume that the ranges
        // start at 0, are contiguous, and are bounded (i.e. there is no range where HighIsMax == true)
        var currentMaxHighKey = shardMap.GetMappings().Max(m => m.Value.High);
        var customerId = GetCustomerId(currentMaxHighKey);
        var customerName = s_customerNames[s_r.Next(s_customerNames.Length)];
        var regionId = 0;
        var productId = 0;

        AddCustomer(
            shardMap,
            credentialsConnectionString,
            customerId,
            customerName,
            regionId);

        AddOrder(
            shardMap,
            credentialsConnectionString,
            customerId,
            productId);
    }

    /// <summary>
    /// Adds a customer to the customers table (or updates the customer if that id already exists).
    /// </summary>
    private static void AddCustomer(
        ShardMap shardMap,
        string credentialsConnectionString,
        int customerId,
        string name,
        int regionId) =>
        // Open and execute the command with retry for transient faults. Note that if the command fails, the connection is closed, so
        // the entire block is wrapped in a retry. This means that only one command should be executed per block, since if we had multiple
        // commands then the first command may be executed multiple times if later commands fail.
        SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
        {
            // Looks up the key in the shard map and opens a connection to the shard
            using var conn = shardMap.OpenConnectionForKey(customerId, credentialsConnectionString);
            // Create a simple command that will insert or update the customer information
            var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                    IF EXISTS (SELECT 1 FROM Customers WHERE CustomerId = @customerId)
                        UPDATE Customers
                            SET Name = @name, RegionId = @regionId
                            WHERE CustomerId = @customerId
                    ELSE
                        INSERT INTO Customers (CustomerId, Name, RegionId)
                        VALUES (@customerId, @name, @regionId)";
            _ = cmd.Parameters.AddWithValue("@customerId", customerId);
            _ = cmd.Parameters.AddWithValue("@name", name);
            _ = cmd.Parameters.AddWithValue("@regionId", regionId);
            cmd.CommandTimeout = 60;

            // Execute the command
            _ = cmd.ExecuteNonQuery();
        });

    /// <summary>
    /// Adds an order to the orders table for the customer.
    /// </summary>
    private static void AddOrder(
        ShardMap shardMap,
        string credentialsConnectionString,
        int customerId,
        int productId)
    {
        SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
        {
            // Looks up the key in the shard map and opens a connection to the shard
            using var conn = shardMap.OpenConnectionForKey(customerId, credentialsConnectionString);
            // Create a simple command that will insert a new order
            var cmd = conn.CreateCommand();

            // Create a simple command
            cmd.CommandText = @"INSERT INTO dbo.Orders (CustomerId, OrderDate, ProductId)
                                        VALUES (@customerId, @orderDate, @productId)";
            _ = cmd.Parameters.AddWithValue("@customerId", customerId);
            _ = cmd.Parameters.AddWithValue("@orderDate", DateTime.Now.Date);
            _ = cmd.Parameters.AddWithValue("@productId", productId);
            cmd.CommandTimeout = 60;

            // Execute the command
            _ = cmd.ExecuteNonQuery();
        });

        ConsoleUtils.WriteInfo("Inserted order for customer ID: {0}", customerId);
    }

    /// <summary>
    /// Gets a customer ID to insert into the customers table.
    /// </summary>
    private static int GetCustomerId(int maxid) =>
        // If this were a real app and we were inserting customer IDs, we would need a 
        // service that generates unique new customer IDs.

        // Since this is a demo, just create a random customer ID. To keep the numbers
        // manageable for demo purposes, only use a range of integers that lies within existing ranges.

        s_r.Next(0, maxid);
}
