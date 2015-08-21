// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.Query;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleStarterKit
{
    internal static class MultiShardQuerySample
    {
        public static void ExecuteMultiShardQuery(RangeShardMap<int> shardMap, string credentialsConnectionString)
        {
            // Get the shards to connect to
            IEnumerable<Shard> shards = shardMap.GetShards();

            // Create the multi-shard connection
            using (MultiShardConnection conn = new MultiShardConnection(shards, credentialsConnectionString))
            {
                // Create a simple command
                using (MultiShardCommand cmd = conn.CreateCommand())
                {
                    // Because this query is grouped by CustomerID, which is sharded,
                    // we will not get duplicate rows.
                    cmd.CommandText = @"
                        SELECT 
                            c.CustomerId, 
                            c.Name AS CustomerName, 
                            COUNT(o.OrderID) AS OrderCount
                        FROM 
                            dbo.Customers AS c INNER JOIN 
                            dbo.Orders AS o
                            ON c.CustomerID = o.CustomerID
                        GROUP BY 
                            c.CustomerId, 
                            c.Name
                        ORDER BY 
                            OrderCount";

                    // Append a column with the shard name where the row came from
                    cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;

                    // Allow for partial results in case some shards do not respond in time
                    cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;

                    // Allow the entire command to take up to 30 seconds
                    cmd.CommandTimeout = 30;

                    // Execute the command. 
                    // We do not need to specify retry logic because MultiShardDataReader will internally retry until the CommandTimeout expires.
                    using (MultiShardDataReader reader = cmd.ExecuteReader())
                    {
                        // Get the column names
                        TableFormatter formatter = new TableFormatter(GetColumnNames(reader).ToArray());

                        int rows = 0;
                        while (reader.Read())
                        {
                            // Read the values using standard DbDataReader methods
                            object[] values = new object[reader.FieldCount];
                            reader.GetValues(values);

                            // Extract just database name from the $ShardLocation pseudocolumn to make the output formater cleaner.
                            // Note that the $ShardLocation pseudocolumn is always the last column
                            int shardLocationOrdinal = values.Length - 1;
                            values[shardLocationOrdinal] = ExtractDatabaseName(values[shardLocationOrdinal].ToString());

                            // Add values to output formatter
                            formatter.AddRow(values);

                            rows++;
                        }

                        Console.WriteLine(formatter.ToString());
                        Console.WriteLine("({0} rows returned)", rows);
                    }
                }
            }
        }

        /// <summary>
        /// Gets the column names from a data reader.
        /// </summary>
        private static IEnumerable<string> GetColumnNames(DbDataReader reader)
        {
            List<string> columnNames = new List<string>();
            foreach (DataRow r in reader.GetSchemaTable().Rows)
            {
                columnNames.Add(r[SchemaTableColumn.ColumnName].ToString());
            }

            return columnNames;
        }

        /// <summary>
        /// Extracts the database name from the provided shard location string.
        /// </summary>
        private static string ExtractDatabaseName(string shardLocationString)
        {
            string[] pattern = new[] { "[", "DataSource=", "Database=", "]" };
            string[] matches = shardLocationString.Split(pattern, StringSplitOptions.RemoveEmptyEntries);
            return matches[1];
        }
    }
}
