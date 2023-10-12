// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

namespace ElasticScaleStarterKit
{
    /// <summary>
    /// Helper methods for interacting with SQL Databases.
    /// </summary>
    internal static class SqlDatabaseUtils
    {
        /// <summary>
        /// SQL master database name.
        /// </summary>
        public const string MasterDatabaseName = "master";

        /// <summary>
        /// Returns true if we can connect to the database.
        /// </summary>
        public static bool TryConnectToSqlDatabase()
        {
            string connectionString =
                Configuration.GetConnectionString(
                    Configuration.ShardMapManagerServerName,
                    MasterDatabaseName);

            try
            {
                using (SqlConnection conn = new SqlConnection(
                    connectionString))
                {
                    conn.RetryLogicProvider = SqlRetryProvider;
                    conn.Open();
                }

                return true;
            }
            catch (SqlException e)
            {
                ConsoleUtils.WriteWarning("Failed to connect to SQL database with connection string:");
                Console.WriteLine("\n{0}\n", connectionString);
                ConsoleUtils.WriteWarning("If this connection string is incorrect, please update the Sql Database settings in App.Config.\n\nException message: {0}", e.Message);
                return false;
            }
        }

        public static bool DatabaseExists(string server, string db)
        {
            using (SqlConnection conn = new SqlConnection(
                Configuration.GetConnectionString(server, MasterDatabaseName)))
            {
                conn.RetryLogicProvider = SqlRetryProvider;
                conn.Open();

                SqlCommand cmd = conn.CreateCommand();
                cmd.RetryLogicProvider = SqlRetryProvider;

                cmd.CommandText = "select count(*) from sys.databases where name = @dbname";
                cmd.Parameters.AddWithValue("@dbname", db);
                cmd.CommandTimeout = 60;
                
                int count = (int)cmd.ExecuteScalar();

                bool exists = count > 0;
                return exists;
            }
        }

        public static bool DatabaseIsOnline(string server, string db)
        {
            using (SqlConnection conn = new SqlConnection(
                Configuration.GetConnectionString(server, MasterDatabaseName)))
            {
                conn.RetryLogicProvider = SqlRetryProvider;
                conn.Open();

                SqlCommand cmd = conn.CreateCommand();
                cmd.RetryLogicProvider = SqlRetryProvider;

                cmd.CommandText = "select count(*) from sys.databases where name = @dbname and state = 0"; // online
                cmd.Parameters.AddWithValue("@dbname", db);
                cmd.CommandTimeout = 60;

                int count = (int)cmd.ExecuteScalar();

                bool exists = count > 0;
                return exists;
            }
        }

        public static void CreateDatabase(string server, string db)
        {
            ConsoleUtils.WriteInfo("Creating database {0}", db);
            using (SqlConnection conn = new SqlConnection(
                Configuration.GetConnectionString(server, MasterDatabaseName)))
            {
                conn.RetryLogicProvider = SqlRetryProvider;
                conn.Open();
                SqlCommand cmd = conn.CreateCommand();

                // Determine if we are connecting to Azure SQL DB
                cmd.CommandText = "SELECT SERVERPROPERTY('EngineEdition')";
                cmd.CommandTimeout = 60;
                cmd.RetryLogicProvider = SqlRetryProvider;

                int engineEdition = (int)cmd.ExecuteScalar();

                if (engineEdition == 5)
                {
                    // Azure SQL DB
                    if (!DatabaseExists(server, db))
                    {
                        // Begin creation (which is async for Standard/Premium editions)
                        cmd.CommandText = string.Format(
                            "CREATE DATABASE {0} (EDITION = '{1}')",
                            BracketEscapeName(db),
                            Configuration.DatabaseEdition);
                        cmd.CommandTimeout = 180;
                        cmd.ExecuteNonQuery();
                    }

                    // Wait for the operation to complete
                    while (!DatabaseIsOnline(server, db))
                    {
                        ConsoleUtils.WriteInfo("Waiting for database {0} to come online...", db);
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                    }

                    ConsoleUtils.WriteInfo("Database {0} is online", db);
                }
                else
                {
                    // Other edition of SQL DB
                    cmd.CommandText = string.Format("CREATE DATABASE {0}", BracketEscapeName(db));
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public static void DropDatabase(string server, string db)
        {
            ConsoleUtils.WriteInfo("Dropping database {0}", db);
            using (SqlConnection conn = new SqlConnection(
                Configuration.GetConnectionString(server, MasterDatabaseName)))
            {
                conn.RetryLogicProvider = SqlRetryProvider;
                conn.Open();
                SqlCommand cmd = conn.CreateCommand();

                // Determine if we are connecting to Azure SQL DB
                cmd.CommandText = "SELECT SERVERPROPERTY('EngineEdition')";
                cmd.CommandTimeout = 60;
                int engineEdition = (int)cmd.ExecuteScalar();

                // Drop the database
                if (engineEdition == 5)
                {
                    // Azure SQL DB

                    cmd.CommandText = string.Format("DROP DATABASE {0}", BracketEscapeName(db));
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    cmd.CommandText = string.Format(
                        @"ALTER DATABASE {0} SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                        DROP DATABASE {0}",
                        BracketEscapeName(db));
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public static void ExecuteSqlScript(string server, string db, string schemaFile)
        {
            ConsoleUtils.WriteInfo("Executing script {0}", schemaFile);
            using (SqlConnection conn = new SqlConnection(
                Configuration.GetConnectionString(server, db)))
            {
                conn.RetryLogicProvider = SqlRetryProvider;
                conn.Open();
                SqlCommand cmd = conn.CreateCommand();
                cmd.RetryLogicProvider = SqlRetryProvider;

                // Read the commands from the sql script file
                IEnumerable<string> commands = ReadSqlScript(schemaFile);

                foreach (string command in commands)
                {
                    cmd.CommandText = command;
                    cmd.CommandTimeout = 60;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static IEnumerable<string> ReadSqlScript(string scriptFile)
        {
            List<string> commands = new List<string>();
            using (TextReader tr = new StreamReader(scriptFile))
            {
                StringBuilder sb = new StringBuilder();
                string line;
                while ((line = tr.ReadLine()) != null)
                {
                    if (line == "GO")
                    {
                        commands.Add(sb.ToString());
                        sb.Clear();
                    }
                    else
                    {
                        sb.AppendLine(line);
                    }
                }
            }

            return commands;
        }

        /// <summary>
        /// Escapes a SQL object name with brackets to prevent SQL injection.
        /// </summary>
        private static string BracketEscapeName(string sqlName)
        {
            return '[' + sqlName.Replace("]", "]]") + ']';
        }

        // Create a retry logic provider
        public static SqlRetryLogicBaseProvider SqlRetryProvider = SqlConfigurableRetryFactory.CreateExponentialRetryProvider(SqlRetryPolicy);

        /// <summary>
        /// Gets the retry policy to use for connections to SQL Server.
        /// </summary>
        private static SqlRetryLogicOption SqlRetryPolicy => new()
        {
            // Tries 5 times before throwing an exception
            NumberOfTries = 5,
            // Preferred gap time to delay before retry
            DeltaTime = TimeSpan.FromSeconds(1),
            // Maximum gap time for each delay time before retry
            MaxTimeInterval = TimeSpan.FromSeconds(20)
        };
    }
}
