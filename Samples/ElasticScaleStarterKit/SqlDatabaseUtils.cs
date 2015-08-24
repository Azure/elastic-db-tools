// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
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
                using (ReliableSqlConnection conn = new ReliableSqlConnection(
                    connectionString,
                    SqlRetryPolicy,
                    SqlRetryPolicy))
                {
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
            using (ReliableSqlConnection conn = new ReliableSqlConnection(
                Configuration.GetConnectionString(server, MasterDatabaseName),
                SqlRetryPolicy,
                SqlRetryPolicy))
            {
                conn.Open();

                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(*) from sys.databases where name = @dbname";
                cmd.Parameters.AddWithValue("@dbname", db);
                cmd.CommandTimeout = 60;
                int count = conn.ExecuteCommand<int>(cmd);

                bool exists = count > 0;
                return exists;
            }
        }

        public static bool DatabaseIsOnline(string server, string db)
        {
            using (ReliableSqlConnection conn = new ReliableSqlConnection(
                Configuration.GetConnectionString(server, MasterDatabaseName),
                SqlRetryPolicy,
                SqlRetryPolicy))
            {
                conn.Open();

                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(*) from sys.databases where name = @dbname and state = 0"; // online
                cmd.Parameters.AddWithValue("@dbname", db);
                cmd.CommandTimeout = 60;
                int count = conn.ExecuteCommand<int>(cmd);

                bool exists = count > 0;
                return exists;
            }
        }

        public static void CreateDatabase(string server, string db)
        {
            ConsoleUtils.WriteInfo("Creating database {0}", db);
            using (ReliableSqlConnection conn = new ReliableSqlConnection(
                Configuration.GetConnectionString(server, MasterDatabaseName),
                SqlRetryPolicy,
                SqlRetryPolicy))
            {
                conn.Open();
                SqlCommand cmd = conn.CreateCommand();

                // Determine if we are connecting to Azure SQL DB
                cmd.CommandText = "SELECT SERVERPROPERTY('EngineEdition')";
                cmd.CommandTimeout = 60;
                int engineEdition = conn.ExecuteCommand<int>(cmd);

                if (engineEdition == 5)
                {
                    // Azure SQL DB
                    SqlRetryPolicy.ExecuteAction(() =>
                        {
                            if (!DatabaseExists(server, db))
                            {
                                // Begin creation (which is async for Standard/Premium editions)
                                cmd.CommandText = string.Format(
                                    "CREATE DATABASE {0} (EDITION = '{1}')",
                                    BracketEscapeName(db),
                                    Configuration.DatabaseEdition);
                                cmd.CommandTimeout = 60;
                                cmd.ExecuteNonQuery();
                            }
                        });

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
                    conn.ExecuteCommand(cmd);
                }
            }
        }

        public static void DropDatabase(string server, string db)
        {
            ConsoleUtils.WriteInfo("Dropping database {0}", db);
            using (ReliableSqlConnection conn = new ReliableSqlConnection(
                Configuration.GetConnectionString(server, MasterDatabaseName),
                SqlRetryPolicy,
                SqlRetryPolicy))
            {
                conn.Open();
                SqlCommand cmd = conn.CreateCommand();

                // Determine if we are connecting to Azure SQL DB
                cmd.CommandText = "SELECT SERVERPROPERTY('EngineEdition')";
                cmd.CommandTimeout = 60;
                int engineEdition = conn.ExecuteCommand<int>(cmd);

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
            using (ReliableSqlConnection conn = new ReliableSqlConnection(
                Configuration.GetConnectionString(server, db),
                SqlRetryPolicy,
                SqlRetryPolicy))
            {
                conn.Open();
                SqlCommand cmd = conn.CreateCommand();

                // Read the commands from the sql script file
                IEnumerable<string> commands = ReadSqlScript(schemaFile);

                foreach (string command in commands)
                {
                    cmd.CommandText = command;
                    cmd.CommandTimeout = 60;
                    conn.ExecuteCommand(cmd);
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

        /// <summary>
        /// Gets the retry policy to use for connections to SQL Server.
        /// </summary>
        public static RetryPolicy SqlRetryPolicy
        {
            get
            {
                return new RetryPolicy<ExtendedSqlDatabaseTransientErrorDetectionStrategy>(10, TimeSpan.FromSeconds(5));
            }
        }

        /// <summary>
        /// Extended sql transient error detection strategy that performs additional transient error
        /// checks besides the ones done by the enterprise library.
        /// </summary>
        private class ExtendedSqlDatabaseTransientErrorDetectionStrategy : ITransientErrorDetectionStrategy
        {
            /// <summary>
            /// Enterprise transient error detection strategy.
            /// </summary>
            private SqlDatabaseTransientErrorDetectionStrategy _sqltransientErrorDetectionStrategy = new SqlDatabaseTransientErrorDetectionStrategy();

            /// <summary>
            /// Checks with enterprise library's default handler to see if the error is transient, additionally checks
            /// for such errors using the code in the in <see cref="IsTransientException"/> function.
            /// </summary>
            /// <param name="ex">Exception being checked.</param>
            /// <returns><c>true</c> if exception is considered transient, <c>false</c> otherwise.</returns>
            public bool IsTransient(Exception ex)
            {
                return _sqltransientErrorDetectionStrategy.IsTransient(ex) || IsTransientException(ex);
            }

            /// <summary>
            /// Detects transient errors not currently considered as transient by the enterprise library's strategy.
            /// </summary>
            /// <param name="ex">Input exception.</param>
            /// <returns><c>true</c> if exception is considered transient, <c>false</c> otherwise.</returns>
            private static bool IsTransientException(Exception ex)
            {
                SqlException se = ex as SqlException;

                if (se != null && se.InnerException != null)
                {
                    Win32Exception we = se.InnerException as Win32Exception;

                    if (we != null)
                    {
                        switch (we.NativeErrorCode)
                        {
                            case 0x102:
                                // Transient wait expired error resulting in timeout
                                return true;
                            case 0x121:
                                // Transient semaphore wait expired error resulting in timeout
                                return true;
                        }
                    }
                }

                return false;
            }
        }
    }
}
