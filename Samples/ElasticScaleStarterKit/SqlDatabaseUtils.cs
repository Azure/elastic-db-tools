// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading;

namespace ElasticScaleStarterKit;

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
        var connectionString =
            Configuration.GetConnectionString(
                Configuration.ShardMapManagerServerName,
                MasterDatabaseName);

        try
        {
            using var conn = new ReliableSqlConnection(
                connectionString,
                SqlRetryPolicy,
                SqlRetryPolicy);
            _ = conn.Open();

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
        using var conn = new ReliableSqlConnection(
            Configuration.GetConnectionString(server, MasterDatabaseName),
            SqlRetryPolicy,
            SqlRetryPolicy);
        _ = conn.Open();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from sys.databases where name = @dbname";
        _ = cmd.Parameters.AddWithValue("@dbname", db);
        cmd.CommandTimeout = 60;
        var count = conn.ExecuteCommand<int>(cmd);

        var exists = count > 0;
        return exists;
    }

    public static bool DatabaseIsOnline(string server, string db)
    {
        using var conn = new ReliableSqlConnection(
            Configuration.GetConnectionString(server, MasterDatabaseName),
            SqlRetryPolicy,
            SqlRetryPolicy);
        _ = conn.Open();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from sys.databases where name = @dbname and state = 0"; // online
        _ = cmd.Parameters.AddWithValue("@dbname", db);
        cmd.CommandTimeout = 60;
        var count = conn.ExecuteCommand<int>(cmd);

        var exists = count > 0;
        return exists;
    }

    public static void CreateDatabase(string server, string db)
    {
        ConsoleUtils.WriteInfo("Creating database {0}", db);
        using var conn = new ReliableSqlConnection(
            Configuration.GetConnectionString(server, MasterDatabaseName),
            SqlRetryPolicy,
            SqlRetryPolicy);
        _ = conn.Open();
        var cmd = conn.CreateCommand();

        // Determine if we are connecting to Azure SQL DB
        cmd.CommandText = "SELECT SERVERPROPERTY('EngineEdition')";
        cmd.CommandTimeout = 60;
        var engineEdition = conn.ExecuteCommand<int>(cmd);

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
                        _ = cmd.ExecuteNonQuery();
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
            _ = conn.ExecuteCommand(cmd);
        }
    }

    public static void DropDatabase(string server, string db)
    {
        ConsoleUtils.WriteInfo("Dropping database {0}", db);
        using var conn = new ReliableSqlConnection(
            Configuration.GetConnectionString(server, MasterDatabaseName),
            SqlRetryPolicy,
            SqlRetryPolicy);
        _ = conn.Open();
        var cmd = conn.CreateCommand();

        // Determine if we are connecting to Azure SQL DB
        cmd.CommandText = "SELECT SERVERPROPERTY('EngineEdition')";
        cmd.CommandTimeout = 60;
        var engineEdition = conn.ExecuteCommand<int>(cmd);

        // Drop the database
        if (engineEdition == 5)
        {
            // Azure SQL DB

            cmd.CommandText = string.Format("DROP DATABASE {0}", BracketEscapeName(db));
            _ = cmd.ExecuteNonQuery();
        }
        else
        {
            cmd.CommandText = string.Format(
                @"ALTER DATABASE {0} SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                        DROP DATABASE {0}",
                BracketEscapeName(db));
            _ = cmd.ExecuteNonQuery();
        }
    }

    public static void ExecuteSqlScript(string server, string db, string schemaFile)
    {
        ConsoleUtils.WriteInfo("Executing script {0}", schemaFile);
        using var conn = new ReliableSqlConnection(
            Configuration.GetConnectionString(server, db),
            SqlRetryPolicy,
            SqlRetryPolicy);
        _ = conn.Open();
        var cmd = conn.CreateCommand();

        // Read the commands from the sql script file
        var commands = ReadSqlScript(schemaFile);

        foreach (var command in commands)
        {
            cmd.CommandText = command;
            cmd.CommandTimeout = 60;
            _ = conn.ExecuteCommand(cmd);
        }
    }

    private static IEnumerable<string> ReadSqlScript(string scriptFile)
    {
        var commands = new List<string>();
        using (TextReader tr = new StreamReader(scriptFile))
        {
            var sb = new StringBuilder();
            string line;
            while ((line = tr.ReadLine()) != null)
            {
                if (line == "GO")
                {
                    commands.Add(sb.ToString());
                    _ = sb.Clear();
                }
                else
                {
                    _ = sb.AppendLine(line);
                }
            }
        }

        return commands;
    }

    /// <summary>
    /// Escapes a SQL object name with brackets to prevent SQL injection.
    /// </summary>
    private static string BracketEscapeName(string sqlName) => '[' + sqlName.Replace("]", "]]") + ']';

    /// <summary>
    /// Gets the retry policy to use for connections to SQL Server.
    /// </summary>
    public static RetryPolicy SqlRetryPolicy => new RetryPolicy<ExtendedSqlDatabaseTransientErrorDetectionStrategy>(10, TimeSpan.FromSeconds(5));

    /// <summary>
    /// Extended sql transient error detection strategy that performs additional transient error
    /// checks besides the ones done by the enterprise library.
    /// </summary>
    private class ExtendedSqlDatabaseTransientErrorDetectionStrategy : ITransientErrorDetectionStrategy
    {
        /// <summary>
        /// Enterprise transient error detection strategy.
        /// </summary>
        private readonly SqlDatabaseTransientErrorDetectionStrategy _sqltransientErrorDetectionStrategy = new();

        /// <summary>
        /// Checks with enterprise library's default handler to see if the error is transient, additionally checks
        /// for such errors using the code in the in <see cref="IsTransientException"/> function.
        /// </summary>
        /// <param name="ex">Exception being checked.</param>
        /// <returns><c>true</c> if exception is considered transient, <c>false</c> otherwise.</returns>
        public bool IsTransient(Exception ex) => _sqltransientErrorDetectionStrategy.IsTransient(ex) || IsTransientException(ex);

        /// <summary>
        /// Detects transient errors not currently considered as transient by the enterprise library's strategy.
        /// </summary>
        /// <param name="ex">Input exception.</param>
        /// <returns><c>true</c> if exception is considered transient, <c>false</c> otherwise.</returns>
        private static bool IsTransientException(Exception ex)
        {
            if (ex is SqlException se && se.InnerException != null)
            {
                if (se.InnerException is Win32Exception we)
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
