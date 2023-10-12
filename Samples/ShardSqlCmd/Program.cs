// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Azure.SqlDatabase.ElasticScale.Query;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ShardSqlCmd
{
    internal static class Program
    {
        private static CommandLine s_commandLine;

        public static void Main(string[] args)
        {
            try
            {
                // Parse command line arguments
                s_commandLine = new CommandLine(args);
                if (!s_commandLine.IsValid)
                {
                    s_commandLine.WriteUsage();
                    return;
                }

                // Get Shard Map Manager
                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                    GetConnectionString(), ShardMapManagerLoadPolicy.Eager);
                Console.WriteLine("Connected to Shard Map Manager");

                // Get Shard Map
                ShardMap map = smm.GetShardMap(s_commandLine.ShardMap);
                Console.WriteLine("Found {0} shards", map.GetShards().Count());

                // Create connection string for MultiShardConnection
                string connectionString = GetCredentialsConnectionString();

                // REPL
                Console.WriteLine();
                while (true)
                {
                    // Read command from console
                    string commandText = GetCommand();
                    if (commandText == null)
                    {
                        // Exit requested
                        break;
                    }

                    // Evaluate command
                    string output;
                    using (MultiShardConnection conn = new MultiShardConnection(map.GetShards(), connectionString))
                    {
                        output = ExecuteCommand(conn, commandText);
                    }

                    // Print output
                    Console.WriteLine(output);
                }
            }
            catch (Exception e)
            {
                // Print exception and exit
                Console.WriteLine(e);
                return;
            }
        }

        /// <summary>
        /// Reads the next SQL command text from the console.
        /// </summary>
        private static string GetCommand()
        {
            StringBuilder sb = new StringBuilder();
            int lineNumber = 1;
            while (true)
            {
                Console.Write("{0}> ", lineNumber);

                string line = Console.ReadLine().Trim();

                switch (line.ToUpperInvariant())
                {
                    case "GO":
                        if (sb.Length == 0)
                        {
                            // "go" with empty command - reset line number
                            lineNumber = 1;
                        }
                        else
                        {
                            return sb.ToString();
                        }

                        break;

                    case "EXIT":
                        return null;

                    default:
                        if (!string.IsNullOrWhiteSpace(line))
                        {
                            sb.AppendLine(line);
                        }

                        lineNumber++;
                        break;
                }
            }
        }

        /// <summary>
        /// Executes the SQL command and returns the output in text format.
        /// </summary>
        private static string ExecuteCommand(MultiShardConnection conn, string commandText)
        {
            try
            {
                StringBuilder output = new StringBuilder();
                output.AppendLine();

                int rowsAffected = 0;

                using (MultiShardCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = commandText;
                    cmd.CommandTimeout = s_commandLine.QueryTimeout;
                    cmd.CommandTimeoutPerShard = s_commandLine.QueryTimeout;

                    // Execute command and time with a stopwatch
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    cmd.ExecutionPolicy = s_commandLine.ExecutionPolicy;
                    cmd.ExecutionOptions = s_commandLine.ExecutionOptions;
                    using (MultiShardDataReader reader = cmd.ExecuteReader(CommandBehavior.Default))
                    {
                        stopwatch.Stop();

                        // Get column names
                        IEnumerable<string> columnNames = GetColumnNames(reader).ToArray();

                        // Create table formatter
                        TableFormatter tableFormatter = new TableFormatter(columnNames.ToArray());

                        // Read results from db
                        while (reader.Read())
                        {
                            rowsAffected++;

                            // Add the row to the table formatter
                            object[] values = new object[reader.FieldCount];
                            reader.GetValues(values);
                            tableFormatter.AddRow(values);
                        }

                        // Write formatter output
                        output.AppendLine(tableFormatter.ToString());
                    }

                    output.AppendLine();
                    output.AppendFormat("({0} rows affected - {1:hh}:{1:mm}:{1:ss} elapsed)", rowsAffected, stopwatch.Elapsed);
                    output.AppendLine();
                }

                return output.ToString();
            }
            catch (MultiShardAggregateException e)
            {
                return e.ToString();
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

        #region Command line parsing

        private class CommandLine
        {
            // Values that are read from the command line
            public string UserName { get; private set; }

            public string Password { get; private set; }

            public string ServerName { get; private set; }

            public string DatabaseName { get; private set; }

            public string ShardMap { get; private set; }

            public bool UseTrustedConnection { get; private set; }

            public MultiShardExecutionPolicy ExecutionPolicy { get; private set; }

            public MultiShardExecutionOptions ExecutionOptions { get; private set; }

            public int QueryTimeout { get; private set; }

            /// <summary>
            /// Gets a value indicating whether the command line is valid, i.e. parsing it succeeded.
            /// </summary>
            public bool IsValid
            {
                get
                {
                    // Verify that a correct combination of parameters were provided
                    return this.ServerName != null &&
                           this.DatabaseName != null &&
                           this.ShardMap != null &&
                           (this.UseTrustedConnection ||
                            (this.UserName != null && this.Password != null)) &&
                            !_parseErrors;
                }
            }

            /// <summary>
            /// True if there were any errors while parsing.
            /// </summary>
            private bool _parseErrors = false;

            /// <summary>
            /// Initializes a new instance of the <see cref="CommandLine" /> class and parses the provided arguments.
            /// </summary>
            public CommandLine(string[] args)
            {
                // Default values
                this.QueryTimeout = 60;
                this.ExecutionPolicy = MultiShardExecutionPolicy.CompleteResults;
                this.ExecutionOptions = MultiShardExecutionOptions.None;

                _args = args;
                this.ParseInternal();
            }

            // Parsing state variables
            private readonly string[] _args;
            private int _parseIndex;

            /// <summary>
            /// Parses the given command line. Returns true for success.
            /// </summary>
            private void ParseInternal()
            {
                _parseIndex = 0;

                string arg;
                while ((arg = this.GetNextArg()) != null)
                {
                    switch (arg)
                    {
                        case "-S": // Server
                            this.ServerName = this.GetNextArg();
                            break;

                        case "-d": // Shard Map Manager database
                            this.DatabaseName = this.GetNextArg();
                            break;

                        case "-sm": // Shard map
                            this.ShardMap = this.GetNextArg();
                            break;

                        case "-U": // User name
                            this.UserName = this.GetNextArg();
                            break;

                        case "-P": // Password
                            this.Password = this.GetNextArg();
                            break;

                        case "-E": // Use trusted connection (aka Windows Authentication)
                            this.UseTrustedConnection = true;
                            break;

                        case "-t": // Query timeout
                            string queryTimeoutString = this.GetNextArg();
                            if (queryTimeoutString != null)
                            {
                                int parsedQueryTimeout;
                                bool parseSuccess = int.TryParse(queryTimeoutString, out parsedQueryTimeout);
                                if (parseSuccess)
                                {
                                    this.QueryTimeout = parsedQueryTimeout;
                                }
                                else
                                {
                                    _parseErrors = true;
                                }
                            }

                            break;

                        case "-pr": // Partial results
                            this.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
                            break;

                        case "-sn": // $ShardName column
                            this.ExecutionOptions |= MultiShardExecutionOptions.IncludeShardNameColumn;
                            break;
                    }
                }
            }

            /// <summary>
            /// Returns the next argument, if it exists, and advances the index. Helper method for ParseInternal.
            /// </summary>
            private string GetNextArg()
            {
                string value = null;
                if (_parseIndex < _args.Length)
                {
                    value = _args[_parseIndex];
                }

                _parseIndex++;
                return value;
            }

            /// <summary>
            /// Writes command line usage information to the console.
            /// </summary>
            public void WriteUsage()
            {
                Console.WriteLine(@"
Usage: 

ShardSqlCmd.exe
        -S  server
        -d  shard map manager database
        -sm shard map
        -U  login id
        -P  password
        -E  trusted connection
        -t  query timeout
        -pr PartialResults mode
        -sn include $ShardName column in results

  e.g.  ShardSqlCmd.exe -S myserver -d myshardmapmanagerdb -sm myshardmap -E
        ShardSqlCmd.exe -S myserver -d myshardmapmanagerdb -sm myshardmap -U mylogin -P mypasword
        ShardSqlCmd.exe -S myserver -d myshardmapmanagerdb -sm myshardmap -U mylogin -P mypasword -pr -sn
");
            }
        }

        #endregion

        #region Creating connection strings

        /// <summary>
        /// Returns a connection string that can be used to connect to the specified server and database.
        /// </summary>
        public static string GetConnectionString()
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());
            connStr.DataSource = s_commandLine.ServerName;
            connStr.InitialCatalog = s_commandLine.DatabaseName;
            return connStr.ToString();
        }

        /// <summary>
        /// Returns a connection string containing just the credentials (i.e. UserID, Password, and IntegratedSecurity) to use for DDR and MSQ.
        /// </summary>
        public static string GetCredentialsConnectionString()
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder
            {
                ApplicationName = "ESC_CMDv1.0",
                UserID = s_commandLine.UserName ?? string.Empty,
                Password = s_commandLine.Password ?? string.Empty,
                IntegratedSecurity = s_commandLine.UseTrustedConnection
            };
            return connStr.ToString();
        }

        #endregion
    }
}