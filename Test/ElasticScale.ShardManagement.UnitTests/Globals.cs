// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.SqlClient;
using System.Security;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    using System;

    /// <summary>
    /// Class that is container of global constants & methods.
    /// </summary>
    internal static class Globals
    {
        /// <summary>
        /// Connection string base for global shard map manager (auth option will be added)
        /// </summary>
        private const string ShardMapManagerConnStringBase =
            @"Data Source=" + Globals.ShardMapManagerTestsDatasourceName + ";Initial Catalog=ShardMapManager;";

        /// <summary>
        /// Connection string for global shard map manager for Integrated Auth
        /// </summary>
        private const string ShardMapManagerConnString = ShardMapManagerConnStringBase + "Integrated Security=SSPI;";

        /// <summary>
        /// Connection string for global shard map manager for Sql Auth
        /// </summary>
        private const string ShardMapManagerConnStringForSqlAuth = ShardMapManagerConnStringBase + "Integrated Security=False;";

        /// <summary>
        /// Connect string for local shard user.
        /// </summary>
        private const string ShardUserConnString = @"Integrated Security=SSPI;";

        /// <summary>
        /// Connect string for local shard user.
        /// </summary>
        private const string ShardUserConnStringForSqlAuth = @"User={0};Password={1}";

        /// <summary>
        /// shardMapManager datasource name for unit tests.
        /// </summary>
        internal const string ShardMapManagerTestsDatasourceName = "localhost";

        /// <summary>
        /// Postfix to be used to create all test databases and ShardMapManager database.
        /// </summary>
        internal static string TestDatabasePostfix = ""; // "-" + DateTime.Now.ToString("yyyyMMdd") + "-" + Guid.NewGuid().ToString();

        /// <summary>
        /// ShardMapManager database name.
        /// </summary>
        internal static string ShardMapManagerDatabaseName = "ShardMapManager" + Globals.TestDatabasePostfix;

        /// <summary>
        /// Connection string for connecting to test server.
        /// </summary>
        internal const string ShardMapManagerTestConnectionString = @"Data Source=" + Globals.ShardMapManagerTestsDatasourceName + ";Integrated Security=SSPI;";

        /// <summary>
        /// Query to create database.
        /// </summary>
        internal const string CreateDatabaseQuery =
            @"IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}') BEGIN ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{0}] END CREATE DATABASE [{0}]";

        /// <summary>
        /// Query to drop database.
        /// </summary>
        internal const string DropDatabaseQuery =
            @"IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}') DROP DATABASE [{0}]";

        /// <summary>
        /// Query to delete all rows from a table.
        /// </summary>
        internal const string CleanTableQuery =
            @"IF OBJECT_ID(N'{0}.{1}', N'U') IS NOT NULL DELETE FROM {0}.{1}";

        /// <summary>
        /// Test user to create for Sql Login tests.
        /// </summary>
        internal static string SqlLoginTestUser = "ElasticDatabaseToolsTestUser_" + System.Environment.CurrentManagedThreadId;

        /// <summary>
        /// Password for test user. (with ' and ; replaced with _ to enable test code to work without T/SQL and connection string escaping)
        /// </summary>
        internal static readonly string SqlLoginTestPassword = "TestPa$$w0rd" + Guid.NewGuid().ToString("N");

        /// <summary>
        /// SMM connection string.
        /// </summary>
        internal static string ShardMapManagerConnectionString
        {
            get
            {
                return Globals.ShardMapManagerConnString;
            }
        }

        /// <summary>
        /// SMM connection string.
        /// </summary>
        internal static string ShardMapManagerConnectionStringForSqlAuth => Globals.ShardMapManagerConnStringForSqlAuth;

        /// <summary>
        /// SMM shard connection string.
        /// </summary>
        internal static string ShardUserConnectionString => Globals.ShardUserConnString;

        /// <summary>
        /// SMM shard connection string.
        /// </summary>
        internal static string ShardUserConnectionStringForSqlAuth(string username) => 
            string.Format(Globals.ShardUserConnStringForSqlAuth, username, SqlLoginTestPassword);

        /// <summary>
        /// The shard user credential for sql auth.
        /// </summary>
        internal static SqlCredential ShardUserCredentialForSqlAuth(string username) => 
            new SqlCredential(username, GenerateSecureString(SqlLoginTestPassword));

        /// <summary>
        /// Generate a secure string
        /// </summary>
        /// <returns>
        /// The <see cref="SecureString"/>.
        /// </returns>
        private static SecureString GenerateSecureString(string text)
        {
            var secret = new SecureString();

            foreach (var character in text.ToCharArray())
            {
                secret.AppendChar(character);
            }

            secret.MakeReadOnly();

            return secret;
        }
    }
}
