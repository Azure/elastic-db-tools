// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Class that is container of global constants & methods.
    /// </summary>
    internal static class Globals
    {
        /// <summary>
        /// Connection string for global shard map manager.
        /// </summary>
        private const string ShardMapManagerConnString = @"Data Source=" + Globals.ShardMapManagerTestsDatasourceName + ";Initial Catalog=ShardMapManager;Integrated Security=SSPI;";

        /// <summary>
        /// Connect string for local shard user.
        /// </summary>
        private const string ShardUserConnString = @"Integrated Security=SSPI;";

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
        internal const string ShardMapManagerTestConnectionString = @"Data Source=" + Globals.ShardMapManagerTestsDatasourceName + ";Integrated Security=True;";

        /// <summary>
        /// Query to create database.
        /// </summary>
        internal const string CreateDatabaseQuery =
            @"IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}') BEGIN DROP DATABASE [{0}] END CREATE DATABASE [{0}]";

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
        /// SMM shard connection string.
        /// </summary>
        internal static string ShardUserConnectionString
        {
            get
            {
                return Globals.ShardUserConnString;
            }
        }
    }
}
