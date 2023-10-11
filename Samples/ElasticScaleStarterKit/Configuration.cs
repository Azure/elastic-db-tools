// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Configuration;
using System.Data.SqlClient;

namespace ElasticScaleStarterKit;

/// <summary>
/// Provides access to app.config settings, and contains advanced configuration settings.
/// </summary>
internal static class Configuration
{
    /// <summary>
    /// Gets the server name for the Shard Map Manager database, which contains the shard maps.
    /// </summary>
    public static string ShardMapManagerServerName => ServerName;

    /// <summary>
    /// Gets the database name for the Shard Map Manager database, which contains the shard maps.
    /// </summary>
    public static string ShardMapManagerDatabaseName => "ElasticScaleStarterKit_ShardMapManagerDb";

    /// <summary>
    /// Gets the name for the Shard Map that contains metadata for all the shards and the mappings to those shards.
    /// </summary>
    public static string ShardMapName => "CustomerIDShardMap";

    /// <summary>
    /// Gets the server name from the App.config file for shards to be created on.
    /// </summary>
    private static string ServerName => ConfigurationManager.AppSettings["ServerName"];

    /// <summary>
    /// Gets the edition to use for Shards and Shard Map Manager Database if the server is an Azure SQL DB server. 
    /// If the server is a regular SQL Server then this is ignored.
    /// </summary>
    public static string DatabaseEdition => ConfigurationManager.AppSettings["DatabaseEdition"];

    /// <summary>
    /// Returns a connection string that can be used to connect to the specified server and database.
    /// </summary>
    public static string GetConnectionString(string serverName, string database)
    {
        var connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString())
        {
            DataSource = serverName,
            InitialCatalog = database
        };
        return connStr.ToString();
    }

    /// <summary>
    /// Returns a connection string to use for Data-Dependent Routing and Multi-Shard Query,
    /// which does not contain DataSource or InitialCatalog.
    /// </summary>
    public static string GetCredentialsConnectionString()
    {
        // Get User name and password from the app.config file. If they don't exist, default to string.Empty.
        var userId = ConfigurationManager.AppSettings["UserName"] ?? string.Empty;
        var password = ConfigurationManager.AppSettings["Password"] ?? string.Empty;

        // Get Integrated Security from the app.config file. 
        // If it exists, then parse it (throw exception on failure), otherwise default to false.
        var integratedSecurityString = ConfigurationManager.AppSettings["IntegratedSecurity"];
        var integratedSecurity = integratedSecurityString != null && bool.Parse(integratedSecurityString);

        var connStr = new SqlConnectionStringBuilder
        {
            // DDR and MSQ require credentials to be set
            UserID = userId,
            Password = password,
            IntegratedSecurity = integratedSecurity,

            // DataSource and InitialCatalog cannot be set for DDR and MSQ APIs, because these APIs will
            // determine the DataSource and InitialCatalog for you.
            //
            // DDR also does not support the ConnectRetryCount keyword introduced in .NET 4.5.1, because it
            // would prevent the API from being able to correctly kill connections when mappings are switched
            // offline.
            //
            // Other SqlClient ConnectionString keywords are supported.

            ApplicationName = "ESC_SKv1.0",
            ConnectTimeout = 30
        };
        return connStr.ToString();
    }
}
