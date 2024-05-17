// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Configuration;
using Microsoft.Data.SqlClient;

namespace ElasticScaleStarterKit
{
    /// <summary>
    /// Provides access to app.config settings, and contains advanced configuration settings.
    /// </summary>
    internal static class Configuration
    {
        /// <summary>
        /// Gets the server name for the Shard Map Manager database, which contains the shard maps.
        /// </summary>
        public static string ShardMapManagerServerName
        {
            get { return ServerName; }
        }

        /// <summary>
        /// Gets the database name for the Shard Map Manager database, which contains the shard maps.
        /// </summary>
        public static string ShardMapManagerDatabaseName
        {
            get { return "ElasticScaleStarterKit_ShardMapManagerDb"; }
        }

        /// <summary>
        /// Gets the name for the Shard Map that contains metadata for all the shards and the mappings to those shards.
        /// </summary>
        public static string ShardMapName
        {
            get { return "CustomerIDShardMap"; }
        }

        /// <summary>
        /// Gets the server name from the App.config file for shards to be created on.
        /// </summary>
        private static string ServerName
        {
            get { return ConfigurationManager.AppSettings["ServerName"]; }
        }

        /// <summary>
        /// Gets the edition to use for Shards and Shard Map Manager Database if the server is an Azure SQL DB server. 
        /// If the server is a regular SQL Server then this is ignored.
        /// </summary>
        public static string DatabaseEdition
        {
            get
            {
                return ConfigurationManager.AppSettings["DatabaseEdition"];
            }
        }

        /// <summary>
        /// Returns a connection string that can be used to connect to the specified server and database.
        /// </summary>
        public static string GetConnectionString(string serverName, string database)
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());
            connStr.DataSource = serverName;
            connStr.InitialCatalog = database;
            return connStr.ToString();
        }

        /// <summary>
        /// Returns a connection string to use for Data-Dependent Routing and Multi-Shard Query,
        /// which does not contain DataSource or InitialCatalog.
        /// </summary>
        public static string GetCredentialsConnectionString()
        {
            // Get User name and password from the app.config file. If they don't exist, default to string.Empty.
            string userId = ConfigurationManager.AppSettings["UserName"] ?? string.Empty;
            string password = ConfigurationManager.AppSettings["Password"] ?? string.Empty;

            string trustServerCertificateString = ConfigurationManager.AppSettings["TrustServerCertificate"] ?? string.Empty;

            var trustServerCertificate = trustServerCertificateString != null && bool.Parse(trustServerCertificateString);

            // Get Sql Auth method from the app.config file. 
            SqlAuthenticationMethod authMethod;
            var enumString = ConfigurationManager.AppSettings["SqlAuthenticationMethod"];
            if (!Enum.TryParse(enumString, out authMethod))
            {
                throw new ArgumentException("Invalid SqlAuthenticationMethod in app.config");
            }

            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder
            {
                Authentication = authMethod,

                // DataSource and InitialCatalog cannot be set for DDR and MSQ APIs, because these APIs will
                // determine the DataSource and InitialCatalog for you.
                //
                // DDR also does not support the ConnectRetryCount keyword introduced in .NET 4.5.1, because it
                // would prevent the API from being able to correctly kill connections when mappings are switched
                // offline.
                //
                // Other SqlClient ConnectionString keywords are supported.

                TrustServerCertificate = trustServerCertificate,

                ApplicationName = "ESC_SKv1.0",

                // Set to 120 if ActiveDirectoryDeviceCodeFlow
                // not even the fastest cut and pasters can get the device code
                // into the browser and click through in 30 seconds.
                ConnectTimeout = authMethod == SqlAuthenticationMethod.ActiveDirectoryDeviceCodeFlow ? 120 : 30,
            };

            // DEVNOTE: NotSpecified behaves the same as SqlPassword (i.e. Sql Auth)
            if (authMethod == SqlAuthenticationMethod.ActiveDirectoryManagedIdentity ||
                authMethod == SqlAuthenticationMethod.ActiveDirectoryMSI ||
                authMethod == SqlAuthenticationMethod.ActiveDirectoryServicePrincipal ||
                authMethod == SqlAuthenticationMethod.ActiveDirectoryPassword ||
                authMethod == SqlAuthenticationMethod.SqlPassword ||
                authMethod == SqlAuthenticationMethod.NotSpecified)
            {
                // DDR and MSQ require credentials to be set

                // ActiveDirectoryManagedIdentity / ActiveDirectoryMSI when using a System Managed System Identify does not use a UserID 
                if (authMethod != SqlAuthenticationMethod.ActiveDirectoryManagedIdentity &&
                    authMethod != SqlAuthenticationMethod.ActiveDirectoryMSI)
                {
                    if (userId == string.Empty)
                    {
                        throw new ArgumentException("UserName must be specified in app.config");
                    }
                }

                connStr.UserID = userId;

                // ActiveDirectoryManagedIdentity/ActiveDirectoryMSI does not use a Password.
                if (authMethod != SqlAuthenticationMethod.ActiveDirectoryManagedIdentity &&
                    authMethod != SqlAuthenticationMethod.ActiveDirectoryMSI)
                {
                    if (password == string.Empty)
                    {
                        throw new ArgumentException("Password must be specified in app.config");
                    }

                    connStr.Password = password;
                }
            }

            return connStr.ToString();
        }
    }
}