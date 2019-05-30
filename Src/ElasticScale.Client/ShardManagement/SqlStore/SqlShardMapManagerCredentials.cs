// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.SqlStore;
using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Container for the credentials for SQL Server backed ShardMapManager.
    /// </summary>
    internal sealed class SqlShardMapManagerCredentials
    {
        /// <summary>
        /// Connection string for shard map manager database.
        /// </summary>
        private SqlConnectionStringBuilder _connectionStringShardMapManager;

        /// <summary>
        /// Connection string for individual shards.
        /// </summary>
        private SqlConnectionStringBuilder _connectionStringShard;

        /// <summary>
        /// Secure credential for shard map manager data source.
        /// </summary>
        private SqlCredential _secureCredential;

        /// <summary>
        /// Secure access token for shard map manager data source.
        /// </summary>
        private string _accessToken;

        /// <summary>
        /// Instantiates the object that holds the credentials for accessing SQL Servers 
        /// containing the shard map manager data.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string for shard map manager data source.
        /// </param>
        public SqlShardMapManagerCredentials(string connectionString)
            : this(connectionString, null)
        {
        }

        /// <summary>
        /// Instantiates the object that holds the credentials for accessing SQL Servers 
        /// containing the shard map manager data.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string for shard map manager data source.
        /// </param>
        public SqlShardMapManagerCredentials(string connectionString, SqlCredential secureCredential)
        {
            this._secureCredential = secureCredential;
            SqlConnectionInfo sqlConnectionInfo = new SqlConnectionInfo(connectionString, secureCredential, null);
            setUpSqlShardMapManagerCredentials(sqlConnectionInfo);
        }
        /// <summary>
        /// Instantiates the object that holds the credentials for accessing SQL Servers 
        /// containing the shard map manager data.
        /// </summary>
        /// <param name="sqlConnectionInfo">Sql Connection Information</param>
        public SqlShardMapManagerCredentials(SqlConnectionInfo sqlConnectionInfo)
        {
            Debug.Assert(sqlConnectionInfo != null);
            setUpSqlShardMapManagerCredentials(sqlConnectionInfo);
        }

        /// <summary>
        /// Instantiates the object that holds the credentials for accessing SQL Servers 
        /// containing the shard map manager data.
        /// </summary>
        /// <param name="sqlConnectionInfo">Sql Connection Information</param>
        private void setUpSqlShardMapManagerCredentials(SqlConnectionInfo sqlConnectionInfo)
        {
            ExceptionUtils.DisallowNullArgument(sqlConnectionInfo, "sqlConnectionInfo");
            ExceptionUtils.DisallowNullArgument(sqlConnectionInfo.ConnectionString, "connectionString");

            // Devnote: If connection string specifies Active Directory authentication and runtime is not
            // .NET 4.6 or higher, then below call will throw.
            SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(sqlConnectionInfo.ConnectionString);

            #region GSM Validation

            // DataSource must be set.
            if (string.IsNullOrEmpty(connectionStringBuilder.DataSource))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired,
                        "DataSource"),
                    "connectionString");
            }

            // InitialCatalog must be set.
            if (string.IsNullOrEmpty(connectionStringBuilder.InitialCatalog))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired,
                        "Initial Catalog"),
                    "connectionString");
            }

            // Ensure credentials are specified for GSM connectivity.
            SqlShardMapManagerCredentials.EnsureCredentials(
                connectionStringBuilder,
                "connectionString",
                sqlConnectionInfo);

            #endregion GSM Validation

            this._accessToken = sqlConnectionInfo.AccessToken;
            this._secureCredential = sqlConnectionInfo.SecureCredential;
            // Copy the input connection strings.
            _connectionStringShardMapManager = new SqlConnectionStringBuilder(connectionStringBuilder.ConnectionString);

            _connectionStringShardMapManager.ApplicationName = ApplicationNameHelper.AddApplicationNameSuffix(
                _connectionStringShardMapManager.ApplicationName,
                GlobalConstants.ShardMapManagerInternalConnectionSuffixGlobal);

            _connectionStringShard = new SqlConnectionStringBuilder(connectionStringBuilder.ConnectionString);

            _connectionStringShard.Remove("Data Source");
            _connectionStringShard.Remove("Initial Catalog");

            _connectionStringShard.ApplicationName = ApplicationNameHelper.AddApplicationNameSuffix(
                _connectionStringShard.ApplicationName,
                GlobalConstants.ShardMapManagerInternalConnectionSuffixLocal);
        }

        /// <summary>
        /// Connection string for shard map manager database.
        /// </summary>
        public string ConnectionStringShardMapManager
        {
            get
            {
                return _connectionStringShardMapManager.ConnectionString;
            }
        }

        /// <summary>
        /// Secure Credential for shard map manager database.
        /// </summary>
        public SqlCredential SecureCredentialShardMapManager => this._secureCredential;


        public string accessTokenShardMapManager => this._accessToken;

        /// <summary>
        /// Connection string for shards.
        /// </summary>
        public string ConnectionStringShard
        {
            get
            {
                return _connectionStringShard.ConnectionString;
            }
        }

        /// <summary>
        /// Location of Shard Map Manager used for logging purpose.
        /// </summary>
        public string ShardMapManagerLocation
        {
            get
            {
                return StringUtils.FormatInvariant(
                "[DataSource={0} Database={1}]",
                _connectionStringShardMapManager.DataSource,
                _connectionStringShardMapManager.InitialCatalog);
            }
        }

        /// <summary>
        /// Ensures that credentials are provided for the given connection string object.
        /// </summary>
        /// <param name="connectionString">
        /// Input connection string object.
        /// </param>
        /// <param name="parameterName">
        /// Parameter name of the connection string object.
        /// </param>
        /// <param name="sqlConnectionInfo">Sql Connection Information</param>
        internal static void EnsureCredentials(SqlConnectionStringBuilder connectionString, string parameterName, SqlConnectionInfo sqlConnectionInfo)
        {
            // Check for integrated authentication
            if (connectionString.IntegratedSecurity)
            {
                return;
            }

            // Check for active directory integrated authentication (if supported)
            if (connectionString.ContainsKey(ShardMapUtils.Authentication) &&
                connectionString[ShardMapUtils.Authentication].ToString().Equals(ShardMapUtils.ActiveDirectoryIntegratedStr))
            {
                return;
            }
            //check if access token is not null, if the access token is present then there is no need of user id/password
            if (sqlConnectionInfo.AccessToken != null)
            {
                // UserID must NOT be set when a access token is provided.
                if (!string.IsNullOrEmpty(connectionString.UserID))
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyNotAllowed,
                            "UserID"),
                        parameterName);
                }

                // Password must NOT be set when access token is provided.
                if (!string.IsNullOrEmpty(connectionString.Password))
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyNotAllowed,
                            "Password"),
                        parameterName);
                }
                if (sqlConnectionInfo.SecureCredential != null)
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyNotAllowed,
                            "Secure credential"),
                        parameterName);
                }
                return;
            }

            // If secure credential not specified, verify that user/pwd are in the connection string. If secure credential
            // specified, verify user/pwd are not in insecurely in the connection string.
            if (sqlConnectionInfo.SecureCredential == null)
            {
                // UserID must be set when integrated authentication is disabled.
                if (string.IsNullOrEmpty(connectionString.UserID))
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired,
                            "UserID"),
                        parameterName);
                }

                // Password must be set when integrated authentication is disabled.
                if (string.IsNullOrEmpty(connectionString.Password))
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyRequired,
                            "Password"),
                        parameterName);
                }
            }
            else
            {
                // UserID must NOT be set when a secure SQL credential is provided.
                if (!string.IsNullOrEmpty(connectionString.UserID))
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyNotAllowed,
                            "UserID"),
                        parameterName);
                }

                // Password must NOT be set when a secure SQL credential is provided.
                if (!string.IsNullOrEmpty(connectionString.Password))
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._SqlShardMapManagerCredentials_ConnectionStringPropertyNotAllowed,
                            "Password"),
                        parameterName);
                }
            }
        }
    }
}
