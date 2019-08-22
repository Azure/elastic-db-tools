// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Container for the credentials for SQL Server backed ShardMapManager.
    /// </summary>
    internal sealed class SqlShardMapManagerCredentials
    {
        /// <summary>
        /// Shard map manager data source
        /// </summary>
        private readonly string _smmDataSource;

        /// <summary>
        /// Shard map manager database
        /// </summary>
        private readonly string _smmInitialCatalog;

        /// <summary>
        /// Instantiates the object that holds the credentials for accessing SQL Servers
        /// containing the shard map manager data.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string for shard map manager data source.
        /// </param>
        public SqlShardMapManagerCredentials(string connectionString)
            : this(new SqlConnectionInfo(connectionString, null))
        {
        }

        /// <summary>
        /// Instantiates the object that holds the credentials for accessing SQL Servers
        /// containing the shard map manager data.
        /// </summary>
        /// <param name="connectionInfo">
        /// Connection info for shard map manager data source.
        /// </param>
        public SqlShardMapManagerCredentials(SqlConnectionInfo connectionInfo)
        {
            ExceptionUtils.DisallowNullArgument(connectionInfo, "connectionInfo");

            // Devnote: If connection string specifies Active Directory authentication and runtime is not
            // .NET 4.6 or higher, then below call will throw.
            SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(connectionInfo.ConnectionString);

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
                connectionInfo.Credential,
                connectionInfo.AccessTokenFactory);

            #endregion GSM Validation

            // Generate connectionInfoShardMapManager
            SqlConnectionStringBuilder connectionStringShardMapManager = new SqlConnectionStringBuilder(connectionStringBuilder.ConnectionString);

            connectionStringShardMapManager.ApplicationName = ApplicationNameHelper.AddApplicationNameSuffix(
                connectionStringShardMapManager.ApplicationName,
                GlobalConstants.ShardMapManagerInternalConnectionSuffixGlobal);

            _smmDataSource = connectionStringShardMapManager.DataSource;
            _smmInitialCatalog = connectionStringShardMapManager.InitialCatalog;

            this.ConnectionInfoShardMapManager =
                connectionInfo.CloneWithUpdatedConnectionString(connectionStringShardMapManager.ConnectionString);

            // Generate connectionInfoShard
            SqlConnectionStringBuilder connectionStringShard = new SqlConnectionStringBuilder(connectionStringBuilder.ConnectionString);

            connectionStringShard.Remove("Data Source");
            connectionStringShard.Remove("Initial Catalog");

            connectionStringShard.ApplicationName = ApplicationNameHelper.AddApplicationNameSuffix(
                connectionStringShard.ApplicationName,
                GlobalConstants.ShardMapManagerInternalConnectionSuffixLocal);

            this.ConnectionInfoShard =
                connectionInfo.CloneWithUpdatedConnectionString(connectionStringShard.ConnectionString);
        }

        /// <summary>
        /// Connection info for shard map manager database.
        /// </summary>
        public SqlConnectionInfo ConnectionInfoShardMapManager { get; private set; }

        /// <summary>
        /// Connection info for shards.
        /// </summary>
        public SqlConnectionInfo ConnectionInfoShard { get; private set; }

        /// <summary>
        /// Location of Shard Map Manager used for logging purpose.
        /// </summary>
        public string ShardMapManagerLocation
        {
            get
            {
                return StringUtils.FormatInvariant(
                "[DataSource={0} Database={1}]",
                _smmDataSource,
                _smmInitialCatalog);
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
        /// <param name="secureCredential">
        /// Input secure SQL credential object.
        /// </param>
        /// <param name="accessTokenFactory">
        /// The access token factory.
        /// </param>
        internal static void EnsureCredentials(
            SqlConnectionStringBuilder connectionString,
            string parameterName,
            SqlCredential secureCredential,
            Func<string> accessTokenFactory)
        {
            // Check for integrated authentication
            if (connectionString.IntegratedSecurity)
            {
                return;
            }

            // Check for active directory integrated authentication (if supported)
            if (connectionString.ContainsKey(ShardMapUtils.Authentication))
            {
                string authentication = connectionString[ShardMapUtils.Authentication].ToString();
                if (authentication.Equals(ShardMapUtils.ActiveDirectoryIntegratedStr, StringComparison.OrdinalIgnoreCase)
                    || authentication.Equals(ShardMapUtils.ActiveDirectoryInteractiveStr, StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
            }

            // If secure credential not specified, verify that user/pwd are in the connection string. If secure credential
            // specified, verify user/pwd are not in insecurely in the connection string.
            bool expectUserIdPasswordInConnectionString = secureCredential == null && accessTokenFactory == null;
            EnsureHasCredential(
                connectionString,
                parameterName,
                expectUserIdPasswordInConnectionString);
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
        /// <param name="expectUserIdPassword">
        /// True if <paramref name="connectionString"/> is expected to have user id & password, otherwise false.
        /// </param>
        private static void EnsureHasCredential(
            SqlConnectionStringBuilder connectionString,
            string parameterName,
            bool expectUserIdPassword)
        {
            if (expectUserIdPassword)
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
