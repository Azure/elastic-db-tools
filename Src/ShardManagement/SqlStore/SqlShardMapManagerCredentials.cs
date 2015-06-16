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
        private SqlConnectionStringBuilder connectionStringShardMapManager;

        /// <summary>
        /// Connection string for individual shards.
        /// </summary>
        private SqlConnectionStringBuilder connectionStringShard;

        /// <summary>
        /// Instantiates the object that holds the credentials for accessing SQL Servers 
        /// containing the shard map manager data.
        /// </summary>
        /// <param name="connectionString">Connection string for Shard map manager data source.</param>
        public SqlShardMapManagerCredentials(string connectionString)
        {
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");


            SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);

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
            SqlShardMapManagerCredentials.EnsureCredentials(connectionStringBuilder, "connectionString");

            #endregion GSM Validation

            // Copy the input connection strings.
            this.connectionStringShardMapManager = new SqlConnectionStringBuilder(connectionStringBuilder.ConnectionString);

            this.connectionStringShardMapManager.ApplicationName = ApplicationNameHelper.AddApplicationNameSuffix(
                this.connectionStringShardMapManager.ApplicationName,
                GlobalConstants.ShardMapManagerInternalConnectionSuffixGlobal);

            this.connectionStringShard = new SqlConnectionStringBuilder(connectionStringBuilder.ConnectionString);

            connectionStringShard.Remove("Data Source");
            connectionStringShard.Remove("Initial Catalog");

            this.connectionStringShard.ApplicationName = ApplicationNameHelper.AddApplicationNameSuffix(
                this.connectionStringShard.ApplicationName,
                GlobalConstants.ShardMapManagerInternalConnectionSuffixLocal);
        }

        /// <summary>
        /// Connection string for shard map manager database.
        /// </summary>
        public string ConnectionStringShardMapManager
        {
            get
            {
                return this.connectionStringShardMapManager.ConnectionString;
            }
        }

        /// <summary>
        /// Connection string for shards.
        /// </summary>
        public string ConnectionStringShard
        {
            get
            {
                return this.connectionStringShard.ConnectionString;
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
                this.connectionStringShardMapManager.DataSource,
                this.connectionStringShardMapManager.InitialCatalog);
            }
        }

        /// <summary>
        /// Ensures that credentials are provided for the given connection string object.
        /// </summary>
        /// <param name="connectionString">Input connection string object.</param>
        /// <param name="parameterName">Parameter name of the connection string object.</param>
        internal static void EnsureCredentials(SqlConnectionStringBuilder connectionString, string parameterName)
        {
            // Check for integrated authentication.
            if (!connectionString.IntegratedSecurity)
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
        }

    }
}
