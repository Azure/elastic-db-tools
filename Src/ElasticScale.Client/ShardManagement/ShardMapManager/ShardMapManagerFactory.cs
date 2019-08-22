// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Describes the policy used for initialization of <see cref="ShardMapManager"/> from the store.
    /// </summary>
    public enum ShardMapManagerLoadPolicy
    {
        /// <summary>
        /// Load all shard maps and their corresponding
        /// mappings into the cache for fast retrieval.
        /// </summary>
        Eager,

        /// <summary>
        /// Load all shard maps and their corresponding
        /// mappings on as needed basis.
        /// </summary>
        Lazy
    }

    /// <summary>
    /// Describes the creation options for shard map manager storage representation.
    /// </summary>
    public enum ShardMapManagerCreateMode
    {
        /// <summary>
        /// If the shard map manager data structures are already present
        /// in the store, then this method will raise exception.
        /// </summary>
        KeepExisting,

        /// <summary>
        /// If the shard map manager data structures are already present
        /// in the store, then this method will overwrite them.
        /// </summary>
        ReplaceExisting
    }

    /// <summary>
    /// Factory for <see cref="ShardMapManager"/>s facilitates the creation and management
    /// of shard map manager persistent state. Use this class as the entry point to the library's
    /// object hierarchy.
    /// </summary>
    public static class ShardMapManagerFactory
    {
        /// <summary>
        /// The Tracer
        /// </summary>
        private static ILogger Tracer
        {
            get
            {
                return TraceHelper.Tracer;
            }
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
        /// with <see cref="ShardMapManagerCreateMode.KeepExisting"/> and <see cref="RetryBehavior.DefaultRetryBehavior"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardMapManager CreateSqlShardMapManager(
            string connectionString)
        {
            return CreateSqlShardMapManager(
                connectionString,
                secureCredential: null);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
        /// with <see cref="ShardMapManagerCreateMode.KeepExisting"/> and <see cref="RetryBehavior.DefaultRetryBehavior"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <param name="secureCredential">Secure credential used for creating shard map manager database.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardMapManager CreateSqlShardMapManager(
            string connectionString,
            SqlCredential secureCredential)
        {
            return CreateSqlShardMapManager(
                connectionString,
                secureCredential,
                ShardMapManagerCreateMode.KeepExisting,
                RetryBehavior.DefaultRetryBehavior);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
        /// with <see cref="RetryBehavior.DefaultRetryBehavior"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <param name="createMode">Describes the option selected by the user for creating shard map manager database.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardMapManager CreateSqlShardMapManager(
            string connectionString,
            ShardMapManagerCreateMode createMode)
        {
            return CreateSqlShardMapManager(
                connectionString,
                null,
                createMode);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
        /// with <see cref="RetryBehavior.DefaultRetryBehavior"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <param name="secureCredential">Secure credential used for creating shard map manager database.</param>
        /// <param name="createMode">Describes the option selected by the user for creating shard map manager database.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardMapManager CreateSqlShardMapManager(
            string connectionString,
            SqlCredential secureCredential,
            ShardMapManagerCreateMode createMode)
        {
            return CreateSqlShardMapManager(
                connectionString,
                secureCredential,
                createMode,
                RetryBehavior.DefaultRetryBehavior);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
        /// with <see cref="RetryPolicy.DefaultRetryPolicy"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <param name="createMode">Describes the option selected by the user for creating shard map manager database.</param>
        /// <param name="targetVersion">Target version of store to create.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        internal static ShardMapManager CreateSqlShardMapManager(
            string connectionString,
            ShardMapManagerCreateMode createMode,
            Version targetVersion)
        {
            return CreateSqlShardMapManagerImpl(
                new SqlConnectionInfo(
                    connectionString,
                    null),
                createMode,
                RetryBehavior.DefaultRetryBehavior,
                null,
                targetVersion);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
        /// with <see cref="ShardMapManagerCreateMode.KeepExisting"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardMapManager CreateSqlShardMapManager(
            string connectionString,
            RetryBehavior retryBehavior)
        {
            return CreateSqlShardMapManager(
                connectionString,
                null,
                retryBehavior);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database,
        /// with <see cref="ShardMapManagerCreateMode.KeepExisting"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <param name="secureCredential">Secure credential used for creating shard map manager database.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardMapManager CreateSqlShardMapManager(
            string connectionString,
            SqlCredential secureCredential,
            RetryBehavior retryBehavior)
        {
            return CreateSqlShardMapManager(
                connectionString,
                secureCredential,
                ShardMapManagerCreateMode.KeepExisting,
                retryBehavior);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <param name="createMode">Describes the option selected by the user for creating shard map manager database.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        public static ShardMapManager CreateSqlShardMapManager(
            string connectionString,
            ShardMapManagerCreateMode createMode,
            RetryBehavior retryBehavior)
        {
            return CreateSqlShardMapManager(
                connectionString,
                null,
                createMode,
                retryBehavior);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for creating shard map manager database.</param>
        /// <param name="secureCredential">Secure credential used for creating shard map manager database.</param>
        /// <param name="createMode">Describes the option selected by the user for creating shard map manager database.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        public static ShardMapManager CreateSqlShardMapManager(
            string connectionString,
            SqlCredential secureCredential,
            ShardMapManagerCreateMode createMode,
            RetryBehavior retryBehavior)
        {
            return CreateSqlShardMapManager(
                new SqlConnectionInfo(
                    connectionString,
                    secureCredential),
                createMode,
                retryBehavior);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database.
        /// </summary>
        /// <param name="connectionInfo">Connection parameters used for creating shard map manager database.</param>
        /// <param name="createMode">Describes the option selected by the user for creating shard map manager database.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        public static ShardMapManager CreateSqlShardMapManager(
            SqlConnectionInfo connectionInfo,
            ShardMapManagerCreateMode createMode,
            RetryBehavior retryBehavior)
        {
            return CreateSqlShardMapManagerImpl(
                connectionInfo,
                createMode,
                retryBehavior,
                null,
                GlobalConstants.GsmVersionClient);
        }

        /// <summary>
        /// Creates a <see cref="ShardMapManager"/> and its corresponding storage structures in the specified SQL Server database.
        /// </summary>
        /// <param name="connectionInfo">Connection parameters used for creating shard map manager database.</param>
        /// <param name="createMode">Describes the option selected by the user for creating shard map manager database.</param>
        /// <param name="retryBehavior">Behavior for performing retries on connections to shard map manager database.</param>
        /// <param name="retryEventHandler">Event handler for store operation retry events.</param>
        /// <param name="targetVersion">Target version of Store to deploy, this is mainly used for upgrade testing.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        private static ShardMapManager CreateSqlShardMapManagerImpl(
            SqlConnectionInfo connectionInfo,
            ShardMapManagerCreateMode createMode,
            RetryBehavior retryBehavior,
            EventHandler<RetryingEventArgs> retryEventHandler,
            Version targetVersion)
        {
            ExceptionUtils.DisallowNullArgument(connectionInfo, "connectionInfo");
            ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

            if (createMode != ShardMapManagerCreateMode.KeepExisting &&
                createMode != ShardMapManagerCreateMode.ReplaceExisting)
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._General_InvalidArgumentValue,
                        createMode,
                        "createMode"),
                    "createMode");
            }

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    "CreateSqlShardMapManager",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                SqlShardMapManagerCredentials credentials = new SqlShardMapManagerCredentials(connectionInfo);

                TransientFaultHandling.RetryPolicy retryPolicy = new TransientFaultHandling.RetryPolicy(
                        new ShardManagementTransientErrorDetectionStrategy(retryBehavior),
                        RetryPolicy.DefaultRetryPolicy.GetRetryStrategy());

                EventHandler<TransientFaultHandling.RetryingEventArgs> handler = (sender, args) =>
                {
                    if (retryEventHandler != null)
                    {
                        retryEventHandler(sender, new RetryingEventArgs(args));
                    }
                };

                try
                {
                    retryPolicy.Retrying += handler;

                    // specifying targetVersion as GlobalConstants.GsmVersionClient to deploy latest store by default.
                    using (IStoreOperationGlobal op = new StoreOperationFactory().CreateCreateShardMapManagerGlobalOperation(
                        credentials,
                        retryPolicy,
                        "CreateSqlShardMapManager",
                        createMode,
                        targetVersion))
                    {
                        op.Do();
                    }

                    stopwatch.Stop();

                    Tracer.TraceInfo(
                        TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                        "CreateSqlShardMapManager",
                        "Complete; Duration: {0}",
                        stopwatch.Elapsed);
                }
                finally
                {
                    retryPolicy.Retrying -= handler;
                }

                return new ShardMapManager(
                    credentials,
                    new SqlStoreConnectionFactory(),
                    new StoreOperationFactory(),
                    new CacheStore(),
                    ShardMapManagerLoadPolicy.Lazy,
                    RetryPolicy.DefaultRetryPolicy,
                    retryBehavior,
                    retryEventHandler);
            }
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="shardMapManager">Shard map manager object used for performing management and read operations for shard maps,
        /// shards and shard mappings or <c>null</c> in case shard map manager does not exist.</param>
        /// <returns>
        /// <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static bool TryGetSqlShardMapManager(
            string connectionString,
            ShardMapManagerLoadPolicy loadPolicy,
            out ShardMapManager shardMapManager)
        {
            return TryGetSqlShardMapManager(
                connectionString,
                null,
                loadPolicy,
                out shardMapManager);
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="secureCredential">Secure credential used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="shardMapManager">Shard map manager object used for performing management and read operations for shard maps,
        ///     shards and shard mappings or <c>null</c> in case shard map manager does not exist.</param>
        /// <returns>
        /// <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static bool TryGetSqlShardMapManager(
            string connectionString,
            SqlCredential secureCredential,
            ShardMapManagerLoadPolicy loadPolicy,
            out ShardMapManager shardMapManager)
        {
            return TryGetSqlShardMapManager(
                connectionString,
                secureCredential,
                loadPolicy,
                RetryBehavior.DefaultRetryBehavior,
                out shardMapManager);
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <param name="shardMapManager">Shard map manager object used for performing management and read operations for shard maps,
        ///     shards and shard mappings or <c>null</c> in case shard map manager does not exist.</param>
        /// <returns>
        /// <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        public static bool TryGetSqlShardMapManager(
            string connectionString,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryBehavior retryBehavior,
            out ShardMapManager shardMapManager)
        {
            return TryGetSqlShardMapManager(
                    connectionString,
                    null,
                    loadPolicy,
                    retryBehavior,
                    out shardMapManager);
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="secureCredential">Secure credential used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <param name="shardMapManager">Shard map manager object used for performing management and read operations for shard maps,
        ///     shards and shard mappings or <c>null</c> in case shard map manager does not exist.</param>
        /// <returns>
        /// <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        public static bool TryGetSqlShardMapManager(
            string connectionString,
            SqlCredential secureCredential,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryBehavior retryBehavior,
            out ShardMapManager shardMapManager)
        {
            return TryGetSqlShardMapManager(
                new SqlConnectionInfo(
                    connectionString,
                    secureCredential),
                loadPolicy,
                retryBehavior,
                out shardMapManager);
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionInfo">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <param name="shardMapManager">Shard map manager object used for performing management and read operations for shard maps,
        ///     shards and shard mappings or <c>null</c> in case shard map manager does not exist.</param>
        /// <returns>
        /// <c>true</c> if a shard map manager object was created, <c>false</c> otherwise.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "2#"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
        public static bool TryGetSqlShardMapManager(
            SqlConnectionInfo connectionInfo,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryBehavior retryBehavior,
            out ShardMapManager shardMapManager)
        {
            ExceptionUtils.DisallowNullArgument(connectionInfo, "connectionInfo");
            ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    "TryGetSqlShardMapManager",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    connectionInfo,
                    loadPolicy,
                    retryBehavior,
                    null,
                    false);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    "TryGetSqlShardMapManager",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return shardMapManager != null;
            }
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database, with <see cref="RetryBehavior.DefaultRetryBehavior"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardMapManager GetSqlShardMapManager(
            string connectionString,
            ShardMapManagerLoadPolicy loadPolicy)
        {
            return GetSqlShardMapManager(
                connectionString,
                null,
                loadPolicy,
                RetryBehavior.DefaultRetryBehavior);
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database, with <see cref="RetryBehavior.DefaultRetryBehavior"/>.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="secureCredential">Secure credential used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardMapManager GetSqlShardMapManager(
            string connectionString,
            SqlCredential secureCredential,
            ShardMapManagerLoadPolicy loadPolicy)
        {
            return GetSqlShardMapManager(
                connectionString,
                secureCredential,
                loadPolicy,
                RetryBehavior.DefaultRetryBehavior);
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        public static ShardMapManager GetSqlShardMapManager(
            string connectionString,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryBehavior retryBehavior)
        {
            return GetSqlShardMapManager(
                new SqlConnectionInfo(
                    connectionString,
                    null),
                loadPolicy,
                retryBehavior,
                null);
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionString">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="secureCredential">Secure credential used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        public static ShardMapManager GetSqlShardMapManager(
            string connectionString,
            SqlCredential secureCredential,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryBehavior retryBehavior)
        {
            return GetSqlShardMapManager(
                new SqlConnectionInfo(
                    connectionString,
                    secureCredential),
                loadPolicy,
                retryBehavior);
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionInfo">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        public static ShardMapManager GetSqlShardMapManager(
            SqlConnectionInfo connectionInfo,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryBehavior retryBehavior)
        {
            return GetSqlShardMapManager(
                connectionInfo,
                loadPolicy,
                retryBehavior,
                null);
        }

        /// <summary>
        /// Create shard management performance counter category and counters
        /// </summary>
        public static void CreatePerformanceCategoryAndCounters()
        {
            PerfCounterInstance.CreatePerformanceCategoryAndCounters();
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionInfo">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <param name="retryEventHandler">Event handler for store operation retry events.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        private static ShardMapManager GetSqlShardMapManager(
            SqlConnectionInfo connectionInfo,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryBehavior retryBehavior,
            EventHandler<RetryingEventArgs> retryEventHandler)
        {
            ExceptionUtils.DisallowNullArgument(connectionInfo, "connectionInfo");
            ExceptionUtils.DisallowNullArgument(retryBehavior, "retryBehavior");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    "GetSqlShardMapManager",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    connectionInfo,
                    loadPolicy,
                    retryBehavior,
                    retryEventHandler,
                    true);

                stopwatch.Stop();

                Debug.Assert(shardMapManager != null);

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMapManagerFactory,
                    "GetSqlShardMapManager",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return shardMapManager;
            }
        }

        /// <summary>
        /// Gets <see cref="ShardMapManager"/> from persisted state in a SQL Server database.
        /// </summary>
        /// <param name="connectionInfo">Connection parameters used for performing operations against shard map manager database(s).</param>
        /// <param name="loadPolicy">Initialization policy.</param>
        /// <param name="retryBehavior">Behavior for detecting transient exceptions in the store.</param>
        /// <param name="retryEventHandler">Event handler for store operation retry events.</param>
        /// <param name="throwOnFailure">Whether to raise exception on failure.</param>
        /// <returns>
        /// A shard map manager object used for performing management and read operations for
        /// shard maps, shards and shard mappings or <c>null</c> if the object could not be created.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "We need to hand of the ShardMapManager to the user.")]
        private static ShardMapManager GetSqlShardMapManager(
            SqlConnectionInfo connectionInfo,
            ShardMapManagerLoadPolicy loadPolicy,
            RetryBehavior retryBehavior,
            EventHandler<RetryingEventArgs> retryEventHandler,
            bool throwOnFailure)
        {
            Debug.Assert(connectionInfo != null);
            Debug.Assert(retryBehavior != null);

            SqlShardMapManagerCredentials credentials = new SqlShardMapManagerCredentials(connectionInfo);

            StoreOperationFactory storeOperationFactory = new StoreOperationFactory();

            IStoreResults result;

            TransientFaultHandling.RetryPolicy retryPolicy = new TransientFaultHandling.RetryPolicy(
                    new ShardManagementTransientErrorDetectionStrategy(retryBehavior),
                    RetryPolicy.DefaultRetryPolicy.GetRetryStrategy());

            EventHandler<TransientFaultHandling.RetryingEventArgs> handler = (sender, args) =>
            {
                if (retryEventHandler != null)
                {
                    retryEventHandler(sender, new RetryingEventArgs(args));
                }
            };

            try
            {
                retryPolicy.Retrying += handler;

                using (IStoreOperationGlobal op = storeOperationFactory.CreateGetShardMapManagerGlobalOperation(
                    credentials,
                    retryPolicy,
                    throwOnFailure ? "GetSqlShardMapManager" : "TryGetSqlShardMapManager",
                    throwOnFailure))
                {
                    result = op.Do();
                }
            }
            finally
            {
                retryPolicy.Retrying -= handler;
            }

            return result.Result == StoreResult.Success ?
                new ShardMapManager(
                    credentials,
                    new SqlStoreConnectionFactory(),
                    storeOperationFactory,
                    new CacheStore(),
                    loadPolicy,
                    RetryPolicy.DefaultRetryPolicy,
                    retryBehavior,
                    retryEventHandler) :
                null;
        }
    }
}
