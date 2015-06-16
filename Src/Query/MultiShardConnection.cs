//******************************************************************************
// Copyright (c) Microsoft Corporation
//
// @File: MultiShardConnection.cs
//
// @Owner: raveeram
// @Test:
//
// Purpose:
// Creates connections to the given set of shards
// and governs their lifetime
//
// Notes:
// * This class is NOT thread-safe.
//
//******************************************************************************

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale: "Multi" is the spelling we want here.
    //
    /// <summary>
    /// Represents a connection to a set of shards and provides the ability to process queries across the shard set.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi")]
    public sealed class MultiShardConnection : IDisposable
    {
        #region Global Vars

        /// <summary>
        /// The suffix to append to each shard's ApplicationName
        /// Will help with server-side telemetry
        /// </summary>
        internal static string ApplicationNameSuffix = "ESC_MSQv" + GlobalConstants.MultiShardQueryVersionInfo;

        /// <summary>
        /// The tracer
        /// </summary>
        private static readonly ILogger s_tracer = TraceHelper.Tracer;

        /// <summary>
        /// Whether this instance has already been disposed
        /// </summary>
        private bool m_disposed = false;

        #endregion

        #region Ctors

        // Suppression rationale:  The SqlConnections we are creating will underlie the object we are returning.  We do not want to dispose them.
        //
        /// <summary>
        /// Initializes a new instance of the MutliShardConnection class.
        /// </summary>
        /// <param name="shards">The collection of <see cref="Shards"/> used for this connection instances.</param>
        /// <param name="connectionString">
        /// These credentials will be used to connect to the <see cref="Shards"/>. 
        /// The same credentials are used on all shards. 
        /// Therefore, all shards need to provide the appropriate permissions for these credentials to execute the command.
        /// </param>
        /// <remarks>
        /// Multiple Active Result Sets (MARS) are not supported and are disabled for any processing at the shards.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public MultiShardConnection(IEnumerable<Shard> shards, string connectionString)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException("connectionString");
            }

            SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);

            ValidateConnectionArguments(shards, connectionStringBuilder);

            // Enhance the ApplicationName with this library's name as a suffix
            string applicationName = ApplicationNameHelper.AddApplicationNameSuffix(
                connectionStringBuilder.ApplicationName,
                ApplicationNameSuffix);

            SqlConnectionStringBuilder builder;
            List<Tuple<ShardLocation, DbConnection>> shardConnections = new List<Tuple<ShardLocation, DbConnection>>();
            Shards = shards;
            foreach (Shard shard in shards)
            {
                builder = new SqlConnectionStringBuilder(connectionStringBuilder.ConnectionString);
                builder.ApplicationName = applicationName;
                builder.DataSource = shard.Location.DataSource;
                builder.InitialCatalog = shard.Location.Database;
                shardConnections.Add(new Tuple<ShardLocation, DbConnection>(shard.Location, 
                    new SqlConnection(builder.ConnectionString)));
            }

            ShardConnections = shardConnections;
        }

        /// <summary>
        /// Creates an instance of this class 
        /// /* TEST ONLY */
        /// </summary>
        /// <param name="shardConnections">Connections to the shards</param>
        internal MultiShardConnection(List<Tuple<ShardLocation, DbConnection>> shardConnections)
        {
            ShardConnections = shardConnections;
        }

        #endregion

        #region Properties

        /// <summary>
        /// Gets the collection of <see cref="Shard"/>s associated with this connection.
        /// </summary>
        public IEnumerable<Shard> Shards { get; private set; }

        internal List<Tuple<ShardLocation, DbConnection>> ShardConnections { get; private set; }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates and returns a <see cref="MultiShardCommand"/> object. 
        /// The <see cref="MultiShardCommand"/> object can then be used to 
        /// execute a command against all shards specified in the connection.
        /// </summary>
        /// <returns>the <see cref="MultiShardCommand"/> with <see cref="MultiShardCommand.CommandText"/> set to null.</returns>
        public MultiShardCommand CreateCommand()
        {
            return MultiShardCommand.Create(this, commandText: null);
        }

        /// <summary>
        /// Releases all resources used by this object.
        /// </summary>
        public void Dispose()
        {
            if (!m_disposed)
            {
                // Dispose off the shard connections
                ShardConnections.ForEach((c) =>
                {
                    if (c.Item2 != null)
                    {
                        c.Item2.Dispose();
                    }
                });

                m_disposed = true;
                s_tracer.Warning("MultiShardConnection.Dispose", "Connection was disposed");
            }
        }

        #endregion

        #region Helpers

        private static void ValidateConnectionArguments(IEnumerable<Shard> shards,
            SqlConnectionStringBuilder connectionStringBuilder)
        {
            if (shards == null)
            {
                throw new ArgumentNullException("shards");
            }

            if (0 == shards.Count<Shard>())
            {
                throw new ArgumentException("No shards provided.");
            }

            // Datasource must not be set
            if (!string.IsNullOrEmpty(connectionStringBuilder.DataSource))
            {
                throw new ArgumentException("DataSource must not be set in the connectionStringBuilder");
            }

            // Initial catalog must not be set
            if (!string.IsNullOrEmpty(connectionStringBuilder.InitialCatalog))
            {
                throw new ArgumentException("InitialCatalog must not be set in the connectionStringBuilder");
            }
        }

        // Suppression rationale:  We explicitly do not want to throw here, so we must catch all exceptions.
        //
        /// <summary>
        /// Closes any open connections to shards
        /// </summary>
        /// <remarks>Does a best-effort close and doesn't throw</remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal void Close()
        {
            foreach (var conn in ShardConnections)
            {
                if (conn.Item2 != null && conn.Item2.State != ConnectionState.Closed)
                {
                    try
                    {
                        conn.Item2.Close();
                    }
                    catch
                    {
                    }
                }
            }
        }

        #endregion

    }
}
