// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Creates connections to the given set of shards
// and governs their lifetime
//
// Notes:
// * This class is NOT thread-safe.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

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
        private bool _disposed = false;

        /// <summary>
        /// The credentials for connection.
        /// </summary>
        private SqlConnectionInfo _connectionInfo;

        /// <summary>
        /// The shard connections
        /// </summary>
        private List<Tuple<ShardLocation, DbConnection>> _shardConnections;

        #endregion

#region Ctors

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardConnection"/> class.
        /// </summary>
        /// <param name="shards">The collection of <see cref="Shard"/>s used for this connection instances.</param>
        /// <param name="connectionString">
        /// These credentials will be used to connect to the <see cref="Shard"/>s.
        /// The same credentials are used on all shards.
        /// Therefore, all shards need to provide the appropriate permissions for these credentials to execute the command.
        /// </param>
        /// <remarks>
        /// Multiple Active Result Sets (MARS) are not supported and are disabled for any processing at the shards.
        /// </remarks>
        public MultiShardConnection(IEnumerable<Shard> shards, string connectionString)
        {
            InitializeConnectionInfo(connectionString);
            InitializeShardConnections(shards);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardConnection"/> class.
        /// </summary>
        /// <param name="shardLocations">The collection of <see cref="ShardLocation"/>s used for this connection instances.</param>
        /// <param name="connectionString">
        /// These credentials will be used to connect to the <see cref="Shard"/>s.
        /// The same credentials are used on all shards.
        /// Therefore, all shards need to provide the appropriate permissions for these credentials to execute the command.
        /// </param>
        /// <remarks>
        /// Multiple Active Result Sets (MARS) are not supported and are disabled for any processing at the shards.
        /// </remarks>
        public MultiShardConnection(IEnumerable<ShardLocation> shardLocations, string connectionString)
        {
            InitializeConnectionInfo(connectionString);
            InitializeShardConnections(shardLocations);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardConnection"/> class.
        /// </summary>
        /// <param name="shards">The collection of <see cref="Shard"/>s used for this connection instances.</param>
        /// <param name="connectionInfo">
        /// These credentials will be used to connect to the <see cref="Shard"/>s.
        /// The same credentials are used on all shards.
        /// Therefore, all shards need to provide the appropriate permissions for these credentials to execute the command.
        /// </param>
        /// <remarks>
        /// Multiple Active Result Sets (MARS) are not supported and are disabled for any processing at the shards.
        /// </remarks>
        public MultiShardConnection(IEnumerable<Shard> shards, SqlConnectionInfo connectionInfo)
        {
            InitializeConnectionInfo(connectionInfo);
            InitializeShardConnections(shards);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardConnection"/> class.
        /// </summary>
        /// <param name="shardLocations">The collection of <see cref="ShardLocation"/>s used for this connection instances.</param>
        /// <param name="connectionInfo">
        /// These credentials will be used to connect to the <see cref="Shard"/>s.
        /// The same credentials are used on all shards.
        /// Therefore, all shards need to provide the appropriate permissions for these credentials to execute the command.
        /// </param>
        /// <remarks>
        /// Multiple Active Result Sets (MARS) are not supported and are disabled for any processing at the shards.
        /// </remarks>
        public MultiShardConnection(IEnumerable<ShardLocation> shardLocations, SqlConnectionInfo connectionInfo)
        {
            InitializeConnectionInfo(connectionInfo);
            InitializeShardConnections(shardLocations);
        }

        /// <summary>
        /// Creates an instance of this class
        /// /* TEST ONLY */
        /// </summary>
        /// <param name="shardConnections">Connections to the shards</param>
        internal MultiShardConnection(List<Tuple<ShardLocation, DbConnection>> shardConnections)
        {
            this._shardConnections = shardConnections;
        }

#endregion

#region Properties

        /// <summary>
        /// Gets the collection of <see cref="Shard"/>s associated with this connection.
        /// </summary>
        public IEnumerable<Shard> Shards
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of <see cref="ShardLocation"/>s associated with this connection.
        /// </summary>
        public IEnumerable<ShardLocation> ShardLocations
        {
            get
            {
                return this._shardConnections.Select(s => s.Item1);
            }
        }

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
            if (!_disposed)
            {
                // Dispose off the shard connections
                this._shardConnections.ForEach(
                (c) =>
                {
                    if (c.Item2 != null)
                    {
                        c.Item2.Dispose();
                    }
                });

                _disposed = true;

                s_tracer.Warning("MultiShardConnection.Dispose", "Connection was disposed");
            }
        }

        #endregion


        #region Internal methods

        /// <summary>
        /// Gets the shard connections
        /// </summary>
        internal List<Tuple<ShardLocation, DbConnection>> GetShardConnections()
        {
            // Refresh the access tokens for all shard connections.
            // (Null check because unit tests use internal code path which doesn't initialize _connectionInfo).
            if (this._connectionInfo != null)
            {
                foreach (var shardConnection in _shardConnections)
                {
                    if (shardConnection.Item2 != null)
                    {
                        this._connectionInfo.RefreshAccessToken(shardConnection.Item2);
                    }
                }
            }

            return _shardConnections;
        }

        #endregion

        #region Helpers

        private static void ValidateNotEmpty<T>(
            IEnumerable<T> namedCollection,
            string collectionName)
        {
            if (!namedCollection.Any())
            {
                throw new ArgumentException(string.Format("No {0} provided.", collectionName));
            }
        }

        private void InitializeConnectionInfo(SqlConnectionInfo connectionInfo)
        {
            if (connectionInfo == null)
            {
                throw new ArgumentNullException("connectionInfo");
            }

            string updatedConnectionString = InitializeConnectionString(connectionInfo.ConnectionString);
            this._connectionInfo = connectionInfo.CloneWithUpdatedConnectionString(updatedConnectionString);
        }

        private void InitializeConnectionInfo(string connectionString)
        {
            string updatedConnectionString = InitializeConnectionString(connectionString);
            this._connectionInfo = new SqlConnectionInfo(updatedConnectionString);
        }

        private static string InitializeConnectionString(string connectionString)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            // Enhance the ApplicationName with this library's name as a suffix
            // Devnote: If connection string specifies Active Directory authentication and runtime is not
            // .NET 4.6 or higher, then below call will throw.
            SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(
                connectionString).WithApplicationNameSuffix(ApplicationNameSuffix);

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

            return connectionStringBuilder.ToString();
        }

        private void InitializeShardConnections(IEnumerable<Shard> shards)
        {
            if (shards == null)
            {
                throw new ArgumentNullException("shards");
            }

            // Force evaluation of the input enumerable so that we don't evaluate it multiple times later
            this.Shards = shards.ToList();
            ValidateNotEmpty(this.Shards, "shards");

            this._shardConnections = (this.Shards.Select(
                s => CreateDbConnectionForLocation(s.Location, _connectionInfo)).ToList());
        }

        private void InitializeShardConnections(IEnumerable<ShardLocation> shardLocations)
        {
            if (shardLocations == null)
            {
                throw new ArgumentNullException("shardLocations");
            }

            // Force evaluation of the input enumerable so that we don't evaluate it multiple times later
            IList<ShardLocation> shardLocationsList = shardLocations.ToList();
            ValidateNotEmpty(shardLocationsList, "shardLocations");

            this.Shards = null;
            this._shardConnections = shardLocationsList.Select(
                s => CreateDbConnectionForLocation(s, _connectionInfo)).ToList();
        }

        // Suppression rationale:  The SqlConnections we are creating will underlie the object we are returning. We do not want to dispose them.
        //
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private static Tuple<ShardLocation, DbConnection> CreateDbConnectionForLocation(
            ShardLocation shardLocation,
            SqlConnectionInfo connectionInfo)
        {
            string shardConnectionString =
                new SqlConnectionStringBuilder(connectionInfo.ConnectionString)
                {
                    DataSource = shardLocation.DataSource,
                    InitialCatalog = shardLocation.Database
                }.ConnectionString;

            SqlConnectionInfo shardConnectionInfo = connectionInfo.CloneWithUpdatedConnectionString(shardConnectionString);

            return new Tuple<ShardLocation, DbConnection>(
                shardLocation,
                shardConnectionInfo.CreateConnection());
        }

        // Suppression rationale:  We explicitly do not want to throw here, so we must catch all exceptions.
        //
        /// <summary>
        /// Closes any open connections to shards
        /// </summary>
        /// <remarks>Does a best-effort close and doesn't throw</remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "We do not want to throw on Close.")]
        internal void Close()
        {
            foreach (var conn in this._shardConnections)
            {
                if (conn.Item2 != null && conn.Item2.State != ConnectionState.Closed)
                {
                    try
                    {
                        conn.Item2.Close();
                    }
                    catch (Exception)
                    {
                    }
                }
            }
        }

#endregion
    }
}
