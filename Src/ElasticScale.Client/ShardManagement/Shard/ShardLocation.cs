// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Globalization;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Types of transport protocols supported in SQL Server connections.
    /// </summary>
    public enum SqlProtocol
    {
        /// <summary>
        /// Default protocol.
        /// </summary>
        Default,

        /// <summary>
        /// TCP/IP protocol.
        /// </summary>
        Tcp,

        /// <summary>
        /// Named pipes protocol.
        /// </summary>
        NamedPipes,

        /// <summary>
        /// Shared memory protocol.
        /// </summary>  
        SharedMemory
    }

    /// <summary>
    /// Represents the location of a shard in terms of its server name and database name. 
    /// This is used to manage connections to the shard and to support other operations on shards.
    /// As opposed to a <see cref="Shard"/>, a shard location is not registered with the shard map.
    /// </summary>
    [Serializable]
    public sealed class ShardLocation : IEquatable<ShardLocation>
    {
        /// <summary>Hashcode for the shard location.</summary>
        private readonly int _hashCode;

        /// <summary>
        /// Constructor that allows specification of protocol, address, port and database to identify a shard.
        /// </summary>
        /// <param name="server">Fully qualified hostname of the server for the shard database.</param>
        /// <param name="database">Name of the shard database.</param>
        /// <param name="protocol">Transport protcol used for the connection.</param>
        /// <param name="port">Port number for TCP/IP connections. Specify 0 to use the default port for the specified <paramref name="protocol"/>.</param>
        public ShardLocation(
            string server,
            string database,
            SqlProtocol protocol,
            int port)
        {
            if (protocol < SqlProtocol.Default || protocol > SqlProtocol.SharedMemory)
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardLocation_UnsupportedProtocol,
                        protocol),
                    "protocol");
            }

            if (port < 0 || 65535 < port)
            {
                throw new ArgumentOutOfRangeException(
                    "port",
                    StringUtils.FormatInvariant(
                        Errors._ShardLocation_InvalidPort,
                        port));
            }

            ExceptionUtils.DisallowNullOrEmptyStringArgument(server, "server");
            ExceptionUtils.DisallowNullOrEmptyStringArgument(database, "database");

            if (server.Length > GlobalConstants.MaximumServerLength)
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardLocation_InvalidServerOrDatabase,
                        "Server",
                        GlobalConstants.MaximumServerLength),
                        "server");
            }

            if (database.Length > GlobalConstants.MaximumDatabaseLength)
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardLocation_InvalidServerOrDatabase,
                        "Database",
                        GlobalConstants.MaximumDatabaseLength),
                        "database");
            }

            this.Protocol = protocol;
            this.Server = server;
            this.Port = port;
            this.Database = database;
            _hashCode = this.CalculateHashCode();
        }

        /// <summary>
        /// Constructor that allows specification of address and database to identify a shard.
        /// </summary>
        /// <param name="server">Fully qualified hostname of the server for the shard database.</param>
        /// <param name="database">Name of the shard database.</param>
        public ShardLocation(string server, string database) :
            this(server, database, SqlProtocol.Default, 0)
        {
        }

        /// <summary>
        /// Constructor that allows specification of address and database to identify a shard.
        /// </summary>
        /// <param name="server">Fully qualified hostname of the server for the shard database.</param>
        /// <param name="database">Name of the shard database.</param>
        /// <param name="protocol">Transport protcol used for the connection.</param>
        public ShardLocation(string server, string database, SqlProtocol protocol) :
            this(server, database, protocol, 0)
        {
        }

        /// <summary>
        /// Protocol name prefix.
        /// </summary>
        public SqlProtocol Protocol
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the fully qualified hostname of the server for the shard database.
        /// </summary>
        public string Server
        {
            get;
            private set;
        }

        /// <summary>
        /// Communication port for TCP/IP protocol. If no port is specified, the property returns 0.
        /// </summary>
        public int Port
        {
            get;
            private set;
        }

        /// <summary>
        /// DataSource name which can be used to construct connection string Data Source property.
        /// </summary>
        public string DataSource
        {
            get
            {
                return StringUtils.FormatInvariant(
                    "{0}{1}{2}",
                    this.GetProtocolPrefix(),
                    this.Server,
                    this.GetPortSuffix());
            }
        }

        /// <summary>
        /// Gets the database name of the shard.
        /// </summary>
        public string Database
        {
            get;
            private set;
        }

        /// <summary>
        /// Converts the shard location to its string representation.
        /// </summary>
        /// <returns>String representation of shard location.</returns>
        public override string ToString()
        {
            return StringUtils.FormatInvariant(
                "[DataSource={0} Database={1}]",
                this.DataSource,
                this.Database);
        }

        /// <summary>
        /// Calculates the hash code for this instance.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        public override int GetHashCode()
        {
            return _hashCode;
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            return this.Equals(obj as ShardLocation);
        }

        /// <summary>
        /// Performs equality comparison with another given ShardLocation.
        /// </summary>
        /// <param name="other">ShardLocation to compare with.</param>
        /// <returns>True if same locations, false otherwise.</returns>
        public bool Equals(ShardLocation other)
        {
            if (other == null)
            {
                return false;
            }
            else
            {
                if (this.GetHashCode() != other.GetHashCode())
                {
                    return false;
                }
                else
                {
                    return (this.Protocol == other.Protocol &&
                            String.Compare(this.DataSource, other.DataSource, StringComparison.OrdinalIgnoreCase) == 0 &&
                            this.Port == other.Port &&
                            String.Compare(this.Database, other.Database, StringComparison.OrdinalIgnoreCase) == 0);
                }
            }
        }

        /// <summary>
        /// Calculates the hash code for the object.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        private int CalculateHashCode()
        {
            int h;

            h = ShardKey.QPHash(this.Protocol.GetHashCode(), this.DataSource.ToUpper(CultureInfo.InvariantCulture).GetHashCode());
            h = ShardKey.QPHash(h, this.Port.GetHashCode());
            h = ShardKey.QPHash(h, this.Database.ToUpper(CultureInfo.InvariantCulture).GetHashCode());

            return h;
        }

        /// <summary>
        /// Gets the connection string data source prefix for the supported protocol.
        /// </summary>
        /// <returns>Connection string prefix containing string representation of protocol.</returns>
        private string GetProtocolPrefix()
        {
            switch (this.Protocol)
            {
                case SqlProtocol.Tcp:
                    return "tcp:";
                case SqlProtocol.NamedPipes:
                    return "np:";
                case SqlProtocol.SharedMemory:
                    return "lpc:";
                default:
                    Debug.Assert(this.Protocol == SqlProtocol.Default);
                    return String.Empty;
            }
        }

        /// <summary>
        /// Gets the connection string data source suffix for supplied port number.
        /// </summary>
        /// <returns>Connection string suffix containing string representation of port.</returns>
        private string GetPortSuffix()
        {
            if (this.Port != 0)
            {
                return StringUtils.FormatInvariant(",{0}", this.Port);
            }
            else
            {
                return String.Empty;
            }
        }
    }
}
