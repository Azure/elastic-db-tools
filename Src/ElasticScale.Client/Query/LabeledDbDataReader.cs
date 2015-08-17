// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Convenience class that holds a DbDataReader along with a string label for the
// shard that the data underlying the DbDataReader came from.
//
// Notes:
// This is useful for keeping the DbDataReader and the label together when 
// executing asynchronously.

using System;
using System.Data.Common;
using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    /// <summary>
    /// Simple, immutable class for affiliating a DbDataReader with additional information
    /// related to the reader (e.g. DbCommand, shard, exceptions encountered etc)
    /// Useful when grabbing DbDataReaders asynchronously.
    /// </summary>
    internal class LabeledDbDataReader : IDisposable
    {
        /// <summary>
        /// Whether DbDataReader has been disposed or not.
        /// </summary>
        private bool _disposed;

        #region Constructors

        /// <summary>
        /// Simple constructor to set up an immutable LabeledDbDataReader object.
        /// </summary>
        /// <param name="reader">The DbDataReader to keep track of.</param>
        /// <param name="shardLocation">The Shard this reader belongs to</param>
        /// <param name="cmd">The command object that produced ther reader.</param>
        /// <exception cref="ArgumentNullException">
        /// If either of the arguments is null.
        /// </exception>
        internal LabeledDbDataReader(DbDataReader reader, ShardLocation shardLocation, DbCommand cmd)
            : this(shardLocation, cmd)
        {
            if (null == reader)
            {
                throw new ArgumentNullException("reader");
            }

            this.DbDataReader = reader;
        }

        internal LabeledDbDataReader(MultiShardException exception, ShardLocation shardLocation, DbCommand cmd)
            : this(shardLocation, cmd)
        {
            if (null == exception)
            {
                throw new ArgumentNullException("exception");
            }

            this.Exception = exception;
        }

        private LabeledDbDataReader(ShardLocation shardLocation, DbCommand cmd)
        {
            if (null == shardLocation)
            {
                throw new ArgumentNullException("shardLocation");
            }

            if (null == cmd)
            {
                throw new ArgumentNullException("cmd");
            }

            this.ShardLocation = shardLocation;
            this.ShardLabel = ShardLocation.ToString();
            this.Command = cmd;
        }

        #endregion Constructors

        #region Internal Properties

        /// <summary>
        /// The location of the shard
        /// </summary>
        internal ShardLocation ShardLocation
        {
            get;
            private set;
        }

        /// <summary>
        /// The Shard location information
        /// </summary>
        internal string ShardLabel
        {
            get;
            private set;
        }

        /// <summary>
        /// The exception encountered when trying to execute against this reader
        /// Could be null if the DbDataReader was instantiated successfully for this Shard
        /// </summary>
        internal MultiShardException Exception
        {
            get;
            private set;
        }

        /// <summary>
        /// The DbDataReader to keep track of.
        /// Could be null if we encountered an exception whilst executing the command against this shard
        /// </summary>
        internal DbDataReader DbDataReader
        {
            get;
            private set;
        }

        /// <summary>
        /// The DbConnection associated with this reader
        /// </summary>
        internal DbConnection Connection
        {
            get
            {
                return this.Command.Connection;
            }
        }

        /// <summary>
        /// The command object that produces this reader.
        /// </summary>
        internal DbCommand Command
        {
            get;
            private set;
        }

        #endregion Internal Properties

        #region IDisposable

        public void Dispose()
        {
            if (!_disposed)
            {
                this.DbDataReader.Dispose();
                _disposed = true;
            }
        }

        #endregion IDisposable
    }
}
