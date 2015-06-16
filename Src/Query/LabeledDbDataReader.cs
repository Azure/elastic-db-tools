//******************************************************************************
// Copyright (c) Microsoft Corporation
//
// @File: LabeledDbDataReader.cs
//
// @Owner: errobins
// @Test:
//
// Purpose:
// Convenience class that holds a DbDataReader along with a string label for the
// shard that the data underlying the DbDataReader came from.
//
// Notes:
// This is useful for keeping the DbDataReader and the label together when 
// executing asynchronously.
//
//******************************************************************************

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
        /// Whether DbDataReader has been disposed or not
        /// </summary>
        private bool m_isDisposed = false;

        #region Constructors

        /// <summary>
        /// Simple constructor to set up an immutable LabeledDbDataReader object.
        /// </summary>
        /// <param name="theReader">The DbDataReader to keep track of.</param>
        /// <param name="shardLocation">The Shard this reader belongs to</param>
        /// <param name="connection">The DbConnection associated with this reader</param>
        /// <exception cref="ArgumentNullException">
        /// If either of the arguments is null.
        /// </exception>
        public LabeledDbDataReader(DbDataReader theReader, ShardLocation shardLocation, DbConnection connection)
            : this(shardLocation, connection)
        {
            if (null == theReader)
            {
                throw new ArgumentNullException("theReader");
            }

            DbDataReader = theReader;
        }

        public LabeledDbDataReader(MultiShardException exception, ShardLocation shardLocation, DbConnection connection)
            : this(shardLocation, connection)
        {
            if (null == exception)
            {
                throw new ArgumentNullException("exception");
            }

            Exception = exception;
        }

        private LabeledDbDataReader(ShardLocation shardLocation, DbConnection connection)
        {
            if (null == shardLocation)
            {
                throw new ArgumentNullException("shardLocation");
            }

            if(null == connection)
            {
                throw new ArgumentNullException("connection");
            }

            ShardLocation = shardLocation;
            ShardLabel = ShardLocation.ToString();
            Connection = connection;
        }

        #endregion Constructors

        #region Public Properties

        /// <summary>
        /// The location of the shard
        /// </summary>
        public ShardLocation ShardLocation { get; private set; }

        /// <summary>
        /// The Shard location information
        /// </summary>
        public string ShardLabel { get; private set; }

        /// <summary>
        /// The exception encountered when trying to execute against this reader
        /// Could be null if the DbDataReader was instantiated successfully for this Shard
        /// </summary>
        public MultiShardException Exception { get; private set; }

        /// <summary>
        /// The DbDataReader to keep track of.
        /// Could be null if we encountered an exception whilst executing the command against this shard
        /// </summary>
        public DbDataReader DbDataReader { get; private set; }

        /// <summary>
        /// The DbConnection associated with this reader
        /// </summary>
        public DbConnection Connection { get; private set; }

        #endregion Public Properties

        public void Dispose()
        {
            if (!(m_isDisposed))
            {
                DbDataReader.Dispose();
                m_isDisposed = true;
            }
        }
    }
}
