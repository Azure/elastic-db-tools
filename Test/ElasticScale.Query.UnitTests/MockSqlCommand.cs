// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Purpose:
// Mocks SqlCommand

using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    public class MockSqlCommand : DbCommand, ICloneable
    {
        #region Global vars

        private SqlCommand _cmd = new SqlCommand();

        #endregion

        #region Ctors

        public MockSqlCommand()
            : this(5)
        {
        }
        public MockSqlCommand(int commandTimeout)
        {
            CommandTimeout = commandTimeout;
        }

        #endregion

        #region Properties

        /// <summary>
        /// The command text to execute against the shards
        /// </summary>
        public override string CommandText { get; set; }

        /// <summary>
        /// Command timeout
        /// </summary>
        public override int CommandTimeout { get; set; }

        /// <summary>
        /// Command type of the command to be executed
        /// </summary>
        public override CommandType CommandType { get; set; }

        public Action ExecuteReaderAction { get; set; }

        public Func<CancellationToken, MockSqlCommand, DbDataReader> ExecuteReaderFunc { get; set; }

        /// <summary>
        /// Gets the SqlParameter Collection
        /// </summary>
        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                return _cmd.Parameters;
            }
        }

        protected override DbParameter CreateDbParameter()
        {
            return new SqlParameter();
        }

        /// <summary>
        /// The ShardedDbConnetion that holds connections to multiple shards
        /// </summary>
        protected override DbConnection DbConnection { get; set; }

        public int RetryCount { get; set; }

        #endregion

        #region ExecuteReader Methods

        /// <summary>
        /// DEVNOTE (VSTS 2202707): Do we want to support command behavior?
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return ExecuteReaderFunc(CancellationToken.None, this);
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return Task.Run<DbDataReader>(() =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var reader = ExecuteReaderFunc(cancellationToken, this);
                    cancellationToken.ThrowIfCancellationRequested();
                    return reader;
                });
        }

        #endregion

        /// <summary>
        /// </summary>
        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Cancel any in progress commands
        /// </summary>
        public override void Cancel()
        {
        }

        /// <summary>
        /// Dispose off any unmanaged/managed resources held
        /// </summary>
        /// <param name="disposing"></param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        public MockSqlCommand Clone()
        {
            var clone = new MockSqlCommand(CommandTimeout);
            clone.CommandText = this.CommandText;
            clone.CommandType = this.CommandType;
            clone.Connection = this.Connection;
            clone.ExecuteReaderFunc = this.ExecuteReaderFunc;
            return clone;
        }

        object ICloneable.Clone()
        {
            return Clone();
        }

        #region UnSupported DbCommand Methods

        #region ExecuteNonQuery Methods

        public override int ExecuteNonQuery()
        {
            throw new NotImplementedException();
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            return base.ExecuteNonQueryAsync(cancellationToken);
        }

        #endregion

        #region ExecuteScalar Methods

        public override object ExecuteScalar()
        {
            throw new NotImplementedException();
        }

        public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            return base.ExecuteScalarAsync(cancellationToken);
        }
        #endregion

        #endregion

        #region UnSupported DbCommand Properties

        /// <summary>
        /// Gets or sets a value indicating whether the command object
        /// should be visible in a customized interface control
        /// </summary>
        /// <remarks>We do not support this</remarks>
        public override bool DesignTimeVisible
        {
            get
            {
                throw new NotSupportedException();
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Currently, Transactions aren't supported against shards
        /// </summary>
        protected override DbTransaction DbTransaction
        {
            get
            {
                throw new NotSupportedException();
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// </summary>
        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                throw new NotSupportedException();
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        #endregion
    }
}
