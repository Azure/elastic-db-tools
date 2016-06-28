// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Scope of a transactional operation. Operations within scope happen atomically.
    /// </summary>
    internal class SqlStoreTransactionScope : IStoreTransactionScope
    {
        /// <summary>
        /// Connection used for operation.
        /// </summary>
        private SqlConnection _conn;

        /// <summary>
        /// Transaction used for operation.
        /// </summary>
        private SqlTransaction _tran;

        /// <summary>
        /// Constructs an instance of an atom transaction scope.
        /// </summary>
        /// <param name="kind">Type of transaction scope.</param>
        /// <param name="conn">Connection to use for the transaction scope.</param>
        protected internal SqlStoreTransactionScope(StoreTransactionScopeKind kind, SqlConnection conn)
        {
            this.Kind = kind;
            _conn = conn;

            switch (this.Kind)
            {
                case StoreTransactionScopeKind.ReadOnly:
                    SqlUtils.WithSqlExceptionHandling(() =>
                    {
                        _tran = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                    });
                    break;
                case StoreTransactionScopeKind.ReadWrite:
                    SqlUtils.WithSqlExceptionHandling(() =>
                    {
                        _tran = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    });
                    break;
                default:
                    // Do not start any transaction.
                    Debug.Assert(this.Kind == StoreTransactionScopeKind.NonTransactional);
                    break;
            }
        }

        /// <summary>
        /// Type of transaction scope.
        /// </summary>
        public StoreTransactionScopeKind Kind
        {
            get;
            private set;
        }

        /// <summary>
        /// Property used to mark successful completion of operation. The transaction
        /// will be committed if this is <c>true</c> and rolled back if this is <c>false</c>.
        /// </summary>
        public virtual bool Success
        {
            get;
            set;
        }

        /// <summary>
        /// Executes the given stored procedure using the <paramref name="operationData"/> values
        /// as the parameter and a single output parameter.
        /// </summary>
        /// <param name="operationName">Operation to execute.</param>
        /// <param name="operationData">Input data for operation.</param>
        /// <returns>Storage results object.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public virtual IStoreResults ExecuteOperation(string operationName, XElement operationData)
        {
            return SqlUtils.WithSqlExceptionHandling<IStoreResults>(() =>
            {
                SqlResults results = new SqlResults();

                using (SqlCommand cmd = _conn.CreateCommand())
                using (XmlReader input = operationData.CreateReader())
                {
                    cmd.Transaction = _tran;
                    cmd.CommandText = operationName;
                    cmd.CommandType = CommandType.StoredProcedure;

                    SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, -1, new SqlXml(input));

                    SqlParameter result = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        results.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    results.Result = (StoreResult)result.Value;
                }

                return results;
            });
        }

        /// <summary>
        /// Asynchronously executes the given operation using the <paramref name="operationData"/> values
        /// as input to the operation.
        /// </summary>
        /// <param name="operationName">Operation to execute.</param>
        /// <param name="operationData">Input data for operation.</param>
        /// <returns>Task encapsulating storage results object.</returns>
        public virtual Task<IStoreResults> ExecuteOperationAsync(string operationName, XElement operationData)
        {
            return SqlUtils.WithSqlExceptionHandlingAsync<IStoreResults>(async () =>
            {
                SqlResults results = new SqlResults();

                using (SqlCommand cmd = _conn.CreateCommand())
                using (XmlReader input = operationData.CreateReader())
                {
                    cmd.Transaction = _tran;
                    cmd.CommandText = operationName;
                    cmd.CommandType = CommandType.StoredProcedure;

                    SqlUtils.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, -1, new SqlXml(input));

                    SqlParameter result = SqlUtils.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        await results.FetchAsync(reader).ConfigureAwait(false);
                    }

                    // Output parameter will be used to specify the outcome.
                    results.Result = (StoreResult)result.Value;
                }

                return results;
            });
        }

        /// <summary>
        /// Executes the given command.
        /// </summary>
        /// <param name="command">Command to execute.</param>
        /// <returns>Storage results object.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public virtual IStoreResults ExecuteCommandSingle(StringBuilder command)
        {
            return SqlUtils.WithSqlExceptionHandling<IStoreResults>(() =>
            {
                SqlResults results = new SqlResults();

                using (SqlCommand cmd = _conn.CreateCommand())
                {
                    cmd.Transaction = _tran;
                    cmd.CommandText = command.ToString();
                    cmd.CommandType = CommandType.Text;

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        results.Fetch(reader);
                    }
                }

                return results;
            });
        }

        /// <summary>
        /// Executes the given set of commands.
        /// </summary>
        /// <param name="commands">Collection of commands to execute.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public virtual void ExecuteCommandBatch(IEnumerable<StringBuilder> commands)
        {
            foreach (StringBuilder batch in commands)
            {
                SqlUtils.WithSqlExceptionHandling(() =>
                {
                    using (SqlCommand cmd = _conn.CreateCommand())
                    {
                        cmd.Transaction = _tran;
                        cmd.CommandText = batch.ToString();
                        cmd.CommandType = CommandType.Text;

                        cmd.ExecuteNonQuery();
                    }
                });
            }
        }

        #region IDisposable

        /// <summary>
        /// Disposes the object. Commits or rolls back the transaction.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Performs actual Dispose of resources.
        /// </summary>
        /// <param name="disposing">Whether the invocation was from IDisposable.Dipose method.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_tran != null)
                {
                    SqlUtils.WithSqlExceptionHandling(() =>
                    {
                        try
                        {
                            if (this.Success)
                            {
                                _tran.Commit();
                            }
                            else
                            {
                                _tran.Rollback();
                            }
                        }
                        catch (InvalidOperationException)
                        {
                            // We ignore zombied transactions.
                        }
                        finally
                        {
                            _tran.Dispose();
                            _tran = null;
                        }
                    });
                }
            }
        }

        #endregion IDisposable
    }
}
