// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Purpose:
// Mocks SqlConnection

using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests;

public class MockSqlConnection : DbConnection
{
    #region Global vars


    #endregion

    #region Ctors

    public MockSqlConnection(string connectionString, Action executeOnOpen)
    {
        ConnectionString = connectionString;
        ExecuteOnOpen = executeOnOpen;
    }

    #endregion

    #region Properties

    /// <summary>
    /// The current state of the connection
    /// Closed by default.
    /// </summary>
    public override ConnectionState State => MockConnectionState;

    public ConnectionState MockConnectionState { get; set; } = ConnectionState.Closed;

    /// <summary>
    /// The time in seconds to wait for connections
    /// to ALL shards to be opened.
    /// Default timeout is 300 seconds.
    /// </summary>
    /// <remarks>Value of 0 indicates that we wait forever</remarks>
    public new int ConnectionTimeout { get; set; }

    /// <summary>
    /// </summary>
    public override string ConnectionString { get; set; }

    /// <summary>
    /// The server version
    /// </summary>
    public override string ServerVersion => throw new NotImplementedException();

    /// <summary>
    /// the data source
    /// </summary>
    public override string DataSource => "SomeSource";

    public string SetDatabase { get; set; }

    /// <summary>
    /// Name of this database
    /// </summary>
    public override string Database => SetDatabase;

    /// <summary>
    /// Aciton to execute on opening the connection to be set by the test.
    /// </summary>
    public Action ExecuteOnOpen { get; set; }

    #endregion

    #region Suppported Methods

    /// <summary>
    /// </summary>
    /// <returns>mock sqlcommand instance</returns>
    public new MockSqlCommand CreateCommand() => new();

    /// <summary>
    /// Creates and returns a DbCommand associated with this connection
    /// </summary>
    /// <returns></returns>
    protected override DbCommand CreateDbCommand() => CreateCommand();

    /// <summary>
    /// Mocks opening a SqlConnection
    /// </summary>
    public override void Open()
    {
        MockConnectionState = ConnectionState.Open;

        ExecuteOnOpen();
    }

    /// <summary>
    /// Mocks opening a SqlConnection asynchronously
    /// </summary>
    /// <param name="cancellationToken">A cancellation token for the user to cancel this async call if necessary</param>
    /// <returns>A task that conveys failure/success in openining connections to shards</returns>
    public override Task OpenAsync(CancellationToken outerCancellationToken) => Task.Run(() =>
                                                                                             {
                                                                                                 outerCancellationToken.ThrowIfCancellationRequested();
                                                                                                 ExecuteOnOpen();
                                                                                                 MockConnectionState = ConnectionState.Open;
                                                                                                 outerCancellationToken.ThrowIfCancellationRequested();
                                                                                             });

    /// <summary>
    /// Closes this connection
    /// </summary>
    public override void Close() => MockConnectionState = ConnectionState.Closed;

    /// <summary>
    /// Disposes off any managed and unmanaged resources
    /// </summary>
    /// <param name="disposing"></param>
    protected override void Dispose(bool disposing) => base.Dispose(disposing);

    #endregion

    #region UnSupported DbConnection APIs

    /// <summary>
    /// </summary>
    /// <param name="isolationLevel"></param>
    /// <returns></returns>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

    /// <summary>
    /// </summary>
    /// <param name="databaseName"></param>
    public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();

    #endregion
}
