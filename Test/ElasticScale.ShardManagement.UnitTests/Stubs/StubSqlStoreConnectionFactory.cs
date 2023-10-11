// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;

/// <summary>
/// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.SqlStoreConnectionFactory
/// </summary>
[DebuggerDisplay("Stub of SqlStoreConnectionFactory")]
[DebuggerNonUserCode]
internal class StubSqlStoreConnectionFactory : SqlStoreConnectionFactory
{
    /// <summary>
    /// Sets the stub of SqlStoreConnectionFactory.GetConnection(StoreConnectionKind kind, SqlStoreConnectionInfo connectionInfo)
    /// </summary>
    internal Func<StoreConnectionKind, SqlConnectionInfo, IStoreConnection> GetConnectionStoreConnectionKindString;

    /// <summary>
    /// Sets the stub of SqlStoreConnectionFactory.GetUserConnection(SqlStoreConnectionInfo connectionInfo)
    /// </summary>
    internal Func<SqlConnectionInfo, IUserStoreConnection> GetUserConnectionString;
    private IStubBehavior ___instanceBehavior;

    /// <summary>
    /// Gets or sets a value that indicates if the base method should be called instead of the fallback behavior
    /// </summary>
    public bool CallBase { get; set; }

    /// <summary>
    /// Gets or sets the instance behavior.
    /// </summary>
    public IStubBehavior InstanceBehavior
    {
        get => StubBehaviors.GetValueOrCurrent(___instanceBehavior);
        set => ___instanceBehavior = value;
    }

    /// <summary>
    /// Initializes a new instance
    /// </summary>
    public StubSqlStoreConnectionFactory() => InitializeStub();

    /// <summary>
    /// Sets the stub of SqlStoreConnectionFactory.GetConnection(StoreConnectionKind kind, SqlStoreConnectionInfo connectionInfo)
    /// </summary>
    public override IStoreConnection GetConnection(
        StoreConnectionKind kind,
        SqlConnectionInfo connectionInfo)
    {
        var func1 = GetConnectionStoreConnectionKindString;
        return func1 != null
            ? func1(kind, connectionInfo)
            : CallBase
            ? base.GetConnection(kind, connectionInfo)
            : InstanceBehavior.Result<StubSqlStoreConnectionFactory, IStoreConnection>(this, "GetConnection");
    }


    /// <summary>
    /// Sets the stub of SqlStoreConnectionFactory.GetUserConnection(SqlStoreConnectionInfo connectionInfo)
    /// </summary>
    public override IUserStoreConnection GetUserConnection(SqlConnectionInfo connectionInfo)
    {
        var func1 = GetUserConnectionString;
        return func1 != null
            ? func1(connectionInfo)
            : CallBase
            ? base.GetUserConnection(connectionInfo)
            : InstanceBehavior.Result<StubSqlStoreConnectionFactory, IUserStoreConnection>(this, "GetUserConnection");
    }

    /// <summary>
    /// Initializes a new instance of type StubSqlStoreConnectionFactory
    /// </summary>
    private void InitializeStub()
    {
    }
}
