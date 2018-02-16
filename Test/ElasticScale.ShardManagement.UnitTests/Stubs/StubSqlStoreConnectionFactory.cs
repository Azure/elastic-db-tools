﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    /// <summary>
    /// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.SqlStoreConnectionFactory
    /// </summary>
    [DebuggerDisplay("Stub of SqlStoreConnectionFactory")]
    [DebuggerNonUserCode]
    internal class StubSqlStoreConnectionFactory : SqlStoreConnectionFactory
    {
        /// <summary>
        /// Sets the stub of SqlStoreConnectionFactory.GetConnection(StoreConnectionKind kind, String connectionString)
        /// </summary>
        internal Func<StoreConnectionKind, string, IStoreConnection> GetConnectionStoreConnectionKindString;
        /// <summary>
        /// Sets the stub of SqlStoreConnectionFactory.GetUserConnection(String connectionString)
        /// </summary>
        internal Func<string, IUserStoreConnection> GetUserConnectionString;
        private bool ___callBase;
        private IStubBehavior ___instanceBehavior;

        /// <summary>
        /// Gets or sets a value that indicates if the base method should be called instead of the fallback behavior
        /// </summary>
        public bool CallBase
        {
            get
            {
                return this.___callBase;
            }
            set
            {
                this.___callBase = value;
            }
        }

        /// <summary>
        /// Gets or sets the instance behavior.
        /// </summary>
        public IStubBehavior InstanceBehavior
        {
            get
            {
                return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
            }
            set
            {
                this.___instanceBehavior = value;
            }
        }

        /// <summary>
        /// Initializes a new instance
        /// </summary>
        public StubSqlStoreConnectionFactory()
        {
            this.InitializeStub();
        }

        /// <summary>
        /// Sets the stub of SqlStoreConnectionFactory.GetConnection(StoreConnectionKind kind, String connectionString)
        /// </summary>
        public override IStoreConnection GetConnection(StoreConnectionKind kind, string connectionString)
        {
            Func<StoreConnectionKind, string, IStoreConnection> func1 = this.GetConnectionStoreConnectionKindString;
            if (func1 != null)
                return func1(kind, connectionString);
            if (this.___callBase)
                return base.GetConnection(kind, connectionString);
            return this.InstanceBehavior.Result<StubSqlStoreConnectionFactory, IStoreConnection>(this, "GetConnection");
        }

        /// <summary>
        /// Sets the stub of SqlStoreConnectionFactory.GetUserConnection(String connectionString)
        /// </summary>
        public override IUserStoreConnection GetUserConnection(string connectionString)
        {
            Func<string, IUserStoreConnection> func1 = this.GetUserConnectionString;
            if (func1 != null)
                return func1(connectionString);
            if (this.___callBase)
                return base.GetUserConnection(connectionString);
            return this.InstanceBehavior.Result<StubSqlStoreConnectionFactory, IUserStoreConnection>(this, "GetUserConnection");
        }

        /// <summary>
        /// Initializes a new instance of type StubSqlStoreConnectionFactory
        /// </summary>
        private void InitializeStub()
        {
        }
    }
}
