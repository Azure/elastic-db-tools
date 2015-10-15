// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Types of store connections.
    /// </summary>
    internal enum StoreConnectionKind
    {
        /// <summary>
        /// Connection to GSM.
        /// </summary>
        Global,

        /// <summary>
        /// Connection to LSM Source Shard.
        /// </summary>
        LocalSource,

        /// <summary>
        /// Connection to LSM Target Shard (useful for Update Location operation only).
        /// </summary>
        LocalTarget
    }

    /// <summary>
    /// Instance of a store connection.
    /// </summary>
    internal interface IStoreConnection : IDisposable
    {
        /// <summary>
        /// Type of store connection.
        /// </summary>
        StoreConnectionKind Kind
        {
            get;
        }

        /// <summary>
        /// Open the store connection.
        /// </summary>
        void Open();

        /// <summary>
        /// Asynchronously opens the store connection.
        /// </summary>
        /// <returns>Task to await completion of the Open</returns>
        Task OpenAsync();

        /// <summary>
        /// Open the store connection, and acquire a lock on the store.
        /// </summary>
        /// <param name="lockId">Lock Id.</param>
        void OpenWithLock(Guid lockId);

        /// <summary>
        /// Closes the store connection.
        /// </summary>
        void Close();

        /// <summary>
        /// Closes the store connection after releasing lock.
        /// </summary>
        /// <param name="lockId">Lock Id.</param>
        void CloseWithUnlock(Guid lockId);

        /// <summary>
        /// Acquires a transactional scope on the connection.
        /// </summary>
        /// <param name="kind">Type of transaction scope.</param>
        /// <returns>Transaction scope on the store connection.</returns>
        IStoreTransactionScope GetTransactionScope(StoreTransactionScopeKind kind);
    }
}
