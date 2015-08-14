// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Scope for a read lock.
    /// </summary>
    internal class ReadLockScope : IDisposable
    {
        /// <summary>
        /// The lock object on which read lock is held.
        /// </summary>
        private ReaderWriterLockSlim _lock;

        /// <summary>
        /// Whether upgrade of read lock is possible.
        /// </summary>
        private bool _upgradable;

        /// <summary>
        /// Acquires the read lock.
        /// </summary>
        /// <param name="_lock">Lock to be acquired.</param>
        /// <param name="upgradable">Whether the lock is upgradable.</param>
        internal ReadLockScope(ReaderWriterLockSlim _lock, bool upgradable)
        {
            this._lock = _lock;

            _upgradable = upgradable;

            if (_upgradable)
            {
                this._lock.EnterUpgradeableReadLock();
            }
            else
            {
                this._lock.EnterReadLock();
            }
        }

        /// <summary>
        /// Upgrade the read lock to a write lock.
        /// </summary>
        /// <returns></returns>
        internal WriteLockScope Upgrade()
        {
            return new WriteLockScope(_lock);
        }

        /// <summary>
        /// Exits the locking scope.
        /// </summary>
        public void Dispose()
        {
            if (_upgradable)
            {
                _lock.ExitUpgradeableReadLock();
            }
            else
            {
                _lock.ExitReadLock();
            }
        }
    }
}
