// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Base class for all objects in the cache, providing locking facilities.
    /// </summary>
    internal abstract class CacheObject : IDisposable
    {
        /// <summary>
        /// Lock object.
        /// </summary>
        private ReaderWriterLockSlim _lock;

        /// <summary>
        /// Whether this object has already been disposed
        /// </summary>
        private bool _disposed = false;

        /// <summary>
        /// Instantiates the underlying lock object.
        /// </summary>
        protected CacheObject()
        {
            _lock = new ReaderWriterLockSlim();
        }

        /// <summary>
        /// Obtains a read locking scope on the object.
        /// </summary>
        /// <param name="upgradable">Whether the read lock should be upgradable.</param>
        /// <returns>Read locking scope.</returns>
        internal ReadLockScope GetReadLockScope(bool upgradable)
        {
            return new ReadLockScope(_lock, upgradable);
        }

        /// <summary>
        /// Obtains a write locking scope on the object.
        /// </summary>
        /// <returns>Write locking scope.</returns>
        internal WriteLockScope GetWriteLockScope()
        {
            Debug.Assert(!_lock.IsUpgradeableReadLockHeld);

            return new WriteLockScope(_lock);
        }

        #region IDisposable

        /// <summary>
        /// Public dispose method. 
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Protected vitual member of the dispose pattern.
        /// </summary>
        /// <param name="disposing">Call came from Dispose.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _lock.Dispose();
                }

                _disposed = true;
            }
        }

        #endregion IDisposable
    }
}
