using System;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Scope for a write lock.
    /// </summary>
    internal class WriteLockScope : IDisposable
    {
        /// <summary>
        /// The lock object on which read lock is held.
        /// </summary>
        private ReaderWriterLockSlim _lock;

        /// <summary>
        /// Acquires the write lock.
        /// </summary>
        /// <param name="_lock">Lock to be acquired.</param>
        internal WriteLockScope(ReaderWriterLockSlim _lock)
        {
            this._lock = _lock;

            this._lock.EnterWriteLock();
        }

        /// <summary>
        /// Exits the locking scope.
        /// </summary>
        public void Dispose()
        {
            this._lock.ExitWriteLock();
        }
    }
}
