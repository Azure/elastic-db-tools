// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Mutual exclusion construct for values.
    /// </summary>
    /// <typeparam name="T">Type of values.</typeparam>
    internal class ValueLock<T> : IDisposable
    {
        /// <summary>
        /// Global lock for mutual exclusion on the global dictionary of values.
        /// </summary>
        private static object s_lock = new object();

        /// <summary>
        /// Existing collection of values.
        /// </summary>
        private static Dictionary<T, RefCountedObject> s_locks = new Dictionary<T, RefCountedObject>();

        /// <summary>
        /// Value being locked.
        /// </summary>
        private T _value;

        /// <summary>
        /// Reference counter for the value.
        /// </summary>
        private RefCountedObject _valueLock;

        /// <summary>
        /// Constructs an instace of lock on input value and locks it.
        /// </summary>
        /// <param name="value">Value being locked.</param>
        internal ValueLock(T value)
        {
            ExceptionUtils.DisallowNullArgument(value, "value");

            _value = value;

            lock (s_lock)
            {
                if (!s_locks.ContainsKey(_value))
                {
                    _valueLock = new RefCountedObject();
                    s_locks.Add(_value, _valueLock);
                }
                else
                {
                    _valueLock = s_locks[_value];
                    _valueLock.AddRef();
                }
            }

            Monitor.Enter(_valueLock);
        }

        /// <summary>
        /// Releases the reference on the value, unlocks it if reference
        /// count reaches 0.
        /// </summary>
        public void Dispose()
        {
            Monitor.Exit(_valueLock);

            lock (s_lock)
            {
                // Impossible to have acquired a lock without a name.
                Debug.Assert(s_locks.ContainsKey(_value));

                if (_valueLock.Release() == 0)
                {
                    s_locks.Remove(_value);
                }
            }
        }

        /// <summary>
        /// Reference counter implementation.
        /// </summary>
        private class RefCountedObject
        {
            /// <summary>
            /// Number of references.
            /// </summary>
            private int _refCount;

            /// <summary>
            /// Instantiates the reference counter, initally set to 1.
            /// </summary>
            internal RefCountedObject()
            {
                _refCount = 1;
            }

            /// <summary>
            /// Increments reference count.
            /// </summary>
            internal void AddRef()
            {
                _refCount++;
            }

            /// <summary>
            /// Decrements the reference count.
            /// </summary>
            /// <returns>New value of reference count.</returns>
            internal int Release()
            {
                return --_refCount;
            }
        }
    }

    /// <summary>
    /// Implementation of name locks. Allows mutual exclusion on names.
    /// </summary>
    internal sealed class NameLock : ValueLock<string>
    {
        /// <summary>
        /// Instantiates a name lock with given name and acquires the name lock.
        /// </summary>
        /// <param name="name">Given name.</param>
        internal NameLock(string name)
            : base(name)
        {
        }
    }

    /// <summary>
    /// Implementation of Id locks. Allows mutual exclusion on Ids.
    /// </summary>
    internal sealed class IdLock : ValueLock<Guid>
    {
        /// <summary>
        /// Instantiates an Id lock with given Id and acquires the name lock.
        /// </summary>
        /// <param name="id">Given id.</param>
        internal IdLock(Guid id)
            : base(id)
        {
        }
    }
}
