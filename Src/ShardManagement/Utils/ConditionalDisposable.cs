using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// A disposable object which opts-out of disposing the inner disposable
    /// only when instructed by the caller.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class ConditionalDisposable<T> : IDisposable where T : IDisposable
    {
        /// <summary>
        /// Inner disposable object.
        /// </summary>
        private T innerDispoable;

        /// <summary>
        /// Constructor which takes an inner disposable object.
        /// </summary>
        /// <param name="innerDisposable"></param>
        public ConditionalDisposable(T innerDisposable)
        {
            this.innerDispoable = innerDisposable;
        }

        /// <summary>
        /// Used for notifying about disposable decision on inner object.
        /// </summary>
        internal bool DoNotDispose
        {
            get;
            set;
        }

        /// <summary>
        /// Disposes the inner object if DoNotDispose is set to false.
        /// </summary>
        public void Dispose()
        {
            if (!this.DoNotDispose)
            {
                this.innerDispoable.Dispose();
            }
        }
    }
}
