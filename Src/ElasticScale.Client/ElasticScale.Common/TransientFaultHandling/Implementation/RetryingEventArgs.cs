// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    using System;

    internal partial class TransientFaultHandling
    {
        /// <summary>
        /// Contains information that is required for the <see cref="RetryPolicy.Retrying"/> event.
        /// </summary>
        internal class RetryingEventArgs : EventArgs
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="RetryingEventArgs"/> class.
            /// </summary>
            /// <param name="currentRetryCount">The current retry attempt count.</param>
            /// <param name="delay">The delay that indicates how long the current thread will be suspended before the next iteration is invoked.</param>
            /// <param name="lastException">The exception that caused the retry conditions to occur.</param>
            public RetryingEventArgs(int currentRetryCount, TimeSpan delay, Exception lastException)
            {
                Guard.ArgumentNotNull(lastException, "lastException");

                this.CurrentRetryCount = currentRetryCount;
                this.Delay = delay;
                this.LastException = lastException;
            }

            /// <summary>
            /// Gets the current retry count.
            /// </summary>
            public int CurrentRetryCount { get; private set; }

            /// <summary>
            /// Gets the delay that indicates how long the current thread will be suspended before the next iteration is invoked.
            /// </summary>
            public TimeSpan Delay { get; private set; }

            /// <summary>
            /// Gets the exception that caused the retry conditions to occur.
            /// </summary>
            public Exception LastException { get; private set; }
        }
    }
}