// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Shard management retrying event arguments
    /// </summary>
    internal sealed class RetryingEventArgs : EventArgs
    {
        /// <summary>
        /// Initializes new instance of <see cref="RetryingEventArgs"/> class.
        /// </summary>
        /// <param name="arg">RetryingEventArgs from RetryPolicy.Retrying event.</param>
        internal RetryingEventArgs(TransientFaultHandling.RetryingEventArgs arg)
        {
            this.CurrentRetryCount = arg.CurrentRetryCount;
            this.Delay = arg.Delay;
            this.LastException = arg.LastException;
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
