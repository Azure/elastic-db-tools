// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Defines the retry behavior to use for detecting transient errors.
    /// </summary>
    public sealed class RetryBehavior
    {
        /// <summary>
        /// Retry policy that tries upto 5 times with a 1 second backoff before giving up
        /// </summary>
        private static readonly Lazy<RetryBehavior> s_defaultRetryBehavior =
            new Lazy<RetryBehavior>(() =>
                new RetryBehavior((e) => false));

        ///<summary>
        /// Initializes an instance of the <see cref="RetryBehavior"/> class
        ///</summary>
        ///<param name="transientErrorDetector">Function that detects transient errors given an exception.
        /// The function needs to return true for an exception that should be treated as transient.
        ///</param>
        public RetryBehavior(Func<Exception, bool> transientErrorDetector)
        {
            if (transientErrorDetector == null)
            {
                throw new ArgumentNullException("transientErrorDetector");
            }

            this.TransientErrorDetector = transientErrorDetector;
        }

        /// <summary>
        /// Gets the default retry behavior.
        /// </summary>
        /// <remarks>
        /// The default retry behavior has a built-in set of exceptions that are considered transient. 
        /// You may create and use a custom <see cref="RetryBehavior"/> object in order 
        /// to treat additional exceptions as transient.
        /// </remarks>
        public static RetryBehavior DefaultRetryBehavior
        {
            get
            {
                return s_defaultRetryBehavior.Value;
            }
        }

        /// <summary>
        /// Transient error detector predicate which decides whether a given exception is transient or not.
        /// </summary>
        internal Func<Exception, bool> TransientErrorDetector
        {
            get;
            private set;
        }
    }
}
