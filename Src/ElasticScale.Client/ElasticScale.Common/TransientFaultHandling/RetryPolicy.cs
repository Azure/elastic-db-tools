// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Defines the retry policy to use when executing commands.
    /// </summary>
    internal sealed class RetryPolicy
    {
        /// <summary>
        /// Retry policy that tries upto 5 times with exponential backoff before giving up
        /// </summary>
        private static readonly Lazy<RetryPolicy> s_defaultRetryStrategy =
            new Lazy<RetryPolicy>(() =>
                new RetryPolicy(
                    retryCount: 5,
                    minBackOff: TransientFaultHandling.RetryStrategy.DefaultMinBackoff,
                    maxBackOff: TransientFaultHandling.RetryStrategy.DefaultMaxBackoff,
                    deltaBackOff: TransientFaultHandling.RetryStrategy.DefaultClientBackoff),
                    true);

        ///<summary>
        /// Initializes an instance of the <see cref="RetryPolicy"/> class
        ///</summary>
        /// <param name="retryCount">The number of retry attempts.</param>
        /// <param name="minBackOff">Minimum backoff time for exponential backoff policy.</param>
        /// <param name="maxBackOff">Maximum backoff time for exponential backoff policy.</param>
        /// <param name="deltaBackOff">Delta backoff time for exponential backoff policy.</param>
        public RetryPolicy(int retryCount, TimeSpan minBackOff, TimeSpan maxBackOff, TimeSpan deltaBackOff)
        {
            this.RetryCount = (retryCount < 0) ? 0 : retryCount;
            this.MinBackOff = (minBackOff < TimeSpan.Zero) ? TimeSpan.Zero : minBackOff;
            this.MaxBackOff = (maxBackOff < TimeSpan.Zero) ? TimeSpan.Zero : maxBackOff;
            this.DeltaBackOff = (deltaBackOff < TimeSpan.Zero) ? TimeSpan.Zero : deltaBackOff;
        }

        /// <summary>
        /// Gets the default retry policy.
        /// </summary>
        /// <remarks>
        /// 5 retries at 1 second intervals.
        /// </remarks>
        public static RetryPolicy DefaultRetryPolicy
        {
            get
            {
                return s_defaultRetryStrategy.Value;
            }
        }

        /// <summary>
        /// Gets the number of retries
        /// </summary>
        public int RetryCount
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets minimum backoff time
        /// </summary>
        public TimeSpan MinBackOff
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets maximum backoff time
        /// </summary>
        public TimeSpan MaxBackOff
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets value used to calculate random delta in the exponential delay between retries.
        /// <remarks>Time delta for next retry attempt is 2^(currentRetryCount - 1) * random value between 80% and 120% of DeltaBackOff</remarks>
        /// </summary>
        public TimeSpan DeltaBackOff
        {
            get;
            private set;
        }

        /// <summary>
        /// Marshals this instance into the TFH library RetryStrategy type.
        /// </summary>
        /// <returns>The RetryStrategy</returns>
        internal TransientFaultHandling.RetryStrategy GetRetryStrategy()
        {
            return new TransientFaultHandling.ExponentialBackoff(this.RetryCount, this.MinBackOff, this.MaxBackOff, this.DeltaBackOff);
        }

        /// <summary>
        /// String representation of <see cref="RetryPolicy"/>
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("RetryCount: {0}, , MinBackoff: {1}, MaxBackoff: {2}, DeltaBackoff: {3}", this.RetryCount, this.MinBackOff, this.MaxBackOff, this.DeltaBackOff);
        }
    }
}
