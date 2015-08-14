// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    /// <summary>
    /// Provides the transient error detection logic for transient faults that are specific to cross shard query.
    /// </summary>
    internal sealed class MultiShardQueryTransientErrorDetectionStrategy : TransientFaultHandling.ITransientErrorDetectionStrategy
    {
        /// <summary>
        /// Delegate used for detecting transient faults.
        /// </summary>
        private Func<Exception, bool> _transientFaultDetector;

        /// <summary>
        /// Standard transient error detection strategy.
        /// </summary>
        private TransientFaultHandling.SqlDatabaseTransientErrorDetectionStrategy _standardDetectionStrategy;

        /// <summary>
        /// Creates a new instance of transient error detection strategy for Shard map manager.
        /// </summary>
        /// <param name="retryBehavior">Behavior for detecting transient errors.</param>
        internal MultiShardQueryTransientErrorDetectionStrategy(RetryBehavior retryBehavior)
        {
            _standardDetectionStrategy = new TransientFaultHandling.SqlDatabaseTransientErrorDetectionStrategy();
            _transientFaultDetector = retryBehavior.TransientErrorDetector;
        }

        /// <summary>
        /// Determines whether the specified exception represents a transient failure that can be compensated by a retry.
        /// </summary>
        /// <param name="ex">The exception object to be verified.</param>
        /// <returns>true if the specified exception is considered as transient; otherwise, false.</returns>
        public bool IsTransient(Exception ex)
        {
            return _standardDetectionStrategy.IsTransient(ex) || _transientFaultDetector(ex);
        }
    }
}
