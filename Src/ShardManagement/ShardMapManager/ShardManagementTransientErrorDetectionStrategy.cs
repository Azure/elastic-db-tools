using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Provides the transient error detection logic for transient faults that are specific to Shard map manager.
    /// </summary>
    internal sealed class ShardManagementTransientErrorDetectionStrategy : TransientFaultHandling.ITransientErrorDetectionStrategy
    {
        /// <summary>
        /// Delegate used for detecting transient faults.
        /// </summary>
        private Func<Exception, bool> transientFaultDetector;

        /// <summary>
        /// Creates a new instance of transient error detection strategy for Shard map manager.
        /// </summary>
        /// <param name="retryBehavior">User specified retry behavior.</param>
        internal ShardManagementTransientErrorDetectionStrategy(RetryBehavior retryBehavior)
        {
            this.transientFaultDetector = retryBehavior.TransientErrorDetector;
        }

        /// <summary>
        /// Determines whether the specified exception represents a transient failure that can be compensated by a retry.
        /// </summary>
        /// <param name="ex">The exception object to be verified.</param>
        /// <returns>true if the specified exception is considered as transient; otherwise, false.</returns>
        public bool IsTransient(Exception ex)
        {
            return SqlUtils.TransientErrorDetector(ex) || this.transientFaultDetector(ex);
        }
    }
}
