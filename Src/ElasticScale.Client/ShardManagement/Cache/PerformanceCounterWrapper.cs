// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Wrapper class around PerformanceCounter to catch and trace all exceptions.
    /// </summary>
    class PerformanceCounterWrapper : IDisposable
    {
        private static ILogger Tracer
        {
            get
            {
                return TraceHelper.Tracer;
            }
        }

        internal bool _isValid;

        private string _counterName;

        private string _instanceName;

        private string _categoryName;

        /// <summary>
        /// Create and wrap performance counter object.
        /// </summary>
        /// <param name="categoryName">Counter catatory.</param>
        /// <param name="instanceName">Instance name to create.</param>
        /// <param name="counterName">Counter name to create.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public PerformanceCounterWrapper(string categoryName, string instanceName, string counterName)
        {
            _isValid = false;
            this._categoryName = categoryName;
            this._instanceName = instanceName;
            this._counterName = counterName;
        }

        /// <summary>
        /// Close performance counter, if initialized earlier. Counter will be removed when we delete instance.
        /// </summary>
        public void Close()
        {
        }

        /// <summary>
        /// Increment counter value by 1.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void Increment()
        {
        }

        /// <summary>
        /// Set raw value of this performance counter.
        /// </summary>
        /// <param name="value">Value to set.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void SetRawValue(long value)
        {
        }

        /// <summary>
        /// Log exceptions using Tracer
        /// </summary>
        /// <param name="method">Method name</param>
        /// <param name="message">Custom message</param>
        /// <param name="e">Exception to trace out</param>
        private static void TraceException(string method, string message, Exception e)
        {
            Tracer.TraceWarning(
                TraceSourceConstants.ComponentNames.PerfCounter,
                method,
                string.Format("Message: {0}. Exception: {1}", message, e.Message));
        }

        /// <summary>
        /// Dispose performance counter.
        /// </summary>
        public void Dispose()
        {
        }
    }
}