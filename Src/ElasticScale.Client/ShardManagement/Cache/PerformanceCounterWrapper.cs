// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        private PerformanceCounter _counter;

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

            // Check if counter exists in the specified category and then create its instance
            if (PerformanceCounterCategory.CounterExists(_counterName, _categoryName))
            {
                try
                {
                    _counter = new PerformanceCounter();
                    _counter.InstanceLifetime = PerformanceCounterInstanceLifetime.Process;
                    _counter.CategoryName = _categoryName;
                    _counter.InstanceName = _instanceName;
                    _counter.CounterName = _counterName;
                    _counter.ReadOnly = false;

                    _counter.RawValue = 0;

                    _isValid = true;
                }
                catch (Exception e)
                {
                    PerformanceCounterWrapper.TraceException("initialize", "Performance counter initialization failed, no data will be collected.", e);
                }
            }
            else
            {
                Tracer.TraceWarning(
                TraceSourceConstants.ComponentNames.PerfCounter,
                "initialize",
                "Performance counter {0} does not exist in shard management catagory.", counterName);
            }
        }

        /// <summary>
        /// Close performance counter, if initialized earlier. Counter will be removed when we delete instance.
        /// </summary>
        public void Close()
        {
            if (_isValid)
            {
                _counter.Close();
            }
        }

        /// <summary>
        /// Increment counter value by 1.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void Increment()
        {
            if (_isValid)
            {
                try
                {
                    _counter.Increment();
                }
                catch (Exception e)
                {
                    PerformanceCounterWrapper.TraceException("increment", "counter increment failed.", e);
                }
            }
        }

        /// <summary>
        /// Set raw value of this performance counter.
        /// </summary>
        /// <param name="value">Value to set.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void SetRawValue(long value)
        {
            if (_isValid)
            {
                try
                {
                    _counter.RawValue = value;
                }
                catch (Exception e)
                {
                    PerformanceCounterWrapper.TraceException("SetRawValue", "failed to set raw value", e);
                }
            }
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
            _counter.Dispose();
        }
    }
}