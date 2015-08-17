// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Simple class that encapsulates an ILogger and 
    /// allows for custom tracing over an ILogger
    /// </summary>
    internal static class TraceHelper
    {
        /// <summary>
        /// The ILogger for the ShardMapManager
        /// </summary>
        private static readonly Lazy<ILogger> s_logger = new
            Lazy<ILogger>(() => LoggerFactory.GetLogger(TraceSourceConstants.ShardManagementTraceSource));

        // The tracer
        public static ILogger Tracer
        {
            get
            {
                return s_logger.Value;
            }
        }

        /// <summary>
        /// Helper to trace at the Verbose TraceLevel to the ILogger
        /// </summary>
        /// <param name="logger">The logger</param>
        /// <param name="componentName">The component name</param>
        /// <param name="methodName">The method name</param>
        /// <param name="message">The formatted message</param>
        /// <param name="vars">The args</param>
        internal static void TraceVerbose(this ILogger logger,
            string componentName,
            string methodName,
            string message,
            params object[] vars)
        {
            string fmtMessage = string.Format(message, vars);
            logger.Verbose("{0}.{1}; {2}; ActivityId: {3};", componentName, methodName, fmtMessage, Trace.CorrelationManager.ActivityId);
        }

        /// <summary>
        /// /// Helper to trace at the Information TraceLevel to the ILogger
        /// </summary>
        /// <param name="logger">The logger</param>
        /// <param name="componentName">The component name</param>
        /// <param name="methodName">The method name</param>
        /// <param name="message">The formatted message</param>
        /// <param name="vars">The args</param>
        internal static void TraceInfo(this ILogger logger,
            string componentName,
            string methodName,
            string message,
            params object[] vars)
        {
            string fmtMessage = string.Format(message, vars);
            logger.Info("{0}.{1}; {2}; ActivityId: {3};", componentName, methodName, fmtMessage, Trace.CorrelationManager.ActivityId);
        }

        /// <summary>
        /// Helper to trace at the Warning TraceLevel to the ILogger
        /// </summary>
        /// <param name="logger">The logger</param>
        /// <param name="componentName">The component name</param>
        /// <param name="methodName">The method name</param>
        /// <param name="message">The formatted message</param>
        /// <param name="vars">The args</param>
        internal static void TraceWarning(this ILogger logger,
            string componentName,
            string methodName,
            string message,
            params object[] vars)
        {
            string fmtMessage = string.Format(message, vars);
            logger.Warning("{0}.{1}; {2}; ActivityId: {3};", componentName, methodName, fmtMessage, Trace.CorrelationManager.ActivityId);
        }

        /// <summary>
        /// Helper to trace at the Error TraceLevel to the ILogger
        /// </summary>
        /// <param name="logger">The logger</param>
        /// <param name="componentName">The component name</param>
        /// <param name="methodName">The method name</param>
        /// <param name="message">The formatted message</param>
        /// <param name="vars">The args</param>
        internal static void TraceError(this ILogger logger,
            string componentName,
            string methodName,
            string message,
            params object[] vars)
        {
            string fmtMessage = string.Format(message, vars);
            logger.Error("{0}.{1}; {2}; ActivityId: {3};", componentName, methodName, fmtMessage, Trace.CorrelationManager.ActivityId);
        }
    }
}
