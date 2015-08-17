// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Trace helper for CrossShardQuery

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    /// <summary>
    /// Trace helper for CrossShardQuery
    /// </summary>
    internal static class TraceHelper
    {
        /// <summary>
        /// The trace source name for cross shard query
        /// </summary>
        private const string MultiShardQueryTraceSourceName = "MultiShardQueryTraceSource";

        // Suppression rationale: prefixing a static member on a static class with m_ is fine.
        //
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1504:ReviewMisleadingFieldNames")]
        private static readonly Lazy<ILogger> s_logger = new
            Lazy<ILogger>(() => LoggerFactory.GetLogger(MultiShardQueryTraceSourceName));

        /// <summary>
        /// The tracer
        /// </summary>
        internal static ILogger Tracer
        {
            get
            {
                return s_logger.Value;
            }
        }

        internal static void TraceInfo(this ILogger logger,
            string methodName,
            string message,
            params object[] vars)
        {
            string fmtMessage = string.Format(message, vars);
            logger.Info("Method: {0}; {1}; ActivityId: {2};", methodName, fmtMessage, Trace.CorrelationManager.ActivityId);
        }

        internal static void TraceWarning(this ILogger logger,
            string methodName,
            string message,
            params object[] vars)
        {
            string fmtMessage = string.Format(message, vars);
            logger.Warning("Method: {0}; {1}; ActivityId: {2};", methodName, fmtMessage,
                Trace.CorrelationManager.ActivityId);
        }

        internal static void TraceError(this ILogger logger,
            string methodName,
            Exception ex,
            string message,
            params object[] vars)
        {
            string fmtMessage = string.Format(message, vars);
            logger.Error("Method: {0}; {1}; Exception: {2}; ActivityId: {3};", methodName, fmtMessage, ex.ToString(),
                Trace.CorrelationManager.ActivityId);
        }
    }
}
