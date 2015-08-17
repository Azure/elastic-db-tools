// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose: System.Diagnostics.TraceSource implementation of the ILogger interface

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// System.Diagnostics TraceSource implementation of the ILogger interface
    /// </summary>
    internal class TraceSourceWrapper : ILogger
    {
        /// <summary>
        /// The trace source instance
        /// </summary>
        private TraceSource _traceSource;

        #region Ctors
        /// <summary>
        /// Creates an instance of the <see cref="TraceSourceWrapper"/>
        /// </summary>
        /// <param name="traceSourceName">The TraceSource name</param>
        public TraceSourceWrapper(string traceSourceName)
        {
            _traceSource = new TraceSource(traceSourceName);
        }

        /// <summary>
        /// Creates an instance of the <see cref="TraceSourceWrapper"/>
        /// </summary>
        /// <param name="traceSourceName">The TraceSource name</param>
        /// <param name="defaultLevel">The default TraceSource level to use</param>
        public TraceSourceWrapper(string traceSourceName, SourceLevels defaultLevel)
        {
            _traceSource = new TraceSource(traceSourceName, defaultLevel);
        }

        #endregion

        #region Information

        /// <summary>
        /// Traces an informational message to the trace source
        /// </summary>
        /// <param name="message">The trace source</param>
        public void Info(string message)
        {
            _traceSource.TraceInformation(message);
        }

        /// <summary>
        /// Traces an informational message to the trace source
        /// </summary>
        /// <param name="format">The format</param>
        /// <param name="vars">The args</param>
        public void Info(string format, params object[] vars)
        {
            _traceSource.TraceInformation(format, vars);
        }

        #endregion Information

        #region Verbose

        /// <summary>
        /// Traces a Verbose message to the trace source
        /// </summary>
        /// <param name="message"></param>
        public void Verbose(string message)
        {
            _traceSource.TraceEvent(TraceEventType.Verbose, 0, message);
        }

        /// <summary>
        /// Traces a Verbose message to the trace source
        /// </summary>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void Verbose(string format, params object[] vars)
        {
            _traceSource.TraceEvent(TraceEventType.Verbose, 0, format, vars);
        }

        #endregion

        #region Warning

        /// <summary>
        /// Traces a message at the Warning level to the trace source
        /// </summary>
        /// <param name="message"></param>
        public void Warning(string message)
        {
            _traceSource.TraceEvent(TraceEventType.Warning, 0, message);
        }

        /// <summary>
        /// Traces a message at the Warning level to the trace source
        /// </summary>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void Warning(string format, params object[] vars)
        {
            _traceSource.TraceEvent(TraceEventType.Warning, 0, format, vars);
        }

        /// <summary>
        /// Traces an exception and a message at the Warning trace level 
        /// to the trace source
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        public void Warning(Exception exception, string message)
        {
            _traceSource.TraceEvent(TraceEventType.Warning, 0, "{0}. Encountered exception: {1}", message, exception);
        }

        /// <summary>
        /// Traces an exception and a message at the Warning trace level 
        /// to the trace source
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void Warning(Exception exception, string format, params object[] vars)
        {
            string fmtMessage = string.Format(format, vars);
            _traceSource.TraceEvent(TraceEventType.Warning, 0, "{0}. Encountered exception: {1}", fmtMessage, exception);
        }

        #endregion Warning

        #region Error

        /// <summary>
        /// Traces the message at the Error trace level to the trace source
        /// </summary>
        /// <param name="message"></param>
        public void Error(string message)
        {
            _traceSource.TraceEvent(TraceEventType.Error, 0, message);
        }

        /// <summary>
        /// Traces the message at the Error trace level to the trace source
        /// </summary>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void Error(string format, params object[] vars)
        {
            _traceSource.TraceEvent(TraceEventType.Error, 0, format, vars);
        }

        /// <summary>
        /// Traces the exception and message
        /// at the Error level to the trace source
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        public void Error(Exception exception, string message)
        {
            _traceSource.TraceEvent(TraceEventType.Error, 0, "{0}. Encountered exception: {1}", message, exception);
        }

        /// <summary>
        /// Traces the exception and message at
        /// the Error level to the trace source
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void Error(Exception exception, string format, params object[] vars)
        {
            string fmtMessage = string.Format(format, vars);
            _traceSource.TraceEvent(TraceEventType.Error, 0, "{0}. Encountered exception: {1}", fmtMessage, exception);
        }

        #endregion Error

        #region Critical

        /// <summary>
        /// Traces the message at the Critical source level to the trace source
        /// </summary>
        /// <param name="message"></param>
        public void Critical(string message)
        {
            _traceSource.TraceEvent(TraceEventType.Critical, 0, message);
        }

        /// <summary>
        /// Traces the message at the Critical source level to the trace source
        /// </summary>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void Critical(string format, params object[] vars)
        {
            _traceSource.TraceEvent(TraceEventType.Critical, 0, format, vars);
        }

        /// <summary>
        /// Traces the message and exception at the Critical source level 
        /// to the trace source
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        public void Critical(Exception exception, string message)
        {
            _traceSource.TraceEvent(TraceEventType.Critical, 0, "{0}. Exception encountered: {1}", message, exception);
        }

        /// <summary>
        /// Traces the message and exception at the Critical source level 
        /// to the trace source
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void Critical(Exception exception, string format, params object[] vars)
        {
            string fmtMessage = string.Format(format, vars);
            _traceSource.TraceEvent(TraceEventType.Critical, 0, "{0}. Exception encountered: {1}", fmtMessage, exception);
        }

        #endregion Critical

        #region Enter/Exit

        /// <summary>
        /// Traces the entry of the method
        /// </summary>
        /// <param name="method"></param>
        /// <param name="activityId"></param>
        public void TraceIn(string method, Guid activityId)
        {
            _traceSource.TraceEvent(TraceEventType.Start, 0, "Start.{0}. ActivityId: {1}", method, activityId);
        }

        /// <summary>
        /// Traces the exit of the method
        /// </summary>
        /// <param name="method"></param>
        /// <param name="activityId"></param>
        public void TraceOut(string method, Guid activityId)
        {
            _traceSource.TraceEvent(TraceEventType.Stop, 0, "Stop.{0}. ActivityId: {1}", method, activityId);
        }

        /// <summary>
        /// Traces the entry of the method
        /// </summary>
        /// <param name="method"></param>
        /// <param name="activityId"></param>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void TraceIn(string method, Guid activityId, string format, params object[] vars)
        {
            string fmtMessage = string.Format(format, vars);
            _traceSource.TraceEvent(TraceEventType.Start, 0, "Start.{0}. {1}. ActivityId: {2}", method, fmtMessage, activityId);
        }

        /// <summary>
        /// Traces the exit of the method
        /// </summary>
        /// <param name="method"></param>
        /// <param name="activityId"></param>
        /// <param name="format"></param>
        /// <param name="vars"></param>
        public void TraceOut(string method, Guid activityId, string format, params object[] vars)
        {
            string fmtMessage = string.Format(format, vars);
            _traceSource.TraceEvent(TraceEventType.Stop, 0, "Stop.{0}. {1}. ActivityId: {2}", method, fmtMessage, activityId);
        }

        #endregion
    }
}
