#region "Usings"
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NLog;
#endregion

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Common
{    
    /// <summary>
    /// NLog adapter implementation of the ILogger interface, routing standard
    /// interface calls into NLog calls
    /// </summary>
    public class NLogWrapper : ILogger
    {
        private Logger _log;
        private Logger _trace;

        private static readonly string traceName = "ApiTrace";
        private static readonly Logger tracer = LogManager.GetLogger(traceName);

        public NLogWrapper(string logName)
        {            
            _log = LogManager.GetLogger(logName);
            _trace = tracer;
        }

        public void Info(object message)
        {
            _log.Info(message);
        }

        public void Info(string fmt, params object[] vars)
        {
            _log.Info(fmt, vars);
        }

        public void Info(Exception ex0, string fmt, params object[] vars)
        {
            var msg = String.Format(fmt, vars);
            _log.InfoException(msg, ex0);
        }

        public void Debug(object message)
        {
            _log.Debug(message);
        }

        public void Debug(string fmt, params object[] vars)
        {
            _log.Debug(fmt, vars);
        }

        public void Debug(Exception exception, string fmt, params object[] vars)
        {
            var msg = String.Format(fmt, vars);
            _log.DebugException(msg, exception);
        }

        public void Warning(object message)
        {
            _log.Warn(message);
        }

        public void Warning(Exception exception, object message)
        {
            _log.WarnException(message.ToString(), exception);
        }

        public void Warning(string fmt, params object[] vars)
        {
            _log.Warn(fmt, vars);
        }

        public void Warning(Exception exception, string fmt, params object[] vars)
        {
            var msg = String.Format(fmt, vars);
            _log.WarnException(msg, exception);
        }

        public void Error(object message)
        {
            _log.Error(message);
        }

        public void Error(Exception exception, object message)
        {
            _log.ErrorException(message.ToString(), exception);
        }

        public void Error(string fmt, params object[] vars)
        {
            _log.Error(fmt, vars);
        }

        public void Error(Exception exception, string fmt, params object[] vars)
        {
            var msg = String.Format(fmt, vars);
            _log.ErrorException(msg, exception);
        }

        public void Fatal(object message)
        {
            _log.Fatal(message);
        }

        public void Fatal(Exception exception, object message)
        {
            _log.FatalException(message.ToString(), exception);
        }

        public void Fatal(string fmt, params object[] vars)
        {
            _log.Fatal(fmt, vars);
        }

        public void Fatal(Exception exception, string fmt, params object[] vars)
        {
            var msg = String.Format(fmt, vars);
            _log.FatalException(string.Format(fmt, vars), exception);
        }

        public void Trace(object message, Action action, TimeSpan threshold, object parameter = null)
        {
            _log.Debug(message);
        }

        public void Trace(object message)
        {
            _log.Debug(message);
        }

        public Guid TraceIn(string method, string properties)
        {
            var eventId = Guid.NewGuid();
            var logEvent = new LogEventInfo()
            {
                Level = LogLevel.Debug,
                Message = properties,
                TimeStamp = DateTime.UtcNow,                  
            };
            logEvent.Properties.Add("api", method);
            logEvent.Properties.Add("eventid", eventId.ToString());
            logEvent.Properties.Add("action", "START");
            _trace.Log(logEvent);
            return eventId;
        }

        public Guid TraceIn(string method)
        {
            return TraceIn(method, "");
        }

        public Guid TraceIn(string method, string fmt, params object[] vars)
        {
            return TraceIn(method, String.Format(fmt, vars));
        }

        public void TraceOut(Guid eventId, string method)
        {
            TraceOut(eventId, method, "");
        }

        public void TraceOut(Guid eventId, string method, string properties)
        {
            var logEvent = new LogEventInfo()
            {
                Level = LogLevel.Debug,
                Message = properties,
                TimeStamp = DateTime.Now,                  
            };
            logEvent.Properties.Add("api", method);
            logEvent.Properties.Add("eventid", eventId.ToString());
            logEvent.Properties.Add("action", "STOP");
            _trace.Log(logEvent);
        }

        public void TraceOut(Guid eventId, string method, string fmt, params object[] vars)
        {
            TraceOut(eventId, method, String.Format(fmt, vars));
        }

        public void TraceApi(string method, TimeSpan timespan)
        {
            TraceApi(method, timespan, "");
        }

        public void TraceApi(string method, TimeSpan timespan, string properties)
        {
            var logEvent = new LogEventInfo()
            {
                Level = LogLevel.Debug,
                Message = String.Concat(timespan.ToString(), ":", properties),
                TimeStamp = DateTime.Now,                  
            };

            logEvent.Properties.Add("api", method);
            logEvent.Properties.Add("eventid", Guid.NewGuid());
            logEvent.Properties.Add("action", "EXEC");
            _trace.Log(logEvent);
        }

        public void TraceApi(string method, TimeSpan timespan, string fmt, params object[] vars)
        {
            TraceApi(method, timespan, string.Format(fmt, vars));
        }
    }
}
