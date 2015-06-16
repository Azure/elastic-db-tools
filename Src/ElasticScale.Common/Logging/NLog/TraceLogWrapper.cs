#region "Usings"
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
#endregion

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Common
{
    /// <summary>
    /// System.Diagnostics Trace implementation of the ILogger interface
    /// </summary>
    public class TraceLogWrapper : ILogger
    {
        public void Info(object message)
        {
            System.Diagnostics.Trace.TraceInformation(message.ToString());
        }

        public void Info(string fmt, params object[] vars)
        {
            System.Diagnostics.Trace.TraceInformation(fmt, vars);            
        }

        public void Info(Exception exception, string fmt, params object[] vars)
        {
            var str = String.Format(fmt, vars);
            System.Diagnostics.Trace.TraceInformation("{0}: {1}", str, exception.ToString());
        }

        public void Debug(object message)
        {
            System.Diagnostics.Trace.WriteLine(message.ToString());
        }

        public void Debug(string fmt, params object[] vars)
        {
            System.Diagnostics.Trace.WriteLine(String.Format(fmt, vars));
        }

        public void Debug(Exception exception, string fmt, params object[] vars)
        {
            var str = String.Format(fmt, vars);
            System.Diagnostics.Trace.TraceWarning("{0}: {1}", str, exception.ToString());
        }

        public void Warning(object message)
        {
            System.Diagnostics.Trace.TraceWarning(message.ToString());
        }

        public void Warning(Exception exception, object message)
        {            
            System.Diagnostics.Trace.TraceWarning("{0}: {1}", 
                message.ToString(), exception.ToString());
        }

        public void Warning(string fmt, params object[] vars)
        {
            System.Diagnostics.Trace.TraceWarning(fmt, vars);
        }

        public void Warning(Exception exception, string fmt, params object[] vars)
        {
            var str = String.Format(fmt, vars);
            System.Diagnostics.Trace.TraceWarning("{0}: {1}", str, exception.ToString());
        }

        public void Error(object message)
        {
            System.Diagnostics.Trace.TraceError(message.ToString());
        }

        public void Error(Exception exception, object message)
        {
            System.Diagnostics.Trace.TraceError("{0}: {1}",
                message.ToString(), exception.ToString());
        }

        public void Error(string fmt, params object[] vars)
        {
            System.Diagnostics.Trace.TraceError(fmt, vars);
        }

        public void Error(Exception exception, string fmt, params object[] vars)
        {
            var str = String.Format(fmt, vars);
            System.Diagnostics.Trace.TraceError("{0}: {1}", str, exception.ToString());
        }

        public void Fatal(object message)
        {
            System.Diagnostics.Trace.TraceError(message.ToString());
        }

        public void Fatal(Exception exception, object message)
        {
            System.Diagnostics.Trace.TraceError("{0}: {1}",
                message.ToString(), exception.ToString());
        }

        public void Fatal(string fmt, params object[] vars)
        {
            System.Diagnostics.Trace.TraceError(fmt, vars);
        }

        public void Fatal(Exception exception, string fmt, params object[] vars)
        {
            var str = String.Format(fmt, vars);
            System.Diagnostics.Trace.TraceError("{0}: {1}", str, exception.ToString());
        }

        public void Trace(object message, Action action, TimeSpan threshold, object parameter = null)
        {
            System.Diagnostics.Trace.WriteLine(message.ToString());
        }

        public void Trace(object message)
        {
            System.Diagnostics.Trace.WriteLine(message.ToString());
        }

        public Guid TraceIn(string method)
        {
            Guid g = Guid.NewGuid();
            System.Diagnostics.Trace.WriteLine(String.Format(
                "START - {0} - {1}", method, g.ToString()));
            return g;
        }

        public void TraceOut(Guid eventId, string method)
        {
            System.Diagnostics.Trace.WriteLine(String.Format(
                "STOP - {0} - {1}", method, eventId.ToString()));
        }

        public Guid TraceIn(string method, string properties)
        {
             Guid g = Guid.NewGuid();
            System.Diagnostics.Trace.WriteLine(String.Format(
                "START - {0} - {1}", method, g.ToString()));
            return g;
        }

        public void TraceOut(Guid eventId, string method, string properties)
        {
            System.Diagnostics.Trace.WriteLine(String.Format(
                "STOP - {0} - {1}", method, eventId.ToString()));
        }

        public Guid TraceIn(string method, string fmt, params object[] vars)
        {
            Guid g = Guid.NewGuid();
            System.Diagnostics.Trace.WriteLine(String.Format(
                "START - {0} - {1}", method, g.ToString()));
            return g;
        }

        public void TraceOut(Guid eventId, string method, string fmt, params object[] vars)
        {
            System.Diagnostics.Trace.WriteLine(String.Format(
                "STOP - {0} - {1}", method, eventId.ToString()));
        }


        public void TraceApi(string method, TimeSpan timespan)
        {
            System.Diagnostics.Trace.WriteLine(String.Format(
                "EXEC - {0} - {1}", method, Guid.NewGuid().ToString()));
        }

        public void TraceApi(string method, TimeSpan timespan, string properties)
        {
            System.Diagnostics.Trace.WriteLine(String.Format(
                "EXEC - {0} - {1}", method, Guid.NewGuid().ToString()));
        }

        public void TraceApi(string method, TimeSpan timespan, string fmt, params object[] vars)
        {
            System.Diagnostics.Trace.WriteLine(String.Format(
                "EXEC - {0} - {1}", method, Guid.NewGuid().ToString()));
        }
    }
}
