#region "Usings"
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NLog.Targets;
using System.Diagnostics;
using NLog;
#endregion


namespace Microsoft.Azure.SqlDatabase.ElasticScale.Common
{
    /// <summary>
    /// Wrapper class to route events to System.Diagnostics
    /// Trace, which in turn routes to the Azure diagnostic
    /// listener (aka to write to Azure table storage)
    /// </summary>
    [Target("AzureEventLog")]
    public sealed class NLogTargetAzureTrace : TargetWithLayout
    {    
        protected override void Write(global::NLog.LogEventInfo logEvent)
        {
            var logLevel = logEvent.Level;
            var msg = Layout.Render(logEvent);

            if (logLevel >= LogLevel.Error)
                Trace.TraceError(msg);
            else if (logLevel >= LogLevel.Warn)
                Trace.TraceWarning(msg);
            else if (logLevel >= LogLevel.Info)
                Trace.TraceInformation(msg);
            else
                Trace.WriteLine(msg);
        }

        protected override void Write(global::NLog.Common.AsyncLogEventInfo logEvent)
        {
            var logLevel = logEvent.LogEvent.Level;
            var msg = Layout.Render(logEvent.LogEvent);

            if (logLevel >= LogLevel.Error)
                Trace.TraceError(msg);
            else if (logLevel >= LogLevel.Warn)
                Trace.TraceWarning(msg);
            else if (logLevel >= LogLevel.Info)
                Trace.TraceInformation(msg);
            else
                Trace.WriteLine(msg);
        }
    }
}
