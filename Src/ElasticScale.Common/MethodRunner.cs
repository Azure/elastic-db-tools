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
    /// Abstract base class providing encapsulation methods for executing
    /// functions.  Wrappers for retry, telemetry, etc.
    /// </summary>
    public abstract class MethodRunner
    {
        #region "Logging"

        /// <summary>
        /// Lazy initialization for the logging channel
        /// </summary>
        private readonly Lazy<ILogger> LoggerLazy;

        /// <summary>
        /// Internal logging object; lazy initialized
        /// </summary>
        protected ILogger Logger
        {
            get
            {
                return LoggerLazy.Value;
            }
        }

        #endregion

        #region "Private Variables"
        private readonly bool _timeMethods;
        private readonly string _componentName;
        private readonly string _logName;
        #endregion

        protected bool TimeMethods { get { return _timeMethods; } }
        protected string ComponentName { get { return _componentName; } } 


        /// <summary>
        /// Instantiate the underlying policies and logging logging 
        /// </summary>
        /// <param name="logName"></param>
        protected MethodRunner(string componentName, string logName = "Log", bool timeMethods = true)
        {
            LoggerLazy = new Lazy<ILogger>(() => { return LoggerFactory.GetLogger(logName); });
            _timeMethods = timeMethods;
            _componentName = componentName;
            _logName = logName;
        }

        /// <summary>
        /// Given a method which does not return any data, time and trace the execution of the method 
        /// </summary>
        /// <param name="func"></param>
        /// <param name="methodName"></param>
        /// <param name="vars"></param>
        protected virtual void Execute(Action func, string methodName, params object[] vars)
        {
            try
            {
                Stopwatch stopWatch = null;
                if (_timeMethods && !String.IsNullOrEmpty(methodName))
                {
                    stopWatch = Stopwatch.StartNew();
                }
                func();
                if (stopWatch != null)
                {
                    stopWatch.Stop();
                    Logger.TraceApi(String.Format("{0}.{1}", _componentName, methodName), stopWatch.Elapsed);
                }

            }
            catch (Exception ex)
            {
                Logger.Warning(ex, "Error in {0}:{1}", _componentName, methodName);
                throw;                
            }
        }
    }
}
