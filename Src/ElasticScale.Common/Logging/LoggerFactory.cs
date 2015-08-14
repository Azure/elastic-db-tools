// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose: Generic pluggable logger factory for retrieving configured
// logging objects

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Generic pluggable logger factory for retrieving configured
    /// logging objects
    /// </summary>
    internal static class LoggerFactory
    {
        #region "Singleton Implementation"

        private static TraceSourceFactory s_loggerFactory = null;

        private static object s_lockObj = new object();

        private static ILogFactory _factory
        {
            get
            {
                if (s_loggerFactory == null)
                {
                    lock (s_lockObj)
                    {
                        if (s_loggerFactory == null)
                        {
                            s_loggerFactory = new TraceSourceFactory();
                        }
                    }
                }

                return s_loggerFactory;
            }
        }

        #endregion

        public static ILogger GetLogger<T>()
        {
            return _factory.Create(typeof(T).Name);
        }

        public static ILogger GetLogger()
        {
            return _factory.Create();
        }

        public static ILogger GetLogger(string logName)
        {
            return _factory.Create(logName);
        }
    }

    /// <summary>
    /// ILogFactory interface to be implemented for
    /// each logger
    /// </summary>
    internal interface ILogFactory
    {
        ILogger Create();
        ILogger Create(string name);
    }
}