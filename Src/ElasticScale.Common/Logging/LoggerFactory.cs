//******************************************************************************
// Copyright (c) Microsoft Corporation
//
// @File: LoggerFactory.cs
//
// @Owner: raveeram
// @Test:
//
// Purpose: Generic pluggable logger factory for retrieving configured
// logging objects
//
//******************************************************************************

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

        private static TraceSourceFactory _loggerFactory = null;
        
        private static object _lockObj = new object();
      
        private static ILogFactory _factory 
        { 
            get 
            {
                if (_loggerFactory == null)
                {
                    lock (_lockObj)
                    {
                        if (_loggerFactory == null)
                        {
                            _loggerFactory = new TraceSourceFactory();
                        }
                    }
                }

                return _loggerFactory;
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