//******************************************************************************
// Copyright (c) Microsoft Corporation
//
// @File: TraceSourceFactory.cs
//
// @Owner: raveeram
// @Test:
//
// Purpose: Concrete implementation of ILogFactory that creates trace sources
//
//******************************************************************************

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Concrete implementation of ILogFactory that creates trace sources
    /// </summary>
    internal sealed class TraceSourceFactory : ILogFactory
    {
        /// <summary>
        /// The default source name
        /// </summary>
        private const string DefaultTraceSourceName = "DiagnosticsTraceSource";

        /// <summary>
        /// The source level of the default tracer
        /// </summary>
        private static readonly SourceLevels DefaultSourceLevels = SourceLevels.Information;

        /// <summary>
        /// The default <see cref="TraceSourceWrapper"/> instance
        /// </summary>
        private readonly TraceSourceWrapper m_defaultDianosticsTraceSource;

        /// <summary>
        /// Keeps track of various TraceSources
        /// </summary>
        private readonly ConcurrentDictionary<string, Lazy<TraceSourceWrapper>> m_traceSourceDictionary;

        /// <summary>
        /// Initializes an instance of the <see cref="TraceSourceFactory"/> class
        /// </summary>
        public TraceSourceFactory()
        {
            m_traceSourceDictionary = new ConcurrentDictionary<string, Lazy<TraceSourceWrapper>>();

            m_defaultDianosticsTraceSource = new TraceSourceWrapper(DefaultTraceSourceName, DefaultSourceLevels);
            m_traceSourceDictionary.TryAdd(DefaultTraceSourceName, new Lazy<TraceSourceWrapper>(() => m_defaultDianosticsTraceSource));
        }

        /// <summary>
        /// Returns the default <see cref="TraceSourceWrapper"/> instance
        /// </summary>
        /// <returns>An instance of <see cref="TraceSourceWrapper"/></returns>
        public ILogger Create()
        {
            return m_defaultDianosticsTraceSource;
        }

        /// <summary>
        /// Creates and returns an instance of <see cref="TraceSourceWrapper"/>
        /// </summary>
        /// <param name="name">The name of the TraceSource</param>
        /// <returns></returns>
        public ILogger Create(string name)
        {
            Lazy<TraceSourceWrapper> traceSource;

            if (!m_traceSourceDictionary.TryGetValue(name, out traceSource))
            {
                traceSource = m_traceSourceDictionary.GetOrAdd(name,
                    k => new Lazy<TraceSourceWrapper>(() => new TraceSourceWrapper(name),
                        System.Threading.LazyThreadSafetyMode.ExecutionAndPublication));
            }

            return traceSource.Value;
        }

        // TODO : Add timer based runtime config refresh
    }
}
