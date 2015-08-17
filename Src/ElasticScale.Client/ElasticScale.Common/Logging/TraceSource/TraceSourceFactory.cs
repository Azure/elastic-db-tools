// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose: Concrete implementation of ILogFactory that creates trace sources

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
        private static readonly SourceLevels s_defaultSourceLevels = SourceLevels.Information;

        /// <summary>
        /// The default <see cref="TraceSourceWrapper"/> instance
        /// </summary>
        private readonly TraceSourceWrapper _defaultDianosticsTraceSource;

        /// <summary>
        /// Keeps track of various TraceSources
        /// </summary>
        private readonly ConcurrentDictionary<string, Lazy<TraceSourceWrapper>> _traceSourceDictionary;

        /// <summary>
        /// Initializes an instance of the <see cref="TraceSourceFactory"/> class
        /// </summary>
        public TraceSourceFactory()
        {
            _traceSourceDictionary = new ConcurrentDictionary<string, Lazy<TraceSourceWrapper>>();

            _defaultDianosticsTraceSource = new TraceSourceWrapper(DefaultTraceSourceName, s_defaultSourceLevels);
            _traceSourceDictionary.TryAdd(DefaultTraceSourceName, new Lazy<TraceSourceWrapper>(() => _defaultDianosticsTraceSource));
        }

        /// <summary>
        /// Returns the default <see cref="TraceSourceWrapper"/> instance
        /// </summary>
        /// <returns>An instance of <see cref="TraceSourceWrapper"/></returns>
        public ILogger Create()
        {
            return _defaultDianosticsTraceSource;
        }

        /// <summary>
        /// Creates and returns an instance of <see cref="TraceSourceWrapper"/>
        /// </summary>
        /// <param name="name">The name of the TraceSource</param>
        /// <returns></returns>
        public ILogger Create(string name)
        {
            Lazy<TraceSourceWrapper> traceSource;

            if (!_traceSourceDictionary.TryGetValue(name, out traceSource))
            {
                traceSource = _traceSourceDictionary.GetOrAdd(name,
                    k => new Lazy<TraceSourceWrapper>(() => new TraceSourceWrapper(name),
                        System.Threading.LazyThreadSafetyMode.ExecutionAndPublication));
            }

            return traceSource.Value;
        }

        // TODO : Add timer based runtime config refresh
    }
}
