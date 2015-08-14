// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#region "Usings"

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
#endregion

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Definition of a generic logging interface to abstract 
    /// implementation details.  Includes api trace methods.
    /// </summary>
    internal interface ILogger
    {
        void Info(string message);
        void Info(string format, params object[] vars);

        void Verbose(string message);
        void Verbose(string format, params object[] vars);

        void Warning(string message);
        void Warning(Exception exception, string message);
        void Warning(string format, params object[] vars);
        void Warning(Exception exception, string format, params object[] vars);

        void Error(string message);
        void Error(Exception exception, string message);
        void Error(string format, params object[] vars);
        void Error(Exception exception, string format, params object[] vars);

        void Critical(string message);
        void Critical(Exception exception, string message);
        void Critical(string format, params object[] vars);
        void Critical(Exception exception, string format, params object[] vars);

        void TraceIn(string method, Guid activityId);
        void TraceOut(string method, Guid activityId);

        void TraceIn(string method, Guid activityId, string format, params object[] vars);
        void TraceOut(string method, Guid activityId, string format, params object[] vars);
    }
}
