// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose: Utility class to set and restore the System.Diagnostics CorrelationManager
// ActivityId via the using pattern

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Utility class to set and restore the System.Diagnostics CorrelationManager
    /// ActivityId via the using pattern
    /// </summary>
    internal sealed class ActivityIdScope : IDisposable
    {
        /// <summary>
        /// The previous activity id that was in scope
        /// </summary>
        private readonly Guid _previousActivityId;

        /// <summary>
        /// Creates an instance of the <see cref="ActivityIdScope"/> class
        /// </summary>
        /// <param name="activityId"></param>
        public ActivityIdScope(Guid activityId)
        {
            _previousActivityId = CorrelationManager.ActivityId;
            CorrelationManager.ActivityId = activityId;
        }

        /// <summary>
        /// Restores the previous activity id when this instance is disposed
        /// </summary>
        public void Dispose()
        {
            CorrelationManager.ActivityId = _previousActivityId;
        }
    }

    internal static class CorrelationManager
    {
#if NETFRAMEWORK
        public static Guid ActivityId
        {
            get
            {
                return Trace.CorrelationManager.ActivityId;
            }
            set
            {
                Trace.CorrelationManager.ActivityId = value;
            }
        }
#else
        [ThreadStatic]
        private static Guid _activityId;

        public static Guid ActivityId
        {
            get
            {
                return _activityId;
            }
            set
            {
                _activityId = value;
            }
        }
#endif
    }
}
