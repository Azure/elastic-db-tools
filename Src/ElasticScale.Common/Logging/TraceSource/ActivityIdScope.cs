//******************************************************************************
// Copyright (c) Microsoft Corporation
//
// @File: ActivityIdScope.cs
//
// @Owner: raveeram
// @Test:
//
// Purpose: Utility class to set and restore the System.Diagnostics CorrelationManager
// ActivityId via the using pattern
//
//******************************************************************************

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
        private readonly Guid m_previousActivityId;

        /// <summary>
        /// Creates an instance of the <see cref="ActivityIdScope"/> class
        /// </summary>
        /// <param name="activityId"></param>
        public ActivityIdScope(Guid activityId)
        {
            m_previousActivityId = Trace.CorrelationManager.ActivityId;
            Trace.CorrelationManager.ActivityId = activityId;
        }

        /// <summary>
        /// Restores the previous activity id when this instance is disposed
        /// </summary>
        public void Dispose()
        {
            Trace.CorrelationManager.ActivityId = m_previousActivityId;
        }
    }
}
