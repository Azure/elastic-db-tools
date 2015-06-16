﻿// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    using System;

    internal partial class TransientFaultHandling
    {
        /// <summary>
        /// Defines an interface that must be implemented by custom components responsible for detecting specific transient conditions.
        /// </summary>
        internal interface ITransientErrorDetectionStrategy
        {
            /// <summary>
            /// Determines whether the specified exception represents a transient failure that can be compensated by a retry.
            /// </summary>
            /// <param name="ex">The exception object to be verified.</param>
            /// <returns>true if the specified exception is considered as transient; otherwise, false.</returns>
            bool IsTransient(Exception ex);
        }
    }
}