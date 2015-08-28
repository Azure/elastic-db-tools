// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

namespace ElasticDapper
{
    /// <summary>
    /// Helper methods for interacting with SQL Databases.
    /// </summary>
    internal static class SqlDatabaseUtils
    {
        /// <summary>
        /// Gets the retry policy to use for connections to SQL Server.
        /// </summary>
        public static RetryPolicy SqlRetryPolicy
        {
            get { return new RetryPolicy<SqlDatabaseTransientErrorDetectionStrategy>(10, TimeSpan.FromSeconds(5)); }
        }
    }
}
