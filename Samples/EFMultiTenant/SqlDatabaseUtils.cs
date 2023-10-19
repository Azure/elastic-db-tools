// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Data.SqlClient;

namespace EFMultiTenantElasticScale
{
    /// <summary>
    /// Helper methods for interacting with SQL Databases.
    /// </summary>
    internal static class SqlDatabaseUtils
    {
        /// <summary>
        /// Create a retry logic provider
        /// </summary>
        public static SqlRetryLogicBaseProvider SqlRetryProvider = SqlConfigurableRetryFactory.CreateExponentialRetryProvider(SqlRetryPolicy);

        /// <summary>
        /// Gets the retry policy to use for connections to SQL Server.
        /// </summary>
        private static SqlRetryLogicOption SqlRetryPolicy => new()
        {
            // Tries 5 times before throwing an exception
            NumberOfTries = 5,
            // Preferred gap time to delay before retry
            DeltaTime = TimeSpan.FromSeconds(1),
            // Maximum gap time for each delay time before retry
            MaxTimeInterval = TimeSpan.FromSeconds(20)
        };

    }
}
