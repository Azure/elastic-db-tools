// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    internal static class ApplicationNameHelper
    {
        /// <summary>
        /// Maximum length allowed for an ApplicationName
        /// </summary>
        internal const int MaxApplicationNameLength = 128;

        /// <summary>
        /// Adds suffix to the application name, but not exceeding certain length.
        /// </summary>
        /// <param name="originalApplicationName">Application provided application name.</param>
        /// <param name="suffixToAppend">Suffix to append to the application name.</param>
        /// <returns>Application name with suffix appended.</returns>
        public static string AddApplicationNameSuffix(string originalApplicationName, string suffixToAppend)
        {
            if (String.IsNullOrEmpty(originalApplicationName))
            {
                return suffixToAppend;
            }

            if (String.IsNullOrEmpty(suffixToAppend))
            {
                return originalApplicationName;
            }

            int maxAppNameSubStringAllowed = MaxApplicationNameLength - suffixToAppend.Length;

            if (originalApplicationName.Length <= maxAppNameSubStringAllowed)
            {
                return originalApplicationName + suffixToAppend;
            }
            else
            {
                // Take the substring of application name that will be fit within the 'program_name' column in dm_exec_sessions.
                return originalApplicationName.Substring(0, maxAppNameSubStringAllowed) + suffixToAppend;
            }
        }

        /// <summary>
        /// Adds suffix to the application name of the provided connection string
        /// Will not exceeding certain length of the application name.
        /// </summary>
        /// <param name="originalConnectionString">The original connection string.</param>
        /// <param name="suffixToAppend">The suffix to append.</param>
        /// <returns>
        /// Modified connection string with suffix appended to application name
        /// </returns>
        public static string AddApplicationNameSuffixToConnectionString(
            string originalConnectionString,
            string suffixToAppend)
        {
            return
                new SqlConnectionStringBuilder(originalConnectionString).WithApplicationNameSuffix(suffixToAppend)
                    .ToString();
        }

        /// <summary>
        /// Mutates provided SqlConnectionStringBuilder with the application name suffix.
        /// Returns modified SqlConnectionStringBuilder.
        /// </summary>
        /// <param name="builder">The builder.</param>
        /// <param name="suffixToAppend">The suffix to append.</param>
        /// <returns></returns>
        public static SqlConnectionStringBuilder WithApplicationNameSuffix(this SqlConnectionStringBuilder builder,
            string suffixToAppend)
        {
            if (builder == null)
            {
                return null;
            }

            builder.ApplicationName = AddApplicationNameSuffix(builder.ApplicationName, suffixToAppend);

            return builder;
        }
    }
}
