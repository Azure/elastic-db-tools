// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    /// <summary>
    /// Extension methods for System.Exception.
    /// </summary>
    internal static class ExceptionExtensionMethods
    {
        /// <summary>
        /// Checks if this exception's type is the same, or a sub-class, of any of the specified types.
        /// </summary>
        /// <param name="ex">This instance.</param>
        /// <param name="types">Types to be matched against.</param>
        /// <returns>Whether or not this exception matched any of the specified types.</returns>
        public static bool IsAnyOf(this Exception ex, Type[] types)
        {
            if (ex == null)
            {
                throw new ArgumentNullException("ex");
            }
            if (types == null)
            {
                throw new ArgumentNullException("types");
            }

            Type exceptionType = ex.GetType();
            return types.Any(type => type.IsAssignableFrom(exceptionType));
        }
    }
}
