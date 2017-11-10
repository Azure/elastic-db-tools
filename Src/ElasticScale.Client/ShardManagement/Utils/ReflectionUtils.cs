// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Reflection;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Utils
{
    internal static class ReflectionUtils
    {
        public static Assembly GetAssembly(this Type type)
        {
#if NETFRAMEWORK
            return type.Assembly;
#else
            return type.GetTypeInfo().Assembly;
#endif
        }
    }
}
