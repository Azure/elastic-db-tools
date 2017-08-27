// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// Setting ComVisible to false makes the types in this assembly not visible
// to COM components.  If you need to access a type in this assembly from
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests")]

[assembly: CLSCompliant(true)]

[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1824:MarkAssembliesWithNeutralResourcesLanguage", Justification = "Too many dubious spelling errors.")]

internal static class ElasticScaleVersionInfo
{
    public const string ProductVersion = "2.0.0";
}
