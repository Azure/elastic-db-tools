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

// This assembly is strong name signed, so friend declaration must also have strong name public key
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests, PublicKey=00240000048000009400000006020000002400005253413100040000010001007d813b35eaf89b7ae4be8a49086058380e083b58752b0a3a8323157e68b4b0f9fd78d2fe75e9ec253d8bb2225637d4c2393234e0f877bfddd7907eda8293083b7f4dbc664f09f6b62ce74266a4e79002783252559f5b23cfc682eb79b51a5f5d16dca2364413ae563b3ab6db2fc9da3ced11f9eef50421b982dfc3a08cb635a8")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests, PublicKey=00240000048000009400000006020000002400005253413100040000010001007d813b35eaf89b7ae4be8a49086058380e083b58752b0a3a8323157e68b4b0f9fd78d2fe75e9ec253d8bb2225637d4c2393234e0f877bfddd7907eda8293083b7f4dbc664f09f6b62ce74266a4e79002783252559f5b23cfc682eb79b51a5f5d16dca2364413ae563b3ab6db2fc9da3ced11f9eef50421b982dfc3a08cb635a8")]

[assembly: CLSCompliant(true)]

[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1824:MarkAssembliesWithNeutralResourcesLanguage", Justification = "Too many dubious spelling errors.")]

internal static class ElasticScaleVersionInfo
{
    public const string ProductVersion = "2.0.0";
}
