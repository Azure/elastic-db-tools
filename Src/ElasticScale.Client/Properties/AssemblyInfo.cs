// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("Microsoft.Azure.SqlDatabase.ElasticScale.Client")]
[assembly: AssemblyDescription("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("4599fe76-62a2-4da8-8a0f-dd190c0c6c58")]

[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ServiceCommon" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.SplitMerge.Client" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.SqlServer.DataWarehouse.Engine" + AssemblyRef.ElasticQueryPublicKey)]
[assembly: InternalsVisibleTo("SplitMergeWorker" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("ShardSplitMergeTests" + AssemblyRef.ProductPublicKey)]

[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.Client.Fakes" + AssemblyRef.FakesPublicKey)]

// No-op, purely for Resharper's intellisense, as it does not understand AssemblyRef.ProductPublicKey
#if DEBUG
#pragma warning disable 1700
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ServiceCommon, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.SplitMerge.Client, PublicKey=00")]
[assembly: InternalsVisibleTo("SplitMergeWorker, PublicKey=00")]
[assembly: InternalsVisibleTo("ShardSplitMergeTests, PublicKey=00")]
#pragma warning restore 1700
#endif

// Added reference to the nuget package in the unsigned versions
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ServiceCommon" + AssemblyRef.TestPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.SplitMerge.Client" + AssemblyRef.TestPublicKey)]
[assembly: InternalsVisibleTo("SplitMergeWorker" + AssemblyRef.TestPublicKey)]
[assembly: InternalsVisibleTo("ShardSplitMergeTests" + AssemblyRef.TestPublicKey)]

[assembly: CLSCompliant(true)]

// Associated VSTS #2466045
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1824:MarkAssembliesWithNeutralResourcesLanguage", Justification = "Too many dubious spelling errors.")]
// [assembly: NeutralResourcesLanguageAttribute("en-US")]

// The Microsoft Azure SQL Database team's build system automatically creates the AssemblyRef class during the build process.
// This class contains the values of the public keys that the assemblies are signed with. When building externally, we manually define it here.
#if STANDALONE_BUILD
internal static class AssemblyRef
{
    public const string ProductPublicKey = "";
    public const string TestPublicKey = "";
    public const string FakesPublicKey = "";
    public const string ElasticQueryPublicKey = "";
}

internal static class ElasticScaleVersionInfo
{
    public const string ProductVersion = "1.0.0";
}
#endif
