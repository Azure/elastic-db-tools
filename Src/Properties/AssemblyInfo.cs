using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Azure.SqlDatabase.ElasticScale;

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
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.Jobs.Common" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.Jobs.Common.Tests" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.Jobs.WorkerRole" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.SplitMerge.Client" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("SplitMergeWorker" + AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("ShardSplitMergeTests" + AssemblyRef.ProductPublicKey)]

// No-op, purely for Resharper's intellisense, as it does not understand AssemblyRef.ProductPublicKey
#if DEBUG
#pragma warning disable 1700
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ServiceCommon, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.Jobs.Common, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.Jobs.Common.Tests, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.Jobs.WorkerRole, PublicKey=00")]
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.SplitMerge.Client, PublicKey=00")]
[assembly: InternalsVisibleTo("SplitMergeWorker, PublicKey=00")]
[assembly: InternalsVisibleTo("ShardSplitMergeTests, PublicKey=00")]
#pragma warning restore 1700
#endif

// Signing is not done in external build, so this InternalsVisibleTo is different built externally
#if EXTERNAL_BUILD
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.Client.Fakes, PublicKey=00")]
#else
[assembly: InternalsVisibleTo("Microsoft.Azure.SqlDatabase.ElasticScale.Client.Fakes, PublicKey=0024000004800000940000000602000000240000525341310004000001000100e92decb949446f688ab9f6973436c535bf50acd1fd580495aae3f875aa4e4f663ca77908c63b7f0996977cb98fcfdb35e05aa2c842002703cad835473caac5ef14107e3a7fae01120a96558785f48319f66daabc862872b2c53f5ac11fa335c0165e202b4c011334c7bc8f4c4e570cf255190f4e3e2cbc9137ca57cb687947bc")]
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
#if EXTERNAL_BUILD
namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    internal static class AssemblyRef
    {
        public const string ProductPublicKey = "";
        public const string TestPublicKey = "";
    }

    internal static class ElasticScaleVersionInfo
    {
        public const string ProductVersion = "1.0.0";
    }
}
#endif
