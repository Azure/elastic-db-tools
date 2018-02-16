// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Utility properties and methods used for managing scripts and errors.
    /// </summary>
    internal static partial class Scripts
    {
        /// <summary>
        /// structure to hold upgrade command batches along with the starting version to apply the upgrade step.
        /// </summary>
        internal struct UpgradeScript
        {
            /// <summary>
            /// Major version to apply this upgrade step.
            /// </summary>
            public int InitialMajorVersion
            {
                get;
                private set;
            }

            /// <summary>
            /// Minor version to apply this upgrade step.
            /// </summary>
            public int InitialMinorVersion
            {
                get;
                private set;
            }

            /// <summary>
            /// Commands in this upgrade step batch. These will be executed only when store is at (this.InitialMajorVersion, this.InitialMinorVersion).
            /// </summary>
            public string Script
            {
                get;
                private set;
            }

            /// <summary>
            /// Construct upgrade steps.
            /// </summary>
            /// <param name="initialMajorVersion">Expected major version of store to run this upgrade step.</param>
            /// <param name="initialMinorVersion">Expected minor version of store to run this upgrade step.</param>
            /// <param name="commands">Commands to execute as part of this upgrade step.</param>
            public UpgradeScript(int initialMajorVersion, int initialMinorVersion, string commands)
                : this()
            {
                this.InitialMajorVersion = initialMajorVersion;
                this.InitialMinorVersion = initialMinorVersion;
                this.Script = commands;
            }
        };

        /// <remarks>
        /// Implemented as property to avoid static initialization ordering issues
        /// </remarks>
        internal static IEnumerable<UpgradeScript> UpgradeGlobalScripts => new[]
        {
            UpgradeShardMapManagerGlobalFrom0_0To1_0,
            UpgradeShardMapManagerGlobalFrom1_0To1_1,
            UpgradeShardMapManagerGlobalFrom1_1To1_2,
            UpgradeShardMapManagerGlobalFrom1000_0To1000_1
        };

        /// <remarks>
        /// Implemented as property to avoid static initialization ordering issues
        /// </remarks>
        internal static IEnumerable<UpgradeScript> UpgradeLocalScripts => new[]
        {
            UpgradeShardMapManagerLocalFrom0_0To1_0,
            UpgradeShardMapManagerLocalFrom1_0To1_1,
            UpgradeShardMapManagerLocalFrom1_1To1_2,
            UpgradeShardMapManagerLocalFrom1000_0To1000_1
        };
    }
}
