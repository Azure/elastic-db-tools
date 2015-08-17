// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Types of transaction scopes used during store operations.
    /// </summary>
    internal enum StoreOperationTransactionScopeKind
    {
        /// <summary>
        /// Scope of GSM.
        /// </summary>
        Global,

        /// <summary>
        /// Scope of source LSM.
        /// </summary>
        LocalSource,

        /// <summary>
        /// Scope of target LSM.
        /// </summary>
        LocalTarget
    }
}
