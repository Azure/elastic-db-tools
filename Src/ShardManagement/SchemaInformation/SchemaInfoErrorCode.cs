// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema
{
    /// <summary>
    /// Possible errors encountered by SchemaInfoCollection.
    /// </summary>
    public enum SchemaInfoErrorCode
    {
        /// <summary>
        /// No <see cref="SchemaInfo"/> exists with the given name.
        /// </summary>
        SchemaInfoNameDoesNotExist,

        /// <summary>
        /// A <see cref="SchemaInfo"/> entry with the given name already exists.
        /// </summary>
        SchemaInfoNameConflict,

        /// <summary>
        /// An entry for the given table already exists in the <see cref="SchemaInfo"/> object.
        /// </summary>
        TableInfoAlreadyPresent
    }
}
