// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Types of supported mappings.
    /// </summary>
    internal enum MappingKind
    {
        PointMapping,
        RangeMapping
    }

    /// <summary>
    /// Interface that represents capability to provide information
    /// relavant to Add/Remove/Update operations for a mapping object.
    /// </summary>
    internal interface IMappingInfoProvider
    {
        /// <summary>
        /// ShardMapManager for the object.
        /// </summary>
        ShardMapManager Manager
        {
            get;
        }

        /// <summary>
        /// Shard map associated with the mapping.
        /// </summary>
        Guid ShardMapId
        {
            get;
        }

        /// <summary>
        /// Storage representation of the mapping.
        /// </summary>
        IStoreMapping StoreMapping
        {
            get;
        }

        /// <summary>
        /// Type of the mapping.
        /// </summary>
        MappingKind Kind
        {
            get;
        }

        /// <summary>
        /// Mapping type, useful for diagnostics.
        /// </summary>
        string TypeName
        {
            get;
        }
    }
}
