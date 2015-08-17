// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlTypes;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Storage representation of a shard schema info.
    /// </summary>
    internal interface IStoreSchemaInfo
    {
        /// <summary>
        /// Schema info name.
        /// </summary>
        string Name
        {
            get;
        }

        /// <summary>
        /// Schema info represented in XML.
        /// </summary>
        SqlXml ShardingSchemaInfo
        {
            get;
        }
    }
}
