// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Type of shard key. Currently, only Int32, Int64, Guid and byte[] are the data types supported as shard keys.
    /// </summary>
    public enum ShardKeyType : int
    {
        /// <summary>
        /// No type specified.
        /// </summary>
        None,

        /// <summary>
        /// 32-bit integral value.
        /// </summary>
        Int32,

        /// <summary>
        /// 64-bit integral value.
        /// </summary>
        Int64,

        /// <summary>
        /// UniqueIdentifier value.
        /// </summary>
        Guid,

        /// <summary>
        /// Array of bytes value.
        /// </summary>
        Binary,

        /// <summary>
        /// Date and time value.
        /// </summary>
        DateTime,

        /// <summary>
        /// Time value.
        /// </summary>
        TimeSpan,

        /// <summary>
        /// Date and time value with offset.
        /// </summary>
        DateTimeOffset,
    }
}
