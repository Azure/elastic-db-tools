// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Records the updated properties on the shard.
    /// </summary>
    [Flags]
    internal enum ShardUpdatedProperties
    {
        Status = 1,
        All = Status
    }

    /// <summary>
    /// Represents updates to a Shard.
    /// </summary>
    internal sealed class ShardUpdate
    {
        /// <summary>
        /// Records the modified properties for update.
        /// </summary>
        private ShardUpdatedProperties _updatedProperties;

        /// <summary>
        /// Holder for update to status property.
        /// </summary>
        private ShardStatus _status;

        /// <summary>
        /// Instantiates the shard update object with no property set.
        /// </summary>
        public ShardUpdate()
        {
        }

        /// <summary>
        /// Status property.
        /// </summary>
        public ShardStatus Status
        {
            get
            {
                return _status;
            }
            set
            {
                _status = value;
                _updatedProperties |= ShardUpdatedProperties.Status;
            }
        }

        /// <summary>
        /// Checks if any of the properties specified in the given bitmap have
        /// been set by the user.
        /// </summary>
        /// <param name="p">Bitmap of properties.</param>
        /// <returns>True if any property is set, false otherwise.</returns>
        internal bool IsAnyPropertySet(ShardUpdatedProperties p)
        {
            return (_updatedProperties & p) != 0;
        }
    }
}
