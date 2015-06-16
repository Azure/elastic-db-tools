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
        private ShardUpdatedProperties updatedProperties;

        /// <summary>
        /// Holder for update to status property.
        /// </summary>
        private ShardStatus status;

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
                return this.status;
            }
            set
            {
                this.status = value;
                this.updatedProperties |= ShardUpdatedProperties.Status;
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
            return (this.updatedProperties & p) != 0;
        }
    }
}
