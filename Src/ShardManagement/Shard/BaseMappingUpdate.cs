using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Base class for updates to mappings from shardlets to shards.
    /// </summary>
    /// <typeparam name="TStatus">Type of status field.</typeparam>
    public abstract class BaseMappingUpdate<TStatus> : IMappingUpdate<TStatus>
    {
        /// <summary>
        /// Records the modified properties for update.
        /// </summary>
        private MappingUpdatedProperties updatedProperties;

        /// <summary>
        /// Holder for update to status property.
        /// </summary>
        private TStatus status;

        /// <summary>
        /// Holder for update to shard property.
        /// </summary>
        private Shard shard;

        /// <summary>
        /// Gets or sets the Status property.
        /// </summary>
        public TStatus Status
        {
            get
            {
                return this.status;
            }
            set
            {
                this.status = value;
                this.updatedProperties |= MappingUpdatedProperties.Status;
            }
        }

        /// <summary>
        /// Gets or sets the Shard property.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", 
            MessageId = "0")]
        public Shard Shard
        {
            get
            {
                return this.shard.Clone();
            }
            set
            {
                ExceptionUtils.DisallowNullArgument(value, "value");
                this.shard = value.Clone();
                this.updatedProperties |= MappingUpdatedProperties.Shard;
            }
        }

        /// <summary>
        /// Status property.
        /// </summary>
        TStatus IMappingUpdate<TStatus>.Status
        {
            get
            {
                return this.Status;
            }
        }

        /// <summary>
        /// Shard property.
        /// </summary>
        Shard IMappingUpdate<TStatus>.Shard
        {
            get
            {
                return this.Shard;
            }
        }

        /// <summary>
        /// Checks if any property is set in the given bitmap.
        /// </summary>
        /// <param name="properties">Properties bitmap.</param>
        /// <returns>True if any of the properties is set, false otherwise.</returns>
        bool IMappingUpdate<TStatus>.IsAnyPropertySet(MappingUpdatedProperties properties)
        {
            return (this.updatedProperties & properties) != 0;
        }

        /// <summary>
        /// Checks if the mapping is being taken offline.
        /// </summary>
        /// <param name="originalStatus">Original status.</param>
        /// <returns>True of the update will take the mapping offline.</returns>
        bool IMappingUpdate<TStatus>.IsMappingBeingTakenOffline(TStatus originalStatus)
        {
            if ((this.updatedProperties & MappingUpdatedProperties.Status) != MappingUpdatedProperties.Status)
            {
                return false;
            }
            else
            {
                return this.IsBeingTakenOffline(originalStatus, this.Status);
            }
        }

        /// <summary>
        /// Detects if the current mapping is being taken offline.
        /// </summary>
        /// <param name="originalStatus">Original status.</param>
        /// <param name="updatedStatus">Updated status.</param>
        /// <returns>Detects in the derived types if the mapping is being taken offline.</returns>
        protected abstract bool IsBeingTakenOffline(TStatus originalStatus, TStatus updatedStatus);
    }
}
