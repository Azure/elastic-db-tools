// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Set of operations that can be performed on mappings with lockOwnerId.
    /// </summary>
    internal enum LockOwnerIdOpType : int
    {
        /// <summary>
        /// Lock the range mapping with the given lockOwnerId
        /// </summary>
        Lock,

        /// <summary>
        /// Unlock the range mapping that has the given lockOwnerId
        /// </summary>
        UnlockMappingForId,

        /// <summary>
        /// Unlock all the range mappings that have the given lockOwnerId
        /// </summary>
        UnlockAllMappingsForId,

        /// <summary>
        /// Unlock all locked range mappings
        /// </summary>
        UnlockAllMappings,
    }

    /// <summary>
    /// Arguments used to create a <see cref="RangeMapping{TKey}"/>.
    /// </summary>
    /// <typeparam name="TKey">Type of the key (boundary values).</typeparam>
    public sealed class RangeMappingCreationInfo<TKey>
    {
        /// <summary>
        /// Arguments used for creation of a range mapping.
        /// </summary>
        /// <param name="value">Range being mapped.</param>
        /// <param name="shard">Shard used as the mapping target.</param>
        /// <param name="status">Status of the mapping.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public RangeMappingCreationInfo(Range<TKey> value, Shard shard, MappingStatus status)
        {
            ExceptionUtils.DisallowNullArgument(value, "value");
            ExceptionUtils.DisallowNullArgument(shard, "shard");
            this.Value = value;
            this.Shard = shard;
            this.Status = status;

            this.Range = new ShardRange(
                new ShardKey(ShardKey.ShardKeyTypeFromType(typeof(TKey)), value.Low),
                new ShardKey(ShardKey.ShardKeyTypeFromType(typeof(TKey)), value.HighIsMax ? null : (object)value.High));
        }

        /// <summary>
        /// Gets Range being mapped.
        /// </summary>
        public Range<TKey> Value
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets Shard for the mapping.
        /// </summary>
        public Shard Shard
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets Status of the mapping.
        /// </summary>
        public MappingStatus Status
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets Range associated with the <see cref="RangeMapping{TKey}"/>.
        /// </summary>
        internal ShardRange Range
        {
            get;
            private set;
        }
    }

    /// <summary>
    /// Represents a mapping between a range of key values and a <see cref="Shard"/>.
    /// </summary>
    /// <typeparam name="TKey">Key type.</typeparam>
    public sealed class RangeMapping<TKey> : IShardProvider<Range<TKey>>, ICloneable<RangeMapping<TKey>>, IMappingInfoProvider
    {
        /// <summary>
        /// Shard object associated with the mapping.
        /// </summary>
        private Shard _shard;

        /// <summary>
        /// Constructs a range mapping given mapping creation arguments.
        /// </summary>
        /// <param name="manager">Owning ShardMapManager.</param>
        /// <param name="creationInfo">Mapping creation information.</param>
        internal RangeMapping(ShardMapManager manager, RangeMappingCreationInfo<TKey> creationInfo)
        {
            Debug.Assert(manager != null);
            Debug.Assert(creationInfo != null);
            Debug.Assert(creationInfo.Shard != null);

            this.Manager = manager;
            _shard = creationInfo.Shard;

            this.StoreMapping = new DefaultStoreMapping(
                Guid.NewGuid(),
                creationInfo.Shard,
                creationInfo.Range.Low.RawValue,
                creationInfo.Range.High.RawValue,
                (int)creationInfo.Status);

            this.Range = creationInfo.Range;
            this.Value = creationInfo.Value;
        }

        /// <summary>
        /// Internal constructor used for deserialization from store representation of
        /// the mapping object.
        /// </summary>
        /// <param name="manager">Owning ShardMapManager.</param>
        /// <param name="shardMap">Owning shard map.</param>
        /// <param name="mapping">Storage representation of the mapping.</param>
        internal RangeMapping(
            ShardMapManager manager,
            ShardMap shardMap,
            IStoreMapping mapping)
        {
            Debug.Assert(manager != null);
            this.Manager = manager;

            Debug.Assert(mapping != null);
            Debug.Assert(mapping.ShardMapId != default(Guid));
            Debug.Assert(mapping.StoreShard.ShardMapId != default(Guid));
            this.StoreMapping = mapping;

            _shard = new Shard(this.Manager, shardMap, mapping.StoreShard);

            this.Range = new ShardRange(
                ShardKey.FromRawValue(ShardKey.ShardKeyTypeFromType(typeof(TKey)), mapping.MinValue),
                ShardKey.FromRawValue(ShardKey.ShardKeyTypeFromType(typeof(TKey)), mapping.MaxValue));

            this.Value = this.Range.High.IsMax ?
                new Range<TKey>(this.Range.Low.GetValue<TKey>()) :
                new Range<TKey>(this.Range.Low.GetValue<TKey>(), this.Range.High.GetValue<TKey>());
        }

        /// <summary>
        /// Gets the <see cref="MappingStatus"/> of the mapping.
        /// </summary>
        public MappingStatus Status
        {
            get
            {
                return ((MappingStatus)this.StoreMapping.Status & MappingStatus.Online);
            }
        }

        /// <summary>
        /// Gets Shard that contains the range of values.
        /// </summary>
        public Shard Shard
        {
            get
            {
                return _shard;
            }
        }

        /// <summary>
        /// Gets the Range of the mapping. 
        /// </summary>
        public Range<TKey> Value
        {
            get;
            private set;
        }

        /// <summary>
        /// Holder of the range value's binary representation.
        /// </summary>
        internal ShardRange Range
        {
            get;
            private set;
        }

        /// <summary>
        /// Identity of the mapping.
        /// </summary>
        internal Guid Id
        {
            get
            {
                return this.StoreMapping.Id;
            }
        }

        /// <summary>
        /// Identify of the ShardMap this shard belongs to.
        /// </summary>
        internal Guid ShardMapId
        {
            get
            {
                return this.StoreMapping.ShardMapId;
            }
        }

        /// <summary>
        /// Reference to the ShardMapManager.
        /// </summary>
        internal ShardMapManager Manager
        {
            get;
            set;
        }

        /// <summary>
        /// Storage representation of the mapping.
        /// </summary>
        internal IStoreMapping StoreMapping
        {
            get;
            private set;
        }

        /// <summary>
        /// The Tracer
        /// </summary>
        private static ILogger Tracer
        {
            get
            {
                return TraceHelper.Tracer;
            }
        }

        /// <summary>
        /// Converts the object to its string representation.
        /// </summary>
        /// <returns>String representation of the object.</returns>
        public override string ToString()
        {
            return StringUtils.FormatInvariant("R[{0}:{1}]", this.Id, this.Range);
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            RangeMapping<TKey> other = obj as RangeMapping<TKey>;

            if (other == null)
            {
                return false;
            }

            if (this.Id.Equals(other.Id))
            {
                Debug.Assert(this.Range.Equals(other.Range));
                return true;
            }

            return false;
        }

        /// <summary>
        /// Calculates the hash code for this instance.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        public override int GetHashCode()
        {
            return this.Id.GetHashCode();
        }

        #region IShardProvider<Range<TKey>>

        /// <summary>
        /// Shard that contains the range of values.
        /// </summary>
        Shard IShardProvider.ShardInfo
        {
            get
            {
                return this.Shard;
            }
        }

        /// <summary>
        /// Performs validation that the local representation is as up-to-date 
        /// as the representation on the backing data store.
        /// </summary>
        /// <param name="shardMap">Shard map to which the shard provider belongs.</param>
        /// <param name="conn">Connection used for validation.</param>
        void IShardProvider.Validate(IStoreShardMap shardMap, SqlConnection conn)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeMapping,
                "Validate", "Start; Connection: {0};", conn.ConnectionString);

            ValidationUtils.ValidateMapping(
                conn,
                this.Manager,
                shardMap,
                this.StoreMapping);

            stopwatch.Stop();

            Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeMapping,
                "Validate", "Complete; Connection: {0}; Duration: {1}",
                conn.ConnectionString, stopwatch.Elapsed);
        }

        /// <summary>
        /// Asynchronously performs validation that the local representation is as 
        /// up-to-date as the representation on the backing data store.
        /// </summary>
        /// <param name="shardMap">Shard map to which the shard provider belongs.</param>
        /// <param name="conn">Connection used for validation.</param>
        /// <returns>A task to await validation completion</returns>
        async Task IShardProvider.ValidateAsync(IStoreShardMap shardMap, SqlConnection conn)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeMapping,
                "ValidateAsync", "Start; Connection: {0};", conn.ConnectionString);

            await ValidationUtils.ValidateMappingAsync(
                conn,
                this.Manager,
                shardMap,
                this.StoreMapping).ConfigureAwait(false);

            stopwatch.Stop();

            Tracer.TraceInfo(TraceSourceConstants.ComponentNames.RangeMapping,
                "ValidateAsync", "Complete; Connection: {0}; Duration: {1}",
                conn.ConnectionString, stopwatch.Elapsed);
        }

        /// <summary>
        /// The range of values. 
        /// </summary>
        Range<TKey> IShardProvider<Range<TKey>>.Value
        {
            get
            {
                return this.Value;
            }
        }

        #endregion IShardProvider<Range<TKey>>

        #region ICloneable<RangeMapping<TKey>>

        /// <summary>
        /// Clones the instance which implements the interface.
        /// </summary>
        /// <returns>Clone of the instance.</returns>
        public RangeMapping<TKey> Clone()
        {
            return new RangeMapping<TKey>(this.Manager, this.Shard.ShardMap, this.StoreMapping);
        }

        #endregion ICloneable<RangeMapping<TKey>>

        #region IMappingInfoProvider

        /// <summary>
        /// ShardMapManager for the object.
        /// </summary>
        ShardMapManager IMappingInfoProvider.Manager
        {
            get
            {
                return this.Manager;
            }
        }

        /// <summary>
        /// Shard map associated with the mapping.
        /// </summary>
        Guid IMappingInfoProvider.ShardMapId
        {
            get
            {
                return this.ShardMapId;
            }
        }

        /// <summary>
        /// Storage representation of the mapping.
        /// </summary>
        IStoreMapping IMappingInfoProvider.StoreMapping
        {
            get
            {
                return this.StoreMapping;
            }
        }

        /// <summary>
        /// Type of the mapping.
        /// </summary>
        MappingKind IMappingInfoProvider.Kind
        {
            get
            {
                return MappingKind.RangeMapping;
            }
        }

        /// <summary>
        /// Mapping type, useful for diagnostics.
        /// </summary>
        string IMappingInfoProvider.TypeName
        {
            get
            {
                return "RangeMapping";
            }
        }

        #endregion IMappingInfoProvider
    }
}
