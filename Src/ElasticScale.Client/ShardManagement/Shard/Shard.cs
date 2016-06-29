// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Arguments used to create a <see cref="Shard"/>.
    /// </summary>
    public sealed class ShardCreationInfo
    {
        /// <summary>
        /// Arguments used to create a <see cref="Shard"/>.
        /// </summary>
        /// <param name="location">Location of the shard.</param>
        public ShardCreationInfo(ShardLocation location) : this(location, ShardStatus.Online)
        {
        }

        /// <summary>
        /// Arguments used to create a <see cref="Shard"/>.
        /// </summary>
        /// <param name="location">Location of the shard.</param>
        /// <param name="status">Status of the shard.</param>
        internal ShardCreationInfo(ShardLocation location, ShardStatus status)
        {
            ExceptionUtils.DisallowNullArgument(location, "location");
            this.Location = location;
            this.Status = status;
        }

        /// <summary>
        /// Gets Location of the shard.
        /// </summary>
        public ShardLocation Location
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets Status of the shard. Users can assign application-specific 
        /// values to the status field, which are kept together with the shard for convenience.
        /// </summary>
        internal ShardStatus Status
        {
            get;
            private set;
        }
    }

    /// <summary>
    /// Representation of a single shard. Shards are basically locators for
    /// data sources i.e. <see cref="ShardLocation"/>s that have been
    /// registered with a shard map. Shards are used in
    /// mapping as targets of mappings (see <see cref="PointMapping{TKey}"/>
    /// and <see cref="RangeMapping{TKey}"/>).
    /// </summary>
    public sealed class Shard : IShardProvider<ShardLocation>, ICloneable<Shard>, IEquatable<Shard>
    {
        /// <summary>Hashcode for the shard.</summary>
        private readonly int _hashCode;

        /// <summary>
        /// Constructs a Shard given shard creation arguments.
        /// </summary>
        /// <param name="manager">Owning ShardMapManager.</param>
        /// <param name="shardMap">Owning shard map.</param>
        /// <param name="creationInfo">Shard creation information.</param>
        internal Shard(
            ShardMapManager manager,
            ShardMap shardMap,
            ShardCreationInfo creationInfo)
        {
            Debug.Assert(manager != null);
            Debug.Assert(shardMap != null);
            Debug.Assert(creationInfo != null);

            this.Manager = manager;
            this.ShardMap = shardMap;

            this.StoreShard = new DefaultStoreShard(
                Guid.NewGuid(),
                Guid.NewGuid(),
                shardMap.Id,
                creationInfo.Location,
                (int)creationInfo.Status);

            _hashCode = this.CalculateHashCode();
        }

        /// <summary>
        /// Internal constructor that uses storage representation.
        /// </summary>
        /// <param name="manager">Owning ShardMapManager.</param>
        /// <param name="shardMap">Owning shard map.</param>
        /// <param name="storeShard">Storage representation of the shard.</param>
        internal Shard(
            ShardMapManager manager,
            ShardMap shardMap,
            IStoreShard storeShard)
        {
            Debug.Assert(manager != null);
            this.Manager = manager;

            Debug.Assert(shardMap != null);
            this.ShardMap = shardMap;

            Debug.Assert(storeShard.ShardMapId != default(Guid));
            this.StoreShard = storeShard;

            _hashCode = this.CalculateHashCode();
        }

        /// <summary>
        /// Gets Location of the shard.
        /// </summary>
        public ShardLocation Location
        {
            get
            {
                return this.StoreShard.Location;
            }
        }

        /// <summary>
        /// Gets the status of the shard which can be either online or offline.
        /// Connections can only be opened using <see cref="Shard.OpenConnection(string, ConnectionOptions)"/> 
        /// on the shard map when the shard is online. Setting the shard status to offline
        /// prevents connections when the shard is undergoing maintenance operations.
        /// </summary>
        internal ShardStatus Status
        {
            get
            {
                return (ShardStatus)this.StoreShard.Status;
            }
        }

        /// <summary>
        /// Identity of the shard. Each shard should have a unique one.
        /// </summary>
        internal Guid Id
        {
            get
            {
                return this.StoreShard.Id;
            }
        }

        /// <summary>
        /// Shard version.
        /// </summary>
        internal Guid Version
        {
            get
            {
                return this.StoreShard.Version;
            }
        }

        /// <summary>
        /// Shard for the ShardProvider object.
        /// </summary>
        internal Shard ShardInfo
        {
            get
            {
                return this;
            }
        }

        /// <summary>
        /// Value corresponding to the Shard. Represents traits of the Shard 
        /// object provided by the ShardInfo property.
        /// </summary>
        internal ShardLocation Value
        {
            get
            {
                return this.Location;
            }
        }

        /// <summary>
        /// Shard map object to which shard belongs.
        /// </summary>
        internal ShardMap ShardMap
        {
            get;
            private set;
        }

        /// <summary>
        /// Identify of the ShardMap this shard belongs to.
        /// </summary>
        internal Guid ShardMapId
        {
            get
            {
                return this.StoreShard.ShardMapId;
            }
        }

        /// <summary>
        /// Reference to the ShardMapManager.
        /// </summary>
        internal ShardMapManager Manager
        {
            get;
            private set;
        }

        /// <summary>
        /// Storage representation of the shard.
        /// </summary>
        internal IStoreShard StoreShard
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

        #region Sync OpenConnection methods

        /// <summary>
        /// Opens a regular <see cref="SqlConnection"/> to the specified shard, with <see cref="ConnectionOptions.Validate"/>.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// </remarks>
        public SqlConnection OpenConnection(string connectionString)
        {
            return this.OpenConnection(connectionString, ConnectionOptions.Validate);
        }

        /// <summary>
        /// Opens a regular <see cref="SqlConnection"/> to the specified shard.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// </remarks>
        public SqlConnection OpenConnection(string connectionString, ConnectionOptions options)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                return this.ShardMap.OpenConnection(this as IShardProvider, connectionString, options);
            }
        }

        #endregion

        #region Async OpenConnection methods

        /// <summary>
        /// Asynchronously opens a regular <see cref="SqlConnection"/> to the specified shard, with <see cref="ConnectionOptions.Validate"/>.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <returns>A Task encapsulating an opened SqlConnection</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// All non-usage errors will be propagated via the returned Task.
        /// </remarks>
        public Task<SqlConnection> OpenConnectionAsync(string connectionString)
        {
            return this.OpenConnectionAsync(connectionString, ConnectionOptions.Validate);
        }

        /// <summary>
        /// Asynchronously a regular <see cref="SqlConnection"/> to the specified shard.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>A Task encapsulating an opened SqlConnection</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// All non-usage errors will be propagated via the returned Task.
        /// </remarks>
        public Task<SqlConnection> OpenConnectionAsync(string connectionString, ConnectionOptions options)
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                return this.ShardMap.OpenConnectionAsync(this as IShardProvider, connectionString, options);
            }
        }

        #endregion

        #region IShardProvider<Shard>

        /// <summary>
        /// Shard for the ShardProvider object.
        /// </summary>
        Shard IShardProvider.ShardInfo
        {
            get
            {
                return this.ShardInfo;
            }
        }

        /// <summary>
        /// Performs validation that the local representation is as 
        /// up-to-date as the representation on the backing data store.
        /// </summary>
        /// <param name="shardMap">Shard map to which the shard provider belongs.</param>
        /// <param name="conn">Connection used for validation.</param>
        void IShardProvider.Validate(IStoreShardMap shardMap, SqlConnection conn)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            Tracer.TraceInfo(TraceSourceConstants.ComponentNames.Shard,
                "Validate", "Start; Connection: {0};", conn.ConnectionString);

            ValidationUtils.ValidateShard(
                conn,
                this.Manager,
                shardMap,
                this.StoreShard);

            stopwatch.Stop();

            Tracer.TraceInfo(TraceSourceConstants.ComponentNames.Shard,
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
            Tracer.TraceInfo(TraceSourceConstants.ComponentNames.Shard,
                "ValidateAsync", "Start; Connection: {0};", conn.ConnectionString);

            await ValidationUtils.ValidateShardAsync(
                conn,
                this.Manager,
                shardMap,
                this.StoreShard).ConfigureAwait(false);

            stopwatch.Stop();

            Tracer.TraceInfo(TraceSourceConstants.ComponentNames.Shard,
                "ValidateAsync", "Complete; Connection: {0}; Duration: {1}",
                conn.ConnectionString, stopwatch.Elapsed);
        }

        /// <summary>
        /// Value corresponding to the Shard. Represents traits of the Shard 
        /// object provided by the ShardInfo property.
        /// </summary>
        ShardLocation IShardProvider<ShardLocation>.Value
        {
            get
            {
                return this.Value;
            }
        }

        #endregion IShardProvider<Shard>

        #region ICloneable<Shard>

        /// <summary>
        /// Clones the instance.
        /// </summary>
        /// <returns>Clone of the instance.</returns>
        public Shard Clone()
        {
            return new Shard(this.Manager, this.ShardMap, this.StoreShard);
        }

        #endregion ICloneable<Shard>

        /// <summary>
        /// Converts the object to its string representation.
        /// </summary>
        /// <returns>String representation of the object.</returns>
        public override string ToString()
        {
            return StringUtils.FormatInvariant("S[{0}:{1}:{2}]", this.Id, this.Version, this.Location);
        }

        #region IEquatable

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            return this.Equals(obj as Shard);
        }

        /// <summary>
        /// Performs equality comparison with given Shard.
        /// </summary>
        /// <param name="other">Shard to compare with.</param>
        /// <returns>True if this object is equal to other object, false otherwise.</returns>
        public bool Equals(Shard other)
        {
            if (null == other)
            {
                return false;
            }
            else
            {
                if (this.GetHashCode() != other.GetHashCode())
                {
                    return false;
                }
                else
                {
                    // DEVNOTE(wbasheer): We are assuming identify comparison, without caring about version.
                    bool result = (this.Id == other.Id) && (this.Version == other.Version);

                    Debug.Assert(!result || (this.ShardMapId == other.ShardMapId));
                    Debug.Assert(!result || (this.Location.GetHashCode() == other.Location.GetHashCode()));
                    Debug.Assert(!result || (this.Status == other.Status));

                    return result;
                }
            }
        }

        /// <summary>
        /// Calculates the hash code for this instance.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        public override int GetHashCode()
        {
            return _hashCode;
        }

        #endregion IEquatable

        /// <summary>
        /// Calculates the hash code for the object.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        private int CalculateHashCode()
        {
            // DEVNOTE(wbasheer): We are assuming identify comparison, without caring about version.
            return this.Id.GetHashCode();
            //return ShardKey.QPHash(this.Id.GetHashCode(), this.Version.GetHashCode());
        }
    }
}
