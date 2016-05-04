// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents a collection of shards and mappings between keys and shards in the collection.
    /// </summary>
    public abstract class ShardMap : ICloneable<ShardMap>
    {
        /// <summary>
        /// The mapper belonging to the ShardMap.
        /// </summary>
        private DefaultShardMapper _defaultMapper;

        /// <summary>
        /// Constructs an instance of ShardMap.
        /// </summary>
        /// <param name="manager">Reference to ShardMapManager.</param>
        /// <param name="ssm">Storage representation.</param>
        internal ShardMap(
            ShardMapManager manager,
            IStoreShardMap ssm)
        {
            Debug.Assert(manager != null);
            Debug.Assert(ssm != null);

            this.Manager = manager;
            this.StoreShardMap = ssm;

            this.ApplicationNameSuffix = GlobalConstants.ShardMapManagerPrefix + ssm.Id.ToString();

            _defaultMapper = new DefaultShardMapper(this.Manager, this);
        }

        /// <summary>Shard map name.</summary>
        public string Name
        {
            get
            {
                return this.StoreShardMap.Name;
            }
        }

        /// <summary>Shard map type.</summary>
        public ShardMapType MapType
        {
            get
            {
                return this.StoreShardMap.MapType;
            }
        }

        /// <summary>Shard map key type.</summary>
        public ShardKeyType KeyType
        {
            get
            {
                return this.StoreShardMap.KeyType;
            }
        }

        /// <summary>
        /// Identity.
        /// </summary>
        internal Guid Id
        {
            get
            {
                return this.StoreShardMap.Id;
            }
        }

        /// <summary>
        /// Reference to ShardMapManager.
        /// </summary>
        internal ShardMapManager Manager
        {
            get;
            private set;
        }

        /// <summary>
        /// Storage representation.
        /// </summary>
        internal IStoreShardMap StoreShardMap
        {
            get;
            private set;
        }

        /// <summary>
        /// Suffix added to application name in connections.
        /// </summary>
        internal string ApplicationNameSuffix
        {
            get;
            private set;
        }

        /// <summary>
        /// The tracer
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
            return StringUtils.FormatInvariant("SM[{0}:{1}:{2}]", this.StoreShardMap.MapType, this.StoreShardMap.KeyType, this.StoreShardMap.Name);
        }

        /// <summary>
        /// Opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
        /// </summary>
        /// <typeparam name="TKey">Type of the key.</typeparam>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <returns>An opened SqlConnection.</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// This call only works if there is a single default mapping.
        /// </remarks>
        public SqlConnection OpenConnectionForKey<TKey>(
            TKey key, string connectionString)
        {
            return this.OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
        }

        /// <summary>
        /// Opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped.
        /// </summary>
        /// <typeparam name="TKey">Type of the key.</typeparam>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>An opened SqlConnection.</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// This call only works if there is a single default mapping.
        /// </remarks>
        public SqlConnection OpenConnectionForKey<TKey>(
            TKey key,
            string connectionString,
            ConnectionOptions options)
        {
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

            Debug.Assert(this.StoreShardMap.KeyType != ShardKeyType.None);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                IShardMapper<TKey> mapper = this.GetMapper<TKey>();

                if (mapper == null)
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._ShardMap_OpenConnectionForKey_KeyTypeNotSupported,
                            typeof(TKey),
                            this.StoreShardMap.Name,
                            ShardKey.TypeFromShardKeyType(this.StoreShardMap.KeyType)),
                            "key");
                }

                Debug.Assert(mapper != null);

                return mapper.OpenConnectionForKey(key, connectionString, options);
            }
        }

        /// <summary>
        /// Asynchronously opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped, with <see cref="ConnectionOptions.Validate"/>.
        /// </summary>
        /// <typeparam name="TKey">Type of the key.</typeparam>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <returns>A Task encapsulating an opened SqlConnection.</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// This call only works if there is a single default mapping.
        /// </remarks>
        public Task<SqlConnection> OpenConnectionForKeyAsync<TKey>(
            TKey key, string connectionString)
        {
            return this.OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
        }

        /// <summary>
        /// Asynchronously opens a regular <see cref="SqlConnection"/> to the shard 
        /// to which the specified key value is mapped.
        /// </summary>
        /// <typeparam name="TKey">Type of the key.</typeparam>
        /// <param name="key">Input key value.</param>
        /// <param name="connectionString">
        /// Connection string with credential information such as SQL Server credentials or Integrated Security settings. 
        /// The hostname of the server and the database name for the shard are obtained from the lookup operation for key.
        /// </param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>A Task encapsulating an opened SqlConnection.</returns>
        /// <remarks>
        /// Note that the <see cref="SqlConnection"/> object returned by this call is not protected against transient faults. 
        /// Callers should follow best practices to protect the connection against transient faults 
        /// in their application code, e.g., by using the transient fault handling 
        /// functionality in the Enterprise Library from Microsoft Patterns and Practices team.
        /// This call only works if there is a single default mapping.
        /// </remarks>
        public Task<SqlConnection> OpenConnectionForKeyAsync<TKey>(
            TKey key,
            string connectionString,
            ConnectionOptions options)
        {
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

            Debug.Assert(this.StoreShardMap.KeyType != ShardKeyType.None);

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                IShardMapper<TKey> mapper = this.GetMapper<TKey>();

                if (mapper == null)
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._ShardMap_OpenConnectionForKey_KeyTypeNotSupported,
                            typeof(TKey),
                            this.StoreShardMap.Name,
                            ShardKey.TypeFromShardKeyType(this.StoreShardMap.KeyType)),
                            "key");
                }

                Debug.Assert(mapper != null);

                return mapper.OpenConnectionForKeyAsync(key, connectionString, options);
            }
        }

        /// <summary>
        /// Gets all shards from the shard map.
        /// </summary>
        /// <returns>All shards belonging to the shard map.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public IEnumerable<Shard> GetShards()
        {
            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "GetShards",
                    "Start; ");

                Stopwatch stopwatch = Stopwatch.StartNew();

                IEnumerable<Shard> shards = _defaultMapper.GetShards();

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "GetShards",
                    "Complete; Duration: {0}",
                    stopwatch.Elapsed);

                return shards;
            }
        }

        /// <summary>
        /// Obtains the shard for the specified location.
        /// </summary>
        /// <param name="location">Location of the shard.</param>
        /// <returns>Shard which has the specified location.</returns>
        public Shard GetShard(ShardLocation location)
        {
            ExceptionUtils.DisallowNullArgument(location, "location");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "GetShard",
                    "Start; Shard Location: {0} ",
                    location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                Shard shard = _defaultMapper.GetShardByLocation(location);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "GetShard",
                    "Complete; Shard Location: {0}; Duration: {1}",
                    location,
                    stopwatch.Elapsed);

                if (shard == null)
                {
                    throw new ShardManagementException(
                        ShardManagementErrorCategory.ShardMap,
                        ShardManagementErrorCode.ShardDoesNotExist,
                        Errors._ShardMap_GetShard_ShardDoesNotExist,
                        location,
                        this.Name);
                }

                return shard;
            }
        }

        /// <summary>
        /// Tries to obtains the shard for the specified location.
        /// </summary>
        /// <param name="location">Location of the shard.</param>
        /// <param name="shard">Shard which has the specified location.</param>
        /// <returns><c>true</c> if shard with specified location is found, <c>false</c> otherwise.</returns>
        public bool TryGetShard(ShardLocation location, out Shard shard)
        {
            ExceptionUtils.DisallowNullArgument(location, "location");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "TryGetShard",
                    "Start; Shard Location: {0} ",
                    location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                shard = _defaultMapper.GetShardByLocation(location);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "TryGetShard",
                    "Complete; Shard Location: {0}; Duration: {1}",
                    location,
                    stopwatch.Elapsed);

                return shard != null;
            }
        }

        /// <summary>
        /// Creates a new shard and registers it with the shard map.
        /// </summary>
        /// <param name="shardCreationArgs">Information about shard to be added.</param>
        /// <returns>A new shard registered with this shard map.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public Shard CreateShard(ShardCreationInfo shardCreationArgs)
        {
            ExceptionUtils.DisallowNullArgument(shardCreationArgs, "shardCreationArgs");

            using (ActivityIdScope activityId = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "CreateShard",
                    "Start; Shard: {0} ",
                    shardCreationArgs.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                Shard shard = _defaultMapper.Add(
                    new Shard(
                        this.Manager,
                        this,
                        shardCreationArgs));

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "CreateShard",
                    "Complete; Shard: {0}; Duration: {1}",
                    shard.Location,
                    stopwatch.Elapsed);

                return shard;
            }
        }

        /// <summary>
        /// Atomically adds a shard to ShardMap using the specified location.
        /// </summary>
        /// <param name="location">Location of shard to be added.</param>
        /// <returns>A shard attached to this shard map.</returns>
        public Shard CreateShard(ShardLocation location)
        {
            ExceptionUtils.DisallowNullArgument(location, "location");

            using (ActivityIdScope activityId = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "CreateShard",
                    "Start; Shard: {0} ",
                    location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                Shard shard = _defaultMapper.Add(
                    new Shard(
                        this.Manager,
                        this,
                        new ShardCreationInfo(location)));

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "CreateShard",
                    "Complete; Shard: {0}; Duration: {1}",
                    location,
                    stopwatch.Elapsed);

                return shard;
            }
        }

        /// <summary>
        /// Removes a shard from ShardMap.
        /// </summary>
        /// <param name="shard">Shard to remove.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void DeleteShard(Shard shard)
        {
            ExceptionUtils.DisallowNullArgument(shard, "shard");

            using (ActivityIdScope activityId = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "DeleteShard",
                    "Start; Shard: {0} ",
                    shard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                _defaultMapper.Remove(shard);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "DeleteShard",
                    "Complete; Shard: {0}; Duration: {1}",
                    shard.Location,
                    stopwatch.Elapsed);
            }
        }

        /// <summary>
        /// Updates a shard with the changes specified in the <paramref name="update"/> parameter.
        /// </summary>
        /// <param name="currentShard">Shard being updated.</param>
        /// <param name="update">Updated properties of the shard.</param>
        /// <returns>New Shard with updated information.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        internal Shard UpdateShard(Shard currentShard, ShardUpdate update)
        {
            ExceptionUtils.DisallowNullArgument(currentShard, "currentShard");
            ExceptionUtils.DisallowNullArgument(update, "update");

            using (ActivityIdScope activityIdScope = new ActivityIdScope(Guid.NewGuid()))
            {
                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "UpdateShard",
                    "Start; Shard: {0}",
                    currentShard.Location);

                Stopwatch stopwatch = Stopwatch.StartNew();

                Shard shard = _defaultMapper.UpdateShard(currentShard, update);

                stopwatch.Stop();

                Tracer.TraceInfo(
                    TraceSourceConstants.ComponentNames.ShardMap,
                    "UpdateShard",
                    "Complete; Shard: {0}; Duration: {1}",
                    currentShard.Location,
                    stopwatch.Elapsed);

                return shard;
            }
        }

        /// <summary>
        /// Opens a connection to the given shard provider.
        /// </summary>
        /// <param name="shardProvider">Shard provider containing shard to be connected to.</param>
        /// <param name="connectionString">Connection string for connection. Must have credentials.</param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Caller will be responsible for disposal")]
        internal SqlConnection OpenConnection(
            IShardProvider shardProvider,
            string connectionString,
            ConnectionOptions options = ConnectionOptions.Validate)
        {
            Debug.Assert(shardProvider != null, "Expecting IShardProvider.");
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

            string connectionStringFinal = this.ValidateAndPrepareConnectionString(
                shardProvider,
                connectionString);

            ExceptionUtils.EnsureShardBelongsToShardMap(
                this.Manager,
                this,
                shardProvider.ShardInfo,
                "OpenConnection",
                "Shard");

            IUserStoreConnection conn = this.Manager.StoreConnectionFactory.GetUserConnection(connectionStringFinal);

            Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.ShardMap,
                "OpenConnection", "Start; Shard: {0}; Options: {1}; ConnectionString: {2}",
                shardProvider.ShardInfo.Location,
                options,
                connectionStringFinal);

            using (ConditionalDisposable<IUserStoreConnection> cd = new ConditionalDisposable<IUserStoreConnection>(conn))
            {
                Stopwatch stopwatch = Stopwatch.StartNew();

                conn.Open();

                stopwatch.Stop();

                // If validation is requested.
                if ((options & ConnectionOptions.Validate) == ConnectionOptions.Validate)
                {
                    shardProvider.Validate(this.StoreShardMap, conn.Connection);
                }

                cd.DoNotDispose = true;

                Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.ShardMap,
                "OpenConnection", "Complete; Shard: {0}; Options: {1}; Open Duration: {2}",
                shardProvider.ShardInfo.Location,
                options,
                stopwatch.Elapsed);
            }
            
            return conn.Connection;
        }

        /// <summary>
        /// Asynchronously opens a connection to the given shard provider.
        /// </summary>
        /// <param name="shardProvider">Shard provider containing shard to be connected to.</param>
        /// <param name="connectionString">Connection string for connection. Must have credentials.</param>
        /// <param name="options">Options for validation operations to perform on opened connection.</param>
        /// <returns>A task encapsulating the SqlConnection</returns>
        /// <remarks>All exceptions are reported via the returned task.</remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Caller will be responsible for disposal")]
        internal async Task<SqlConnection> OpenConnectionAsync(
            IShardProvider shardProvider,
            string connectionString,
            ConnectionOptions options = ConnectionOptions.Validate)
        {
            Debug.Assert(shardProvider != null, "Expecting IShardProvider.");
            ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");

            string connectionStringFinal = this.ValidateAndPrepareConnectionString(
                shardProvider,
                connectionString);

            ExceptionUtils.EnsureShardBelongsToShardMap(
                this.Manager,
                this,
                shardProvider.ShardInfo,
                "OpenConnectionAsync",
                "Shard");

            IUserStoreConnection conn = this.Manager.StoreConnectionFactory.GetUserConnection(connectionStringFinal);

            Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.ShardMap,
                "OpenConnectionAsync", "Start; Shard: {0}; Options: {1}; ConnectionString: {2}",
                shardProvider.ShardInfo.Location,
                options,
                connectionStringFinal);

            using (ConditionalDisposable<IUserStoreConnection> cd = new ConditionalDisposable<IUserStoreConnection>(conn))
            {
                Stopwatch stopwatch = Stopwatch.StartNew();

                await conn.OpenAsync().ConfigureAwait(false);

                stopwatch.Stop();

                // If validation is requested.
                if ((options & ConnectionOptions.Validate) == ConnectionOptions.Validate)
                {
                    await shardProvider.ValidateAsync(this.StoreShardMap, conn.Connection).ConfigureAwait(false);
                }

                cd.DoNotDispose = true;

                Tracer.TraceInfo(
                TraceSourceConstants.ComponentNames.ShardMap,
                "OpenConnectionAsync", "Complete; Shard: {0}; Options: {1}; Open Duration: {2}",
                shardProvider.ShardInfo.Location,
                options,
                stopwatch.Elapsed);
            }
            
            return conn.Connection;
        }

        /// <summary>
        /// Gets the mapper. This method is used by OpenConnection and Lookup of V.
        /// </summary>
        /// <typeparam name="V">Shard provider type.</typeparam>
        /// <returns>Appropriate mapper for the given shard map.</returns>
        internal abstract IShardMapper<V> GetMapper<V>();

        #region ICloneable<ShardMap>

        /// <summary>
        /// Clones the given shard map.
        /// </summary>
        /// <returns>A cloned instance of the shard map.</returns>
        ShardMap ICloneable<ShardMap>.Clone()
        {
            return this.CloneCore();
        }

        /// <summary>
        /// Clones the current shard map instance.
        /// </summary>
        /// <returns>Cloned shard map instance.</returns>
        protected abstract ShardMap CloneCore();

        #endregion ICloneable<ShardMap>

        /// <summary>
        /// Ensures that the provided connection string is valid and builds the connection string
        /// to be used for DDR connection to the given shard provider.
        /// </summary>
        /// <param name="shardProvider">Shard provider containing shard to be connected to.</param>
        /// <param name="connectionString">Input connection string.</param>
        /// <returns>Connection string for DDR connection.</returns>
        private string ValidateAndPrepareConnectionString(IShardProvider shardProvider, string connectionString)
        {
            Debug.Assert(shardProvider != null);
            Debug.Assert(connectionString != null);

            // Devnote: If connection string specifies Active Directory authentication and runtime is not 
            // .NET 4.6 or higher, then below call will throw.
            SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);

            // DataSource must not be set.
            if (!string.IsNullOrEmpty(connectionStringBuilder.DataSource))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed,
                        "DataSource"),
                    "connectionString");
            }

            // InitialCatalog must not be set.
            if (!string.IsNullOrEmpty(connectionStringBuilder.InitialCatalog))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed,
                        "Initial Catalog"),
                    "connectionString");
            }

            // ConnectRetryCount must not be set (default value is 1)
            if (ShardMapUtils.IsConnectionResiliencySupported && (int)connectionStringBuilder[ShardMapUtils.ConnectRetryCount] > 1)
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardMap_OpenConnection_ConnectionStringPropertyDisallowed,
                        ShardMapUtils.ConnectRetryCount),
                    "connectionString");
            }

            // Verify that either UserID/Password or provided or integrated authentication is enabled.
            SqlShardMapManagerCredentials.EnsureCredentials(connectionStringBuilder, "connectionString");

            Shard s = shardProvider.ShardInfo;

            connectionStringBuilder.DataSource = s.Location.DataSource;
            connectionStringBuilder.InitialCatalog = s.Location.Database;

            // Append the proper post-fix for ApplicationName
            connectionStringBuilder.ApplicationName = ApplicationNameHelper.AddApplicationNameSuffix(
                connectionStringBuilder.ApplicationName,
                this.ApplicationNameSuffix);

            // Disable connection resiliency if necessary
            if (ShardMapUtils.IsConnectionResiliencySupported)
            {
                connectionStringBuilder[ShardMapUtils.ConnectRetryCount] = 0;
            }

            return connectionStringBuilder.ConnectionString;
        }
    }
}
